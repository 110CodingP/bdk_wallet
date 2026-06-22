// Bitcoin Dev Kit
// Written in 2020 by Alekos Filini <alekos.filini@gmail.com>
//
// Copyright (c) 2020-2025 Bitcoin Dev Kit Developers
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE
// or http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your option.
// You may not use this file except in accordance with one or both of these
// licenses.

//! Wallet
//!
//! This module defines the [`Wallet`].

use alloc::{
    boxed::Box,
    collections::BTreeSet,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use core::fmt::{Debug, Display};
use core::{cmp::Ordering, fmt, mem, ops::Deref};

use bdk_chain::{
    indexed_tx_graph,
    indexer::keychain_txout::KeychainTxOutIndex,
    keychain_txout::InsertDescriptorError,
    local_chain::{ApplyHeaderError, CannotConnectError, CheckPoint, CheckPointIter, LocalChain},
    spk_client::{
        FullScanRequest, FullScanRequestBuilder, FullScanResponse, SyncRequest, SyncRequestBuilder,
        SyncResponse,
    },
    tx_graph::{CalculateFeeError, CanonicalTx, TxGraph, TxUpdate},
    BlockId, CanonicalizationParams, ChainPosition, ConfirmationBlockTime, DescriptorExt,
    FullTxOut, Indexed, IndexedTxGraph, Indexer, Merge,
};
use bitcoin::{
    absolute,
    consensus::encode::serialize,
    constants::genesis_block,
    psbt,
    secp256k1::Secp256k1,
    sighash::{EcdsaSighashType, TapSighashType},
    transaction, Address, Amount, Block, FeeRate, Network, NetworkKind, OutPoint, Psbt, ScriptBuf,
    Sequence, SignedAmount, Transaction, TxOut, Txid, Weight, Witness,
};
use miniscript::psbt::{PsbtExt, PsbtInputExt, PsbtInputSatisfier};
use rand_core::RngCore;

use crate::error::MissingKeychain;

mod changeset;
pub mod coin_selection;
pub mod error;
mod event;
pub mod export;
pub mod locked_outpoints;
mod params;
mod persisted;
pub mod signer;
pub mod tx_builder;
pub(crate) mod utils;

use crate::descriptor::{
    checksum::calc_checksum, error::Error as DescriptorError, policy::BuildSatisfaction,
    DerivedDescriptor, DescriptorMeta, ExtendedDescriptor, ExtractPolicy, IntoWalletDescriptor,
    XKeyUtils,
};
use crate::psbt::PsbtUtils;
use crate::types::*;
use crate::wallet::{
    coin_selection::{DefaultCoinSelectionAlgorithm, Excess, InsufficientFunds},
    error::{BuildFeeBumpError, CreateTxError, MiniscriptPsbtError},
    signer::{SignOptions, SignerError, SignersContainer},
    tx_builder::{FeePolicy, TxBuilder, TxParams},
    utils::{check_nsequence_rbf, After, Older, SecpCtx},
};
use crate::{
    collections::{BTreeMap, HashMap, HashSet},
    error::InitError,
};

// re-exports
pub use bdk_chain::Balance;
pub use changeset::ChangeSet;
pub use error::{LoadError, LoadMismatch};
pub use event::*;
pub use params::*;
pub use persisted::*;
pub use utils::IsDust;
pub use utils::TxDetails;

/// A Bitcoin wallet
///
/// The `Wallet` acts as a way of coherently interfacing with output descriptors and related
/// transactions. Its main components are the output *descriptors* from which it can derive
/// addresses.
///
/// The user is responsible for loading and writing wallet changes which are represented as
/// [`ChangeSet`]s (see [`take_staged`]). Also see individual functions and example for instructions
/// on when [`Wallet`] state needs to be persisted.
///
/// The `Wallet` descriptors must not derive the same script pubkeys.
///  See [`KeychainTxOutIndex::insert_descriptor()`] for more details.
///
/// [`take_staged`]: Wallet::take_staged
#[derive(Debug)]
pub struct Wallet<K> {
    chain: LocalChain,
    tx_graph: IndexedTxGraph<ConfirmationBlockTime, KeychainTxOutIndex<K>>,
    stage: ChangeSet<K>,
    network: Network,
    secp: SecpCtx,
    locked_outpoints: HashSet<OutPoint>,
}

/// An update to [`Wallet`].
///
/// It updates [`KeychainTxOutIndex`], [`bdk_chain::TxGraph`] and [`LocalChain`] atomically.
#[derive(Debug, Clone)]
pub struct Update<K> {
    /// Contains the last active derivation indices per keychain (`K`), which is used to update the
    /// [`KeychainTxOutIndex`].
    pub last_active_indices: BTreeMap<K, u32>,

    /// Update for the wallet's internal [`TxGraph`].
    pub tx_update: TxUpdate<ConfirmationBlockTime>,

    /// Update for the wallet's internal [`LocalChain`].
    pub chain: Option<CheckPoint>,
}

impl<K> From<FullScanResponse<K>> for Update<K> {
    fn from(value: FullScanResponse<K>) -> Self {
        Self {
            last_active_indices: value.last_active_indices,
            tx_update: value.tx_update,
            chain: value.chain_update,
        }
    }
}

impl<K> From<SyncResponse> for Update<K> {
    fn from(value: SyncResponse) -> Self {
        Self {
            last_active_indices: BTreeMap::new(),
            tx_update: value.tx_update,
            chain: value.chain_update,
        }
    }
}

impl<K> Default for Update<K> {
    fn default() -> Self {
        Update {
            last_active_indices: Default::default(),
            tx_update: Default::default(),
            chain: Default::default(),
        }
    }
}

/// A derived address and the index it was found at.
/// For convenience this automatically derefs to `Address`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressInfo<K> {
    /// Child index of this address
    pub index: u32,
    /// Address
    pub address: Address,
    /// Type of keychain
    pub keychain: K,
}

impl<K> Deref for AddressInfo<K> {
    type Target = Address;

    fn deref(&self) -> &Self::Target {
        &self.address
    }
}

impl<K> fmt::Display for AddressInfo<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.address)
    }
}

/// A `CanonicalTx` managed by a `Wallet`.
pub type WalletTx<'a> = CanonicalTx<'a, Arc<Transaction>, ConfirmationBlockTime>;

impl<K: Ord + Clone + Debug> Wallet<K> {
    /// Create a new [`Wallet`] with given `params`.
    pub fn create_with_params(params: CreateParams<K>) -> Result<Self, InitError<K>> {
        let secp = params.secp;
        let network = params.network;
        let descriptors = params.descriptors;
        let genesis_hash = params
            .genesis_hash
            .unwrap_or(genesis_block(network).block_hash());
        let (chain, chain_changeset) = LocalChain::from_genesis_hash(genesis_hash);

        let locked_outpoints = HashSet::new();

        let mut stage = ChangeSet {
            descriptor: None,
            change_descriptor: None,
            local_chain: chain_changeset,
            network: Some(network),
            descriptors: descriptors.clone(),
            ..Default::default()
        };

        let tx_graph = make_indexed_graph(
            &mut stage,
            Default::default(),
            Default::default(),
            descriptors,
            params.lookahead,
            params.use_spk_cache,
        )
        .map_err(|err| match err {
            InsertDescriptorError::KeychainAlreadyAssigned {
                keychain,
                existing_assignment: _,
            } => InitError::KeychainAlreadyExists(Box::new(keychain)),
            InsertDescriptorError::DescriptorAlreadyAssigned {
                descriptor: desc,
                existing_assignment: _,
            } => InitError::DescAlreadyExists(desc),
        })?;

        Ok(Wallet {
            network,
            chain,
            tx_graph,
            stage,
            secp,
            locked_outpoints,
        })
    }

    /// Build [`Wallet`] by loading from persistence or [`ChangeSet`].
    ///
    /// Note that the descriptor secret keys are not persisted to the db.
    /// You can check the wallet's descriptors are what you expect with [`LoadParams::descriptor`]
    ///
    /// # Synopsis
    ///
    /// ```rust,no_run
    /// # use bdk_wallet::{Wallet, ChangeSet, KeychainKind};
    /// # use bitcoin::{BlockHash, Network, hashes::Hash};
    /// # fn main() -> anyhow::Result<()> {
    /// # const EXTERNAL_DESC: &str = "wpkh(tprv8ZgxMBicQKsPdy6LMhUtFHAgpocR8GC6QmwMSFpZs7h6Eziw3SpThFfczTDh5rW2krkqffa11UpX3XkeTTB2FvzZKWXqPY54Y6Rq4AQ5R8L/84'/1'/0'/0/*)";
    /// # const INTERNAL_DESC: &str = "wpkh(tprv8ZgxMBicQKsPdy6LMhUtFHAgpocR8GC6QmwMSFpZs7h6Eziw3SpThFfczTDh5rW2krkqffa11UpX3XkeTTB2FvzZKWXqPY54Y6Rq4AQ5R8L/84'/1'/0'/1/*)";
    /// # let changeset: ChangeSet<KeychainKind> = ChangeSet::default();
    /// // Load a wallet from changeset (no persistence).
    /// let wallet = Wallet::load()
    ///     .load_wallet_no_persist(changeset)?
    ///     .expect("must have data to load wallet");
    ///
    /// // Load a wallet that is persisted to SQLite database.
    /// # let temp_dir = tempfile::tempdir().expect("must create tempdir");
    /// # let file_path = temp_dir.path().join("store.db");
    /// # let genesis_hash = BlockHash::all_zeros();
    /// let mut conn = bdk_wallet::rusqlite::Connection::open(file_path)?;
    /// let mut wallet = Wallet::load()
    ///     // check loaded descriptors matches these values and extract private keys
    ///     .descriptor(KeychainKind::External, Some(EXTERNAL_DESC))
    ///     .descriptor(KeychainKind::Internal, Some(INTERNAL_DESC))
    ///     // ensure loaded wallet's genesis hash matches this value
    ///     .check_genesis_hash(genesis_hash)
    ///     // set a lookahead for our indexer
    ///     .lookahead(101)
    ///     .load_wallet(&mut conn)?
    ///     .expect("must have data to load wallet");
    /// # Ok(())
    /// # }
    /// ```
    pub fn load() -> LoadParams<K> {
        LoadParams::new()
    }

    /// Load [`Wallet`] from the given previously persisted [`ChangeSet`] and `params`.
    ///
    /// Returns `Ok(None)` if the changeset is empty. Refer to [`Wallet::load`] for more.
    pub fn load_with_params(
        changeset: ChangeSet<K>,
        params: LoadParams<K>,
    ) -> Result<Option<Self>, LoadError<K>> {
        if changeset.is_empty() {
            return Ok(None);
        }
        let secp = Secp256k1::new();
        let network = changeset.network.ok_or(LoadError::MissingNetwork)?;
        let network_kind = NetworkKind::from(network);
        let chain = LocalChain::from_changeset(changeset.local_chain)
            .map_err(|_| LoadError::MissingGenesis)?;

        if let Some(exp_network) = params.check_network {
            if network != exp_network {
                return Err(LoadError::Mismatch(LoadMismatch::Network {
                    loaded: network,
                    expected: exp_network,
                }));
            }
        }
        if let Some(exp_genesis_hash) = params.check_genesis_hash {
            if chain.genesis_hash() != exp_genesis_hash {
                return Err(LoadError::Mismatch(LoadMismatch::Genesis {
                    loaded: chain.genesis_hash(),
                    expected: exp_genesis_hash,
                }));
            }
        }

        for (keychain, check_desc) in params.check_descriptors {
            if let Some(make_desc) = check_desc {
                // Check if the keychain contains the expected descriptor.
                let loaded_desc = changeset.descriptors.get(&keychain);
                let exp_desc = make_desc(&secp, network_kind).map_err(LoadError::Descriptor)?;
                match loaded_desc {
                    Some(descriptor) => {
                        if descriptor.descriptor_id() != exp_desc.descriptor_id() {
                            return Err(LoadError::Mismatch(LoadMismatch::Descriptor {
                                keychain,
                                loaded: Some(Box::new(descriptor.clone())),
                                expected: Some(Box::new(exp_desc)),
                            }));
                        }
                    }
                    None => {
                        return Err(LoadError::Mismatch(LoadMismatch::Descriptor {
                            keychain,
                            loaded: None,
                            expected: Some(Box::new(exp_desc)),
                        }));
                    }
                }
            }
            // Check if the keychain is present in loaded changeset.
            else {
                if changeset.descriptors.contains_key(&keychain) {
                    return Err(LoadError::MissingDescriptor(keychain));
                }
            }
        }

        // Apply locked outpoints
        let locked_outpoints = changeset.locked_outpoints.outpoints;
        let locked_outpoints = locked_outpoints
            .into_iter()
            .filter(|&(_op, is_locked)| is_locked)
            .map(|(op, _)| op)
            .collect();

        let mut stage = ChangeSet::default();

        if changeset.descriptor.is_some() {
            stage.descriptors = changeset.descriptors.clone();
        }

        let tx_graph = make_indexed_graph(
            &mut stage,
            changeset.tx_graph,
            changeset.indexer,
            changeset.descriptors.clone(),
            params.lookahead,
            params.use_spk_cache,
        )
        .map_err(|err| match err {
            InsertDescriptorError::KeychainAlreadyAssigned {
                keychain,
                existing_assignment: _,
            } => LoadError::KeychainAlreadyExists(Box::new(keychain)),
            InsertDescriptorError::DescriptorAlreadyAssigned {
                descriptor: desc,
                existing_assignment: _,
            } => LoadError::DescAlreadyExists(desc),
        })?;

        Ok(Some(Wallet {
            chain,
            tx_graph,
            stage,
            network,
            secp,
            locked_outpoints,
        }))
    }

    /// Get the [`Network`] the wallet is using.
    pub fn network(&self) -> Network {
        self.network
    }

    /// Iterator over all keychains in this wallet
    pub fn keychains(&self) -> impl Iterator<Item = (K, &ExtendedDescriptor)> {
        self.tx_graph.index.keychains()
    }

    /// Peek an address of the given `keychain` at `index` without revealing it.
    ///
    /// For non-wildcard descriptors this returns the same address at every provided index.
    ///
    /// This returns an error whenever `keychain` does not exist in the [`Wallet`].
    /// The `Option` returned is `None` whenever index is greater than the
    /// [BIP32](https://github.com/bitcoin/bips/blob/master/bip-0032.mediawiki) max index.
    pub fn peek_address(
        &self,
        keychain: K,
        mut index: u32,
    ) -> Result<Option<AddressInfo<K>>, MissingKeychain> {
        let mut spk_iter = self
            .tx_graph
            .index
            .unbounded_spk_iter(keychain.clone())
            .ok_or(MissingKeychain)?;
        if !spk_iter.descriptor().has_wildcard() {
            index = 0;
        }
        let (index, spk) = match spk_iter.nth(index as usize) {
            Some(res) => res,
            None => return Ok(None),
        };

        Ok(Some(AddressInfo {
            index,
            address: Address::from_script(&spk, self.network).expect("must have address form"),
            keychain,
        }))
    }

    /// Attempt to reveal the next address of the given `keychain`.
    ///
    /// This will increment the keychain's derivation index. If the keychain's descriptor doesn't
    /// contain a wildcard or every address is already revealed up to the maximum derivation
    /// index defined in [BIP32](https://github.com/bitcoin/bips/blob/master/bip-0032.mediawiki),
    /// then the last revealed address will be returned.
    ///
    /// Returns an error if `keychain` does not exist.
    ///
    /// **WARNING**: To avoid address reuse you must persist the changes resulting from one or more
    /// calls to this method before closing the wallet. For example:
    ///
    /// ```rust,no_run
    /// # use bdk_wallet::{LoadParams, ChangeSet, KeychainKind};
    /// use bdk_chain::rusqlite::Connection;
    /// let mut conn = Connection::open_in_memory().expect("must open connection");
    /// let mut wallet = LoadParams::new()
    ///     .load_wallet(&mut conn)
    ///     .expect("database is okay")
    ///     .expect("database has data");
    /// let next_address = wallet
    ///     .reveal_next_address(KeychainKind::External)
    ///     .expect("keychain must exist");
    /// wallet.persist(&mut conn).expect("write is okay");
    ///
    /// // Now it's safe to show the user their next address!
    /// println!("Next address: {}", next_address.address);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn reveal_next_address(&mut self, keychain: K) -> Result<AddressInfo<K>, MissingKeychain> {
        let index = &mut self.tx_graph.index;
        let stage = &mut self.stage;

        let ((index, spk), index_changeset) = index
            .reveal_next_spk(keychain.clone())
            .ok_or(MissingKeychain)?;

        stage.merge(index_changeset.into());

        Ok(AddressInfo {
            index,
            address: Address::from_script(spk.as_script(), self.network)
                .expect("must have address form"),
            keychain,
        })
    }

    /// Reveal addresses up to and including the target `index` and return an iterator
    /// of newly revealed addresses.
    ///
    /// If the target `index` is unreachable, we make a best effort to reveal up to the last
    /// possible index. If all addresses up to the given `index` are already revealed, then
    /// no new addresses are returned.
    ///
    /// Returns an error if `keychain` does not exist.
    ///
    /// **WARNING**: To avoid address reuse you must persist the changes resulting from one or more
    /// calls to this method before closing the wallet. See [`Wallet::reveal_next_address`].
    pub fn reveal_addresses_to(
        &mut self,
        keychain: K,
        index: u32,
    ) -> Result<impl Iterator<Item = AddressInfo<K>> + '_, MissingKeychain> {
        let (spks, index_changeset) = self
            .tx_graph
            .index
            .reveal_to_target(keychain.clone(), index)
            .ok_or(MissingKeychain)?;

        self.stage.merge(index_changeset.into());

        Ok(spks.into_iter().map(move |(index, spk)| AddressInfo {
            index,
            address: Address::from_script(&spk, self.network).expect("must have address form"),
            keychain: keychain.clone(),
        }))
    }

    /// Get the next unused address for the given `keychain`, i.e. the address with the lowest
    /// derivation index that hasn't been used in a transaction.
    ///
    /// This will attempt to reveal a new address if all previously revealed addresses have
    /// been used, in which case the returned address will be the same as calling
    /// [`Wallet::reveal_next_address`].
    ///
    /// Returns an error if `keychain` does not exist.
    ///
    /// **WARNING**: To avoid address reuse you must persist the changes resulting from one or more
    /// calls to this method before closing the wallet. See [`Wallet::reveal_next_address`].
    pub fn next_unused_address(&mut self, keychain: K) -> Result<AddressInfo<K>, MissingKeychain> {
        let index = &mut self.tx_graph.index;

        let ((index, spk), index_changeset) = index
            .next_unused_spk(keychain.clone())
            .ok_or(MissingKeychain)?;

        self.stage
            .merge(indexed_tx_graph::ChangeSet::from(index_changeset).into());

        Ok(AddressInfo {
            index,
            address: Address::from_script(spk.as_script(), self.network)
                .expect("must have address form"),
            keychain,
        })
    }

    /// Marks an address used of the given `keychain` at `index`.
    ///
    /// Returns whether the given index was present and then removed from the unused set.
    ///
    /// Returns an error if `keychain` does not exist.
    pub fn mark_used(&mut self, keychain: K, index: u32) -> Result<bool, MissingKeychain> {
        Ok(self.tx_graph.index.mark_used(keychain, index))
    }

    /// Undoes the effect of [`mark_used`] and returns whether the `index` was inserted
    /// back into the unused set.
    ///
    /// Since this is only a superficial marker, it will have no effect if the address at the given
    /// `index` was actually used, i.e. the wallet has previously indexed a tx output for the
    /// derived spk.
    ///
    /// Returns an error if `keychain` does not exist.
    ///
    /// [`mark_used`]: Self::mark_used
    pub fn unmark_used(&mut self, keychain: K, index: u32) -> Result<bool, MissingKeychain> {
        Ok(self.tx_graph.index.unmark_used(keychain, index))
    }

    /// List addresses that are revealed but unused.
    ///
    /// Note if the returned iterator is empty you can reveal more addresses
    /// by using [`reveal_next_address`](Self::reveal_next_address) or
    /// [`reveal_addresses_to`](Self::reveal_addresses_to).
    ///
    /// Returns an error if `keychain` does not exist.
    pub fn list_unused_addresses(
        &self,
        keychain: K,
    ) -> Result<impl DoubleEndedIterator<Item = AddressInfo<K>> + '_, MissingKeychain> {
        Ok(self
            .tx_graph
            .index
            .unused_keychain_spks(keychain.clone())
            .map(move |(index, spk)| AddressInfo {
                index,
                address: Address::from_script(spk.as_script(), self.network)
                    .expect("must have address form"),
                keychain: keychain.clone(),
            }))
    }

    /// Return whether or not a `script` is part of this wallet (either internal or external)
    pub fn is_mine(&self, script: ScriptBuf) -> bool {
        self.tx_graph.index.index_of_spk(script).is_some()
    }

    /// Finds how the wallet derived the script pubkey `spk`.
    ///
    /// Will only return `Some(_)` if the wallet has given out the spk.
    pub fn derivation_of_spk(&self, spk: ScriptBuf) -> Option<(K, u32)> {
        self.tx_graph.index.index_of_spk(spk).cloned()
    }

    /// Return the list of unspent outputs of this wallet
    pub fn list_unspent(&self) -> impl Iterator<Item = LocalOutput<K>> + '_ {
        self.tx_graph
            .graph()
            .filter_chain_unspents(
                &self.chain,
                self.chain.tip().block_id(),
                CanonicalizationParams::default(),
                self.tx_graph.index.outpoints().iter().cloned(),
            )
            .map(|((k, i), full_txo)| new_local_utxo(k, i, full_txo))
    }

    /// Get the [`TxDetails`] of a wallet transaction.
    ///
    /// If the transaction with txid [`Txid`] cannot be found in the wallet's transactions, `None`
    /// is returned.
    pub fn tx_details(&self, txid: Txid) -> Option<TxDetails> {
        let tx: WalletTx = self.transactions().find(|c| c.tx_node.txid == txid)?;

        let (sent, received) = self.sent_and_received(&tx.tx_node.tx);
        let fee: Option<Amount> = self.calculate_fee(&tx.tx_node.tx).ok();
        let fee_rate: Option<FeeRate> = self.calculate_fee_rate(&tx.tx_node.tx).ok();
        let balance_delta: SignedAmount = self.tx_graph.index.net_value(&tx.tx_node.tx, ..);
        let chain_position = tx.chain_position;

        let tx_details: TxDetails = TxDetails {
            txid,
            received,
            sent,
            fee,
            fee_rate,
            balance_delta,
            chain_position,
            tx: tx.tx_node.tx,
        };

        Some(tx_details)
    }

    /// List all relevant outputs (includes both spent and unspent, confirmed and unconfirmed).
    ///
    /// To list only unspent outputs (UTXOs), use [`Wallet::list_unspent`] instead.
    pub fn list_output(&self) -> impl Iterator<Item = LocalOutput<K>> + '_ {
        self.tx_graph
            .graph()
            .filter_chain_txouts(
                &self.chain,
                self.chain.tip().block_id(),
                CanonicalizationParams::default(),
                self.tx_graph.index.outpoints().iter().cloned(),
            )
            .map(|((k, i), full_txo)| new_local_utxo(k, i, full_txo))
    }

    /// Get all the checkpoints the wallet is currently storing indexed by height.
    pub fn checkpoints(&self) -> CheckPointIter {
        self.chain.iter_checkpoints()
    }

    /// Returns the latest checkpoint.
    pub fn latest_checkpoint(&self) -> CheckPoint {
        self.chain.tip()
    }

    /// Get unbounded script pubkey iterators for both `Internal` and `External` keychains.
    ///
    /// This is intended to be used when doing a full scan of your addresses (e.g. after restoring
    /// from seed words). You pass the `BTreeMap` of iterators to a blockchain data source (e.g.
    /// electrum server) which will go through each address until it reaches a *stop gap*.
    ///
    /// Note carefully that iterators go over **all** script pubkeys on the keychains (not what
    /// script pubkeys the wallet is storing internally).
    pub fn all_unbounded_spk_iters(
        &self,
    ) -> BTreeMap<K, impl Iterator<Item = Indexed<ScriptBuf>> + Clone> {
        self.tx_graph.index.all_unbounded_spk_iters()
    }

    /// Get an unbounded script pubkey iterator for the given `keychain`.
    ///
    /// See [`all_unbounded_spk_iters`] for more documentation.
    ///
    /// Returns an error if `keychain` does not exist.
    ///
    /// [`all_unbounded_spk_iters`]: Self::all_unbounded_spk_iters
    pub fn unbounded_spk_iter(
        &self,
        keychain: K,
    ) -> Result<impl Iterator<Item = Indexed<ScriptBuf>> + Clone, MissingKeychain> {
        self.tx_graph
            .index
            .unbounded_spk_iter(keychain)
            .ok_or(MissingKeychain)
    }

    /// Returns the utxo owned by this wallet corresponding to `outpoint` if it exists in the
    /// wallet's database.
    pub fn get_utxo(&self, op: OutPoint) -> Option<LocalOutput<K>> {
        let ((keychain, index), _) = self.tx_graph.index.txout(op)?;
        self.tx_graph
            .graph()
            .filter_chain_unspents(
                &self.chain,
                self.chain.tip().block_id(),
                CanonicalizationParams::default(),
                core::iter::once(((), op)),
            )
            .map(|(_, full_txo)| new_local_utxo(keychain.clone(), index, full_txo))
            .next()
    }

    /// Inserts a [`TxOut`] at [`OutPoint`] into the wallet's transaction graph.
    ///
    /// This is used for providing a previous output's value so that we can use [`calculate_fee`]
    /// or [`calculate_fee_rate`] on a given transaction. Outputs inserted with this method will
    /// not be returned in [`list_unspent`] or [`list_output`].
    ///
    /// **WARNINGS:** This should only be used to add `TxOut`s that the wallet does not own. Only
    /// insert `TxOut`s that you trust the values for!
    ///
    /// You must persist the changes resulting from one or more calls to this method if you need
    /// the inserted `TxOut` data to be reloaded after closing the wallet.
    /// See [`Wallet::reveal_next_address`].
    ///
    /// [`calculate_fee`]: Self::calculate_fee
    /// [`calculate_fee_rate`]: Self::calculate_fee_rate
    /// [`list_unspent`]: Self::list_unspent
    /// [`list_output`]: Self::list_output
    pub fn insert_txout(&mut self, outpoint: OutPoint, txout: TxOut) {
        let additions = self.tx_graph.insert_txout(outpoint, txout);
        self.stage.merge(additions.into());
    }

    /// Calculates the fee of a given transaction. Returns [`Amount::ZERO`] if `tx` is a coinbase
    /// transaction.
    ///
    /// To calculate the fee for a [`Transaction`] with inputs not owned by this wallet you must
    /// manually insert the TxOut(s) into the tx graph using the [`insert_txout`] function.
    ///
    /// Note `tx` does not have to be in the graph for this to work.
    ///
    /// # Examples
    ///
    /// ```rust, no_run
    /// # use bitcoin::Txid;
    /// # use bdk_wallet::{Wallet, KeychainKind};
    /// # let mut wallet: Wallet<KeychainKind> = todo!();
    /// # let txid:Txid = todo!();
    /// let tx = wallet.get_tx(txid).expect("transaction").tx_node.tx;
    /// let fee = wallet.calculate_fee(&tx).expect("fee");
    /// ```
    ///
    /// ```rust, no_run
    /// # use bitcoin::Psbt;
    /// # use bdk_wallet::{Wallet, KeychainKind};
    /// # let mut wallet: Wallet<KeychainKind> = todo!();
    /// # let mut psbt: Psbt = todo!();
    /// let tx = &psbt.clone().extract_tx().expect("tx");
    /// let fee = wallet.calculate_fee(tx).expect("fee");
    /// ```
    /// [`insert_txout`]: Self::insert_txout
    pub fn calculate_fee(&self, tx: &Transaction) -> Result<Amount, CalculateFeeError> {
        self.tx_graph.graph().calculate_fee(tx)
    }

    /// Calculate the [`FeeRate`] for a given transaction.
    ///
    /// To calculate the fee rate for a [`Transaction`] with inputs not owned by this wallet you
    /// must manually insert the TxOut(s) into the tx graph using the [`insert_txout`] function.
    ///
    /// Note `tx` does not have to be in the graph for this to work.
    ///
    /// # Examples
    ///
    /// ```rust, no_run
    /// # use bitcoin::Txid;
    /// # use bdk_wallet::{Wallet, KeychainKind};
    /// # let mut wallet: Wallet<KeychainKind> = todo!();
    /// # let txid:Txid = todo!();
    /// let tx = wallet.get_tx(txid).expect("transaction").tx_node.tx;
    /// let fee_rate = wallet.calculate_fee_rate(&tx).expect("fee rate");
    /// ```
    ///
    /// ```rust, no_run
    /// # use bitcoin::Psbt;
    /// # use bdk_wallet::{Wallet, KeychainKind};
    /// # let mut wallet: Wallet<KeychainKind> = todo!();
    /// # let mut psbt: Psbt = todo!();
    /// let tx = &psbt.clone().extract_tx().expect("tx");
    /// let fee_rate = wallet.calculate_fee_rate(tx).expect("fee rate");
    /// ```
    /// [`insert_txout`]: Self::insert_txout
    pub fn calculate_fee_rate(&self, tx: &Transaction) -> Result<FeeRate, CalculateFeeError> {
        self.calculate_fee(tx).map(|fee| fee / tx.weight())
    }

    /// Compute the `tx`'s sent and received [`Amount`]s.
    ///
    /// This method returns a tuple `(sent, received)`. Sent is the sum of the txin amounts
    /// that spend from previous txouts tracked by this wallet. Received is the summation
    /// of this tx's outputs that send to script pubkeys tracked by this wallet.
    ///
    /// # Examples
    ///
    /// ```rust, no_run
    /// # use bitcoin::Txid;
    /// # use bdk_wallet::{Wallet, KeychainKind};
    /// # let mut wallet: Wallet<KeychainKind> = todo!();
    /// # let txid:Txid = todo!();
    /// let tx = wallet.get_tx(txid).expect("tx exists").tx_node.tx;
    /// let (sent, received) = wallet.sent_and_received(&tx);
    /// ```
    ///
    /// ```rust, no_run
    /// # use bitcoin::Psbt;
    /// # use bdk_wallet::{Wallet, KeychainKind};
    /// # let mut wallet: Wallet<KeychainKind> = todo!();
    /// # let mut psbt: Psbt = todo!();
    /// let tx = &psbt.clone().extract_tx().expect("tx");
    /// let (sent, received) = wallet.sent_and_received(tx);
    /// ```
    pub fn sent_and_received(&self, tx: &Transaction) -> (Amount, Amount) {
        self.tx_graph.index.sent_and_received(tx, ..)
    }

    /// Get a single transaction from the wallet as a [`WalletTx`] (if the transaction exists).
    ///
    /// `WalletTx` contains the full transaction alongside meta-data such as:
    /// * Blocks that the transaction is [`Anchor`]ed in. These may or may not be blocks that exist
    ///   in the best chain.
    /// * The [`ChainPosition`] of the transaction in the best chain - whether the transaction is
    ///   confirmed or unconfirmed. If the transaction is confirmed, the anchor which proves the
    ///   confirmation is provided. If the transaction is unconfirmed, the unix timestamp of when
    ///   the transaction was last seen in the mempool is provided.
    ///
    /// ```rust, no_run
    /// use bdk_chain::Anchor;
    /// use bdk_wallet::{chain::ChainPosition, Wallet, KeychainKind};
    /// # let wallet: Wallet<KeychainKind> = todo!();
    /// # let my_txid: bitcoin::Txid = todo!();
    ///
    /// let wallet_tx = wallet.get_tx(my_txid).expect("panic if tx does not exist");
    ///
    /// // get reference to full transaction
    /// println!("my tx: {:#?}", wallet_tx.tx_node.tx);
    ///
    /// // list all transaction anchors
    /// for anchor in wallet_tx.tx_node.anchors {
    ///     println!(
    ///         "tx is anchored by block of hash {}",
    ///         anchor.anchor_block().hash
    ///     );
    /// }
    ///
    /// // get confirmation status of transaction
    /// match wallet_tx.chain_position {
    ///     ChainPosition::Confirmed {
    ///         anchor,
    ///         transitively: None,
    ///     } => println!(
    ///         "tx is confirmed at height {}, we know this since {}:{} is in the best chain",
    ///         anchor.block_id.height, anchor.block_id.height, anchor.block_id.hash,
    ///     ),
    ///     ChainPosition::Confirmed {
    ///         anchor,
    ///         transitively: Some(_),
    ///     } => println!(
    ///         "tx is an ancestor of a tx anchored in {}:{}",
    ///         anchor.block_id.height, anchor.block_id.hash,
    ///     ),
    ///     ChainPosition::Unconfirmed { first_seen, last_seen } => println!(
    ///         "tx is first seen at {:?}, last seen at {:?}, it is unconfirmed as it is not anchored in the best chain",
    ///         first_seen, last_seen
    ///     ),
    /// }
    /// ```
    ///
    /// [`Anchor`]: bdk_chain::Anchor
    pub fn get_tx(&self, txid: Txid) -> Option<WalletTx<'_>> {
        let graph = self.tx_graph.graph();
        graph
            .list_canonical_txs(
                &self.chain,
                self.chain.tip().block_id(),
                CanonicalizationParams::default(),
            )
            .find(|tx| tx.tx_node.txid == txid)
    }

    /// Iterate over relevant and canonical transactions in the wallet.
    ///
    /// A transaction is relevant when it spends from or spends to at least one tracked output. A
    /// transaction is canonical when it is confirmed in the best chain, or does not conflict
    /// with any transaction confirmed in the best chain.
    ///
    /// To iterate over all transactions, including those that are irrelevant and not canonical, use
    /// [`TxGraph::full_txs`].
    ///
    /// To iterate over all canonical transactions, including those that are irrelevant, use
    /// [`TxGraph::list_canonical_txs`].
    pub fn transactions(&self) -> impl Iterator<Item = WalletTx<'_>> + '_ {
        let tx_graph = self.tx_graph.graph();
        let tx_index = &self.tx_graph.index;
        tx_graph
            .list_canonical_txs(
                &self.chain,
                self.chain.tip().block_id(),
                CanonicalizationParams::default(),
            )
            .filter(|c_tx| tx_index.is_tx_relevant(&c_tx.tx_node.tx))
    }

    /// Array of relevant and canonical transactions in the wallet sorted with a comparator
    /// function.
    ///
    /// This is a helper method equivalent to collecting the result of [`Wallet::transactions`]
    /// into a [`Vec`] and then sorting it.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use bdk_wallet::{LoadParams, Wallet, WalletTx, KeychainKind};
    /// # let mut wallet:Wallet<KeychainKind> = todo!();
    /// // Transactions by chain position: first unconfirmed then descending by confirmed height.
    /// let sorted_txs: Vec<WalletTx> =
    ///     wallet.transactions_sort_by(|tx1, tx2| tx2.chain_position.cmp(&tx1.chain_position));
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn transactions_sort_by<F>(&self, compare: F) -> Vec<WalletTx<'_>>
    where
        F: FnMut(&WalletTx, &WalletTx) -> Ordering,
    {
        let mut txs: Vec<WalletTx> = self.transactions().collect();
        txs.sort_unstable_by(compare);
        txs
    }

    /// Return the balance, separated into available, trusted-pending, untrusted-pending, and
    /// immature values.
    pub fn balance(&self) -> Balance {
        self.tx_graph.graph().balance(
            &self.chain,
            self.chain.tip().block_id(),
            CanonicalizationParams::default(),
            self.tx_graph.index.outpoints().iter().cloned(),
            |_, _| false,
        )
    }

    /// Return the secp256k1 context used for all signing operations.
    pub fn secp_ctx(&self) -> &SecpCtx {
        &self.secp
    }

    /// The derivation index of this wallet. It will return `None` if it has not derived any
    /// addresses. Otherwise, it will return the index of the highest address it has derived.
    ///
    /// Returns an error if `keychain` does not exist.
    pub fn derivation_index(&self, keychain: K) -> Result<Option<u32>, MissingKeychain> {
        let _ = self
            .tx_graph
            .index
            .get_descriptor(keychain.clone())
            .ok_or(MissingKeychain)?;
        Ok(self.tx_graph.index.last_revealed_index(keychain))
    }

    /// The index of the next address that you would get if you were to ask the wallet for a new
    /// address.
    ///
    /// Returns an error if `keychain` does not exist.
    pub fn next_derivation_index(&self, keychain: K) -> Result<u32, MissingKeychain> {
        Ok(self
            .tx_graph
            .index
            .next_index(keychain)
            .ok_or(MissingKeychain)?
            .0)
    }

    fn get_descriptor_for_txout(&self, txout: &TxOut) -> Option<DerivedDescriptor> {
        let &(ref keychain, child) = self
            .tx_graph
            .index
            .index_of_spk(txout.script_pubkey.clone())?;
        let descriptor = self
            .public_descriptor(keychain.clone())
            .expect("keychain must exist");
        descriptor.at_derivation_index(child).ok()
    }

    /// Get the corresponding PSBT Input for a [`LocalOutput`].
    pub fn get_psbt_input(
        &self,
        utxo: LocalOutput<K>,
        sighash_type: Option<psbt::PsbtSighashType>,
        only_witness_utxo: bool,
    ) -> Result<psbt::Input, CreateTxError> {
        // Try to find the prev_script in our db to figure out if this is internal or external,
        // and the derivation index.
        let &(ref keychain, child) = self
            .tx_graph
            .index
            .index_of_spk(utxo.txout.script_pubkey)
            .ok_or(CreateTxError::UnknownUtxo)?;

        let mut psbt_input = psbt::Input {
            sighash_type,
            ..psbt::Input::default()
        };

        let desc = self
            .public_descriptor(keychain.clone())
            .expect("keychain must exist.");
        let derived_descriptor = desc
            .at_derivation_index(child)
            .expect("child can't be hardened");

        psbt_input
            .update_with_descriptor_unchecked(&derived_descriptor)
            .map_err(MiniscriptPsbtError::Conversion)?;

        let prev_output = utxo.outpoint;
        if let Some(prev_tx) = self.tx_graph.graph().get_tx(prev_output.txid) {
            // We want to check that the prevout actually exists in the transaction before
            // continuing.
            let prevout = prev_tx.output.get(prev_output.vout as usize).ok_or(
                MiniscriptPsbtError::UtxoUpdate(miniscript::psbt::UtxoUpdateError::UtxoCheck),
            )?;
            if desc.is_witness() || desc.is_taproot() {
                psbt_input.witness_utxo = Some(prevout.clone());
            }
            if !desc.is_taproot() && (!desc.is_witness() || !only_witness_utxo) {
                psbt_input.non_witness_utxo = Some(prev_tx.as_ref().clone());
            }
        }
        Ok(psbt_input)
    }

    fn update_psbt_with_descriptor(&self, psbt: &mut Psbt) -> Result<(), MiniscriptPsbtError> {
        // We need to borrow `psbt` mutably within the loops, so we have to allocate a vec for all
        // the input utxos and outputs.
        let utxos = (0..psbt.inputs.len())
            .filter_map(|i| psbt.get_utxo_for(i).map(|utxo| (true, i, utxo)))
            .chain(
                psbt.unsigned_tx
                    .output
                    .iter()
                    .enumerate()
                    .map(|(i, out)| (false, i, out.clone())),
            )
            .collect::<Vec<_>>();

        // Try to figure out the keychain and derivation for every input and output.
        for (is_input, index, out) in utxos.into_iter() {
            if let Some(&(ref keychain, child)) =
                self.tx_graph.index.index_of_spk(out.script_pubkey)
            {
                let desc = self
                    .public_descriptor(keychain.clone())
                    .expect("keychain must exist");
                let desc = desc
                    .at_derivation_index(child)
                    .expect("child can't be hardened");

                if is_input {
                    psbt.update_input_with_descriptor(index, &desc)
                        .map_err(MiniscriptPsbtError::UtxoUpdate)?;
                } else {
                    psbt.update_output_with_descriptor(index, &desc)
                        .map_err(MiniscriptPsbtError::OutputUpdate)?;
                }
            }
        }

        Ok(())
    }

    /// Return the checksum of the public descriptor associated to the `keychain`.
    ///
    /// Internally calls [`Self::public_descriptor`] to fetch the right descriptor.
    ///
    /// Returns an error if `keychain` does not exist.
    pub fn descriptor_checksum(&self, keychain: K) -> Result<String, MissingKeychain> {
        Ok(self
            .public_descriptor(keychain)?
            .to_string()
            .split_once('#')
            .unwrap()
            .1
            .to_string())
    }

    /// Applies an update to the wallet and stages the changes (but does not persist them).
    ///
    /// Usually you create an `update` by interacting with some blockchain data source and inserting
    /// transactions related to your wallet into it.
    ///
    /// After applying updates you should persist the staged wallet changes. For an example of how
    /// to persist staged wallet changes see [`Wallet::reveal_next_address`].
    pub fn apply_update(&mut self, update: impl Into<Update<K>>) -> Result<(), CannotConnectError> {
        let Update {
            last_active_indices,
            tx_update,
            chain,
        } = update.into();

        let mut changeset = ChangeSet::default();

        if let Some(tip) = chain {
            changeset.merge(self.chain.apply_update(tip)?.into());
        }

        changeset.merge(
            self.tx_graph
                .index
                .reveal_to_target_multi(&last_active_indices)
                .into(),
        );

        changeset.merge(self.tx_graph.apply_update(tx_update).into());

        self.stage.merge(changeset);

        Ok(())
    }

    /// Applies an update to the wallet, stages the changes, and returns events.
    ///
    /// Usually you create an `update` by interacting with some blockchain data source and inserting
    /// transactions related to your wallet into it. Staged changes are NOT persisted.
    ///
    /// After applying updates you should process the events in your app before persisting the
    /// staged wallet changes. For an example of how to persist staged wallet changes see
    /// [`Wallet::reveal_next_address`].
    ///
    /// ```rust,no_run
    /// # use bitcoin::*;
    /// # use bdk_wallet::*;
    /// use bdk_wallet::WalletEvent;
    /// # let wallet_update = Update::default();
    /// # let mut wallet = doctest_wallet!();
    /// let events = wallet.apply_update_events(wallet_update)?;
    /// // Handle wallet relevant events from this update.
    /// events.iter().for_each(|event| {
    ///     match event {
    ///         // The chain tip changed.
    ///         WalletEvent::ChainTipChanged { old_tip, new_tip } => {
    ///             todo!() // handle event
    ///         }
    ///         // An unconfirmed tx is now confirmed in a block.
    ///         WalletEvent::TxConfirmed {
    ///             txid,
    ///             tx,
    ///             block_time,
    ///             old_block_time: None,
    ///         } => {
    ///             todo!() // handle event
    ///         }
    ///         // A confirmed tx is now confirmed in a new block (reorg).
    ///         WalletEvent::TxConfirmed {
    ///             txid,
    ///             tx,
    ///             block_time,
    ///             old_block_time: Some(old_block_time),
    ///         } => {
    ///             todo!() // handle event
    ///         }
    ///         // A new unconfirmed tx was seen in the mempool.
    ///         WalletEvent::TxUnconfirmed {
    ///             txid,
    ///             tx,
    ///             old_block_time: None,
    ///         } => {
    ///             todo!() // handle event
    ///         }
    ///         // A previously confirmed tx in now unconfirmed in the mempool (reorg).
    ///         WalletEvent::TxUnconfirmed {
    ///             txid,
    ///             tx,
    ///             old_block_time: Some(old_block_time),
    ///         } => {
    ///             todo!() // handle event
    ///         }
    ///         // An unconfirmed tx was replaced in the mempool (RBF or double spent input).
    ///         WalletEvent::TxReplaced {
    ///             txid,
    ///             tx,
    ///             conflicts,
    ///         } => {
    ///             todo!() // handle event
    ///         }
    ///         // An unconfirmed tx was dropped from the mempool (fee too low).
    ///         WalletEvent::TxDropped { txid, tx } => {
    ///             todo!() // handle event
    ///         }
    ///         _ => {
    ///             // unexpected event, do nothing
    ///         }
    ///     }
    ///     // take staged wallet changes
    ///     let staged = wallet.take_staged();
    ///     // persist staged changes
    /// });
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    /// [`TxBuilder`]: crate::TxBuilder
    pub fn apply_update_events(
        &mut self,
        update: impl Into<Update<K>>,
    ) -> Result<Vec<WalletEvent>, CannotConnectError> {
        self.events_helper(|wallet| wallet.apply_update(update))
    }

    /// Get a reference of the staged [`ChangeSet`] that is yet to be committed (if any).
    pub fn staged(&self) -> Option<&ChangeSet<K>> {
        if self.stage.is_empty() {
            None
        } else {
            Some(&self.stage)
        }
    }

    /// Get a mutable reference of the staged [`ChangeSet`] that is yet to be committed (if any).
    pub fn staged_mut(&mut self) -> Option<&mut ChangeSet<K>> {
        if self.stage.is_empty() {
            None
        } else {
            Some(&mut self.stage)
        }
    }

    /// Take the staged [`ChangeSet`] to be persisted now (if any).
    pub fn take_staged(&mut self) -> Option<ChangeSet<K>> {
        self.stage.take()
    }

    /// Get a reference to the inner [`TxGraph`].
    pub fn tx_graph(&self) -> &TxGraph<ConfirmationBlockTime> {
        self.tx_graph.graph()
    }

    /// Get a reference to the inner [`KeychainTxOutIndex`].
    pub fn spk_index(&self) -> &KeychainTxOutIndex<K> {
        &self.tx_graph.index
    }

    /// Get a reference to the inner [`LocalChain`].
    pub fn local_chain(&self) -> &LocalChain {
        &self.chain
    }

    /// List the locked outpoints.
    pub fn list_locked_outpoints(&self) -> impl Iterator<Item = OutPoint> + '_ {
        self.locked_outpoints.iter().copied()
    }

    /// List unspent outpoints that are currently locked.
    pub fn list_locked_unspent(&self) -> impl Iterator<Item = OutPoint> + '_ {
        self.list_unspent()
            .filter(|output| self.is_outpoint_locked(output.outpoint))
            .map(|output| output.outpoint)
    }

    /// Whether the `outpoint` is locked. See [`Wallet::lock_outpoint`] for more.
    pub fn is_outpoint_locked(&self, outpoint: OutPoint) -> bool {
        self.locked_outpoints.contains(&outpoint)
    }

    /// Lock a wallet output identified by the given `outpoint`.
    ///
    /// A locked UTXO will not be selected as an input to fund a transaction. This is useful
    /// for excluding or reserving candidate inputs during transaction creation.
    ///
    /// **You must persist the staged change for the lock status to be persistent**. To unlock a
    /// previously locked outpoint, see [`Wallet::unlock_outpoint`].
    pub fn lock_outpoint(&mut self, outpoint: OutPoint) {
        if self.locked_outpoints.insert(outpoint) {
            let changeset = locked_outpoints::ChangeSet {
                outpoints: [(outpoint, true)].into(),
            };
            self.stage.merge(changeset.into());
        }
    }

    /// Unlock the wallet output of the specified `outpoint`.
    ///
    /// **You must persist the staged change for the lock status to be persistent**.
    pub fn unlock_outpoint(&mut self, outpoint: OutPoint) {
        if self.locked_outpoints.remove(&outpoint) {
            let changeset = locked_outpoints::ChangeSet {
                outpoints: [(outpoint, false)].into(),
            };
            self.stage.merge(changeset.into());
        }
    }

    /// Introduces a `block` of `height` to the wallet, and tries to connect it to the
    /// `prev_blockhash` of the block's header.
    ///
    /// This is a convenience method that is equivalent to calling [`apply_block_connected_to`]
    /// with `prev_blockhash` and `height-1` as the `connected_to` parameter.
    ///
    /// [`apply_block_connected_to`]: Self::apply_block_connected_to
    pub fn apply_block(&mut self, block: &Block, height: u32) -> Result<(), CannotConnectError> {
        let connected_to = match height.checked_sub(1) {
            Some(prev_height) => BlockId {
                height: prev_height,
                hash: block.header.prev_blockhash,
            },
            None => BlockId {
                height,
                hash: block.block_hash(),
            },
        };
        self.apply_block_connected_to(block, height, connected_to)
            .map_err(|err| match err {
                ApplyHeaderError::InconsistentBlocks => {
                    unreachable!("connected_to is derived from the block so must be consistent")
                }
                ApplyHeaderError::CannotConnect(err) => err,
            })
    }

    /// Introduces a `block` of `height` to the wallet, and tries to connect it to the
    /// `prev_blockhash` of the block's header and returns events.
    ///
    /// This is a convenience method that is equivalent to calling
    /// [`apply_block_connected_to_events`] with `prev_blockhash` and `height-1` as the
    /// `connected_to` parameter.
    ///
    /// See [`apply_update_events`] for more information on the returned [`WalletEvent`]s.
    ///
    /// [`apply_block_connected_to_events`]: Self::apply_block_connected_to_events
    /// [`apply_update_events`]: Self::apply_update_events
    pub fn apply_block_events(
        &mut self,
        block: &Block,
        height: u32,
    ) -> Result<Vec<WalletEvent>, CannotConnectError> {
        self.events_helper(|wallet| wallet.apply_block(block, height))
    }

    /// Applies relevant transactions from `block` of `height` to the wallet, and connects the
    /// block to the internal chain.
    ///
    /// The `connected_to` parameter informs the wallet how this block connects to the internal
    /// [`LocalChain`]. Relevant transactions are filtered from the `block` and inserted into the
    /// internal [`TxGraph`].
    ///
    /// **WARNING**: You must persist the changes resulting from one or more calls to this method
    /// if you need the inserted block data to be reloaded after closing the wallet.
    /// See [`Wallet::reveal_next_address`].
    pub fn apply_block_connected_to(
        &mut self,
        block: &Block,
        height: u32,
        connected_to: BlockId,
    ) -> Result<(), ApplyHeaderError> {
        let mut changeset = ChangeSet::default();
        changeset.merge(
            self.chain
                .apply_header_connected_to(&block.header, height, connected_to)?
                .into(),
        );
        changeset.merge(self.tx_graph.apply_block_relevant(block, height).into());
        self.stage.merge(changeset);
        Ok(())
    }

    /// Applies relevant transactions from `block` of `height` to the wallet, connects the
    /// block to the internal chain and returns events.
    ///
    /// See [`apply_block_connected_to`] for more information.
    ///
    /// See [`apply_update_events`] for more information on the returned [`WalletEvent`]s.
    ///
    /// [`apply_block_connected_to`]: Self::apply_block_connected_to
    /// [`apply_update_events`]: Self::apply_update_events
    pub fn apply_block_connected_to_events(
        &mut self,
        block: &Block,
        height: u32,
        connected_to: BlockId,
    ) -> Result<Vec<WalletEvent>, ApplyHeaderError> {
        self.events_helper(|wallet| wallet.apply_block_connected_to(block, height, connected_to))
    }

    /// Apply relevant unconfirmed transactions to the wallet.
    ///
    /// Transactions that are not relevant are filtered out.
    ///
    /// This method takes in an iterator of `(tx, last_seen)` where `last_seen` is the timestamp of
    /// when the transaction was last seen in the mempool. This is used for conflict resolution
    /// when there are conflicting unconfirmed transactions in the mempool. The transaction with the
    /// later `last_seen` is prioritized.
    ///
    /// **WARNING**: You must persist the changes resulting from one or more calls to this method
    /// if you need the applied unconfirmed transactions to be reloaded after closing the wallet.
    /// See [`Wallet::reveal_next_address`].
    pub fn apply_unconfirmed_txs<T: Into<Arc<Transaction>>>(
        &mut self,
        unconfirmed_txs: impl IntoIterator<Item = (T, u64)>,
    ) {
        let changeset = self
            .tx_graph
            .batch_insert_relevant_unconfirmed(unconfirmed_txs);
        self.stage.merge(changeset.into())
    }

    /// Apply relevant unconfirmed transactions to the wallet and returns events.
    ///
    /// See [`apply_unconfirmed_txs`] for more information.
    ///
    /// See [`apply_update_events`] for more information on the returned [`WalletEvent`]s.
    ///
    /// [`apply_unconfirmed_txs`]: Self::apply_unconfirmed_txs
    /// [`apply_update_events`]: Self::apply_update_events
    pub fn apply_unconfirmed_txs_events<T: Into<Arc<Transaction>>>(
        &mut self,
        unconfirmed_txs: impl IntoIterator<Item = (T, u64)>,
    ) -> Vec<WalletEvent> {
        self.events_helper::<_, _, core::convert::Infallible>(|wallet| {
            wallet.apply_unconfirmed_txs(unconfirmed_txs);
            Ok(())
        })
        .expect("`apply_unconfirmed_txs` should not fail")
    }

    /// Apply evictions of the given transaction IDs with their associated timestamps.
    ///
    /// This function is used to mark specific unconfirmed transactions as evicted from the mempool.
    /// Eviction means that these transactions are not considered canonical by default, and will
    /// no longer be part of the wallet's [`transactions`] set. This can happen for example when
    /// a transaction is dropped from the mempool due to low fees or conflicts with another
    /// transaction.
    ///
    /// Only transactions that are currently unconfirmed and canonical are considered for eviction.
    /// Transactions that are not relevant to the wallet are ignored. Note that an evicted
    /// transaction can become canonical again if it is later observed on-chain or seen in the
    /// mempool with a higher priority (e.g., due to a fee bump).
    ///
    /// ## Parameters
    ///
    /// `evicted_txs`: An iterator of `(Txid, u64)` tuples, where:
    /// - `Txid`: The transaction ID of the transaction to be evicted.
    /// - `u64`: The timestamp indicating when the transaction was evicted from the mempool. This
    ///   will usually correspond to the time of the latest chain sync. See docs for
    ///   [`start_sync_with_revealed_spks`].
    ///
    /// ## Notes
    ///
    /// - Not all blockchain backends support automatic mempool eviction handling - this method may
    ///   be used in such cases. It can also be used to negate the effect of
    ///   [`apply_unconfirmed_txs`] for a particular transaction without the need for an additional
    ///   sync.
    /// - The changes are staged in the wallet's internal state and must be persisted to ensure they
    ///   are retained across wallet restarts. Use [`Wallet::take_staged`] to retrieve the staged
    ///   changes and persist them to your database of choice.
    /// - Evicted transactions are removed from the wallet's canonical transaction set, but the data
    ///   remains in the wallet's internal transaction graph for historical purposes.
    /// - Ensure that the timestamps provided are accurate and monotonically increasing, as they
    ///   influence the wallet's canonicalization logic.
    ///
    /// [`transactions`]: Wallet::transactions
    /// [`apply_unconfirmed_txs`]: Wallet::apply_unconfirmed_txs
    /// [`start_sync_with_revealed_spks`]: Wallet::start_sync_with_revealed_spks
    pub fn apply_evicted_txs(&mut self, evicted_txs: impl IntoIterator<Item = (Txid, u64)>) {
        let chain = &self.chain;
        let canon_txids: BTreeSet<Txid> = self
            .tx_graph
            .graph()
            .list_canonical_txs(
                chain,
                chain.tip().block_id(),
                CanonicalizationParams::default(),
            )
            .map(|c_tx| c_tx.tx_node.txid)
            .collect();
        let changeset = self.tx_graph.batch_insert_relevant_evicted_at(
            evicted_txs
                .into_iter()
                .filter(|(txid, _)| canon_txids.contains(txid)),
        );
        self.stage.merge(changeset.into())
    }

    /// Apply evictions of the given transaction IDs with their associated timestamps and returns
    /// events.
    ///
    /// See [`apply_evicted_txs`] for more information.
    ///
    /// See [`apply_update_events`] for more information on the returned [`WalletEvent`]s.
    ///
    /// [`apply_evicted_txs`]: Self::apply_evicted_txs
    /// [`apply_update_events`]: Self::apply_update_events
    pub fn apply_evicted_txs_events(
        &mut self,
        evicted_txs: impl IntoIterator<Item = (Txid, u64)>,
    ) -> Vec<WalletEvent> {
        self.events_helper::<_, _, core::convert::Infallible>(|wallet| {
            wallet.apply_evicted_txs(evicted_txs);
            Ok(())
        })
        .expect("`apply_evicted_txs` should not fail")
    }

    /// Generates wallet events by executing a wallet-mutating function and surfacing internal
    /// state changes.
    ///
    /// It works by taking some wallet operation that modifies state, capturing "before" and "after"
    /// snapshots of the wallet's chain tip and transactions and comparing them in order to
    /// generate a list of [`WalletEvent`]s representing what changed.
    ///
    /// Common kinds of events include:
    ///
    /// - [`WalletEvent::ChainTipChanged`]: The blockchain tip changed
    /// - [`WalletEvent::TxConfirmed`]: A transaction was confirmed in a block
    /// - [`WalletEvent::TxUnconfirmed`]: A transaction was newly unconfirmed
    /// - [`WalletEvent::TxReplaced`]: An unconfirmed transaction was replaced (e.g., via RBF)
    /// - [`WalletEvent::TxDropped`]: An unconfirmed transaction was dropped from the mempool
    ///
    /// This is useful when you need to track specific changes to your wallet state, such
    /// as updating a UI to reflect transaction status changes, triggering notifications when
    /// transactions confirm, logging state changes for debugging or auditing, or responding to
    /// chain reorganizations.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use bdk_chain::local_chain::CannotConnectError;
    /// # use bdk_wallet::{Wallet, Update, WalletEvent, KeychainKind};
    /// # let mut wallet: Wallet<KeychainKind> = todo!();
    /// // Apply an update and get events describing what changed
    /// let update = Update::default();
    /// let func = |wallet: &mut Wallet<KeychainKind>| wallet.apply_update(update);
    /// let events = wallet.events_helper(func)?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// If `f` returns an error, then returns `E` of a type defined by the function
    /// passed in.
    pub fn events_helper<F, T, E>(&mut self, f: F) -> Result<Vec<WalletEvent>, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: Debug + Display,
    {
        // Snapshot of chain tip and transactions before
        let chain_tip1 = self.chain.tip().block_id();
        let wallet_txs1 = self.map_transactions();

        // Call `f` on self
        f(self)?;

        // Chain tip and transactions after
        let chain_tip2 = self.chain.tip().block_id();
        let wallet_txs2 = self.map_transactions();

        Ok(wallet_events(
            self,
            chain_tip1,
            chain_tip2,
            wallet_txs1,
            wallet_txs2,
        ))
    }

    /// Returns a map of canonical transactions keyed by txid.
    ///
    /// This is used internally to help generate [`WalletEvent`]s.
    fn map_transactions(
        &self,
    ) -> BTreeMap<Txid, (Arc<Transaction>, ChainPosition<ConfirmationBlockTime>)> {
        self.transactions()
            .map(|wtx| {
                (
                    wtx.tx_node.txid,
                    (wtx.tx_node.tx.clone(), wtx.chain_position),
                )
            })
            .collect()
    }

    /// Returns the descriptor used to create addresses for a particular `keychain`.
    ///
    /// It's the "public" version of the wallet's descriptor, meaning a new descriptor that has
    /// the same structure but with the all secret keys replaced by their corresponding public key.
    /// This can be used to build a watch-only version of a wallet.
    pub fn public_descriptor(&self, keychain: K) -> Result<&ExtendedDescriptor, MissingKeychain> {
        self.tx_graph
            .index
            .get_descriptor(keychain)
            .ok_or(MissingKeychain)
    }
}

impl Wallet<KeychainKind> {
    fn complete_transaction(
        &self,
        tx: Transaction,
        selected: Vec<Utxo>,
        params: TxParams,
    ) -> Result<Psbt, CreateTxError> {
        let mut psbt = Psbt::from_unsigned_tx(tx)?;

        if params.add_global_xpubs {
            let all_xpubs = self
                .keychains()
                .flat_map(|(_, desc)| desc.get_extended_keys())
                .collect::<Vec<_>>();

            for xpub in all_xpubs {
                let origin = match xpub.origin {
                    Some(origin) => origin,
                    None if xpub.xkey.depth == 0 => {
                        (xpub.root_fingerprint(&self.secp), vec![].into())
                    }
                    _ => return Err(CreateTxError::MissingKeyOrigin(xpub.xkey.to_string())),
                };

                psbt.xpub.insert(xpub.xkey, origin);
            }
        }

        let mut lookup_output = selected
            .into_iter()
            .map(|utxo| (utxo.outpoint(), utxo))
            .collect::<HashMap<_, _>>();

        // Add metadata for the inputs.
        for (psbt_input, input) in psbt.inputs.iter_mut().zip(psbt.unsigned_tx.input.iter()) {
            let utxo = match lookup_output.remove(&input.previous_output) {
                Some(utxo) => utxo,
                None => continue,
            };

            match utxo {
                Utxo::Local(utxo) => {
                    *psbt_input =
                        match self.get_psbt_input(utxo, params.sighash, params.only_witness_utxo) {
                            Ok(psbt_input) => psbt_input,
                            Err(e) => match e {
                                CreateTxError::UnknownUtxo => psbt::Input {
                                    sighash_type: params.sighash,
                                    ..psbt::Input::default()
                                },
                                _ => return Err(e),
                            },
                        }
                }
                Utxo::Foreign {
                    outpoint,
                    psbt_input: foreign_psbt_input,
                    ..
                } => {
                    let is_taproot = foreign_psbt_input
                        .witness_utxo
                        .as_ref()
                        .map(|txout| txout.script_pubkey.is_p2tr())
                        .unwrap_or(false);
                    if !is_taproot
                        && !params.only_witness_utxo
                        && foreign_psbt_input.non_witness_utxo.is_none()
                    {
                        return Err(CreateTxError::MissingNonWitnessUtxo(outpoint));
                    }
                    *psbt_input = *foreign_psbt_input;
                }
            }
        }

        self.update_psbt_with_descriptor(&mut psbt)?;

        Ok(psbt)
    }

    /// Given the options returns the list of utxos that must be used to form the
    /// transaction and any further that may be used if needed.
    fn filter_utxos(&self, params: &TxParams, current_height: u32) -> Vec<WeightedUtxo> {
        if params.manually_selected_only {
            vec![]
        // Only process optional UTxOs if manually_selected_only is false.
        } else {
            let manually_selected_outpoints = params
                .utxos
                .iter()
                .map(|wutxo| wutxo.utxo.outpoint())
                .collect::<HashSet<OutPoint>>();
            self.tx_graph
                .graph()
                // Get all unspent UTxOs from wallet.
                // NOTE: the UTxOs returned by the following method already belong to wallet as the
                // call chain uses get_tx_node infallibly.
                .filter_chain_unspents(
                    &self.chain,
                    self.chain.tip().block_id(),
                    CanonicalizationParams::default(),
                    self.tx_graph.index.outpoints().iter().cloned(),
                )
                // Filter out locked outpoints.
                .filter(|(_, txo)| !self.is_outpoint_locked(txo.outpoint))
                // Only create LocalOutput if UTxO is mature.
                .filter_map(move |((k, i), full_txo)| {
                    full_txo
                        .is_mature(current_height)
                        .then(|| new_local_utxo(k, i, full_txo))
                })
                // only process UTXOs not selected manually, they will be considered later in the
                // chain
                // NOTE: this avoid UTXOs in both required and optional list
                .filter(|may_spend| !manually_selected_outpoints.contains(&may_spend.outpoint))
                // only add to optional UTxOs those which satisfy the change policy if we reuse
                // change
                .filter(|local_output| {
                    self.keychains().count() == 1
                        || params.change_policy.is_satisfied_by(local_output)
                })
                // Only add to optional UTxOs those marked as spendable.
                .filter(|local_output| !params.unspendable.contains(&local_output.outpoint))
                // If bumping fees only add to optional UTxOs those confirmed.
                .filter(|local_output| {
                    params.bumping_fee.is_none() || local_output.chain_position.is_confirmed()
                })
                .map(|utxo| WeightedUtxo {
                    satisfaction_weight: self
                        .public_descriptor(utxo.keychain)
                        .expect("keychain must exist.")
                        .max_weight_to_satisfy()
                        .unwrap(),
                    utxo: Utxo::Local(utxo),
                })
                .collect()
        }
    }

    /// Start building a transaction.
    ///
    /// This returns a blank [`TxBuilder`] from which you can specify the parameters for the
    /// transaction.
    ///
    /// ## Example
    ///
    /// ```
    /// # use std::str::FromStr;
    /// # use bitcoin::*;
    /// # use bdk_wallet::*;
    /// # use bdk_wallet::ChangeSet;
    /// # use bdk_wallet::error::CreateTxError;
    /// # use anyhow::Error;
    /// # let descriptor = "wpkh(tpubD6NzVbkrYhZ4Xferm7Pz4VnjdcDPFyjVu5K4iZXQ4pVN8Cks4pHVowTBXBKRhX64pkRyJZJN5xAKj4UDNnLPb5p2sSKXhewoYx5GbTdUFWq/*)";
    /// # let mut wallet = doctest_wallet!();
    /// # let to_address = Address::from_str("2N4eQYCbKUHCCTUjBJeHcJp9ok6J2GZsTDt").unwrap().assume_checked();
    /// let psbt = {
    ///    let mut builder =  wallet.build_tx();
    ///    builder
    ///        .add_recipient(to_address.script_pubkey(), Amount::from_sat(50_000));
    ///    builder.finish()?
    /// };
    ///
    /// // sign and broadcast ...
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    ///
    /// [`TxBuilder`]: crate::TxBuilder
    pub fn build_tx(&mut self) -> TxBuilder<'_, DefaultCoinSelectionAlgorithm> {
        TxBuilder {
            wallet: self,
            params: TxParams::default(),
            coin_selection: DefaultCoinSelectionAlgorithm::default(),
        }
    }

    pub(crate) fn create_tx<Cs: coin_selection::CoinSelectionAlgorithm>(
        &mut self,
        coin_selection: Cs,
        params: TxParams,
        rng: &mut impl RngCore,
    ) -> Result<Psbt, CreateTxError> {
        let keychains: BTreeMap<_, _> = self.tx_graph.index.keychains().collect();
        let external_descriptor = keychains.get(&KeychainKind::External).expect("must exist");
        let internal_descriptor = keychains.get(&KeychainKind::Internal);

        let external_policy = external_descriptor
            .extract_policy(
                &SignersContainer::default(),
                BuildSatisfaction::None,
                &self.secp,
            )?
            .unwrap();
        let internal_policy = internal_descriptor
            .map(|desc| {
                Ok::<_, CreateTxError>(
                    desc.extract_policy(
                        &SignersContainer::default(),
                        BuildSatisfaction::None,
                        &self.secp,
                    )?
                    .unwrap(),
                )
            })
            .transpose()?;

        // The policy allows spending external outputs, but it requires a policy path that hasn't
        // been provided
        if params.change_policy != tx_builder::ChangeSpendPolicy::OnlyChange
            && external_policy.requires_path()
            && params.external_policy_path.is_none()
        {
            return Err(CreateTxError::SpendingPolicyRequired(
                KeychainKind::External,
            ));
        };
        // Same for the internal_policy path
        if let Some(internal_policy) = &internal_policy {
            if params.change_policy != tx_builder::ChangeSpendPolicy::ChangeForbidden
                && internal_policy.requires_path()
                && params.internal_policy_path.is_none()
            {
                return Err(CreateTxError::SpendingPolicyRequired(
                    KeychainKind::Internal,
                ));
            };
        }

        let external_requirements = external_policy.get_condition(
            params
                .external_policy_path
                .as_ref()
                .unwrap_or(&BTreeMap::new()),
        )?;
        let internal_requirements = internal_policy
            .map(|policy| {
                Ok::<_, CreateTxError>(
                    policy.get_condition(
                        params
                            .internal_policy_path
                            .as_ref()
                            .unwrap_or(&BTreeMap::new()),
                    )?,
                )
            })
            .transpose()?;

        let requirements =
            external_requirements.merge(&internal_requirements.unwrap_or_default())?;

        let version = match params.version {
            Some(transaction::Version(0)) => return Err(CreateTxError::Version0),
            Some(transaction::Version::ONE) if requirements.csv.is_some() => {
                return Err(CreateTxError::Version1Csv)
            }
            Some(v) => v,
            None => transaction::Version::TWO,
        };

        // We use a match here instead of a unwrap_or_else as it's way more readable :)
        let current_height = match params.current_height {
            // If they didn't tell us the current height, we assume it's the latest sync height.
            None => {
                let tip_height = self.chain.tip().height();
                absolute::LockTime::from_height(tip_height).expect("invalid height")
            }
            Some(h) => h,
        };

        let lock_time = match params.locktime {
            // When no `nLockTime` is specified, we try to prevent fee sniping, if possible.
            None => {
                // Fee sniping can be partially prevented by setting the timelock
                // to current_height. If we don't know the current_height,
                // we default to 0.
                let fee_sniping_height = current_height;

                // We choose the biggest between the required nlocktime and the fee sniping
                // height.
                match requirements.timelock {
                    // No requirement, just use the fee_sniping_height.
                    None => fee_sniping_height,
                    // There's a block-based requirement, but the value is lower than the
                    // fee_sniping_height.
                    Some(value @ absolute::LockTime::Blocks(_)) if value < fee_sniping_height => {
                        fee_sniping_height
                    }
                    // There's a time-based requirement or a block-based requirement greater
                    // than the fee_sniping_height use that value.
                    Some(value) => value,
                }
            }
            // Specific nLockTime required and we have no constraints, so just set to that value.
            Some(x) if requirements.timelock.is_none() => x,
            // Specific nLockTime required and it's compatible with the constraints.
            Some(x)
                if requirements.timelock.unwrap().is_same_unit(x)
                    && x >= requirements.timelock.unwrap() =>
            {
                x
            }
            // Invalid nLockTime required.
            Some(x) => {
                return Err(CreateTxError::LockTime {
                    requested: x,
                    required: requirements.timelock.unwrap(),
                })
            }
        };

        // nSequence value for inputs.
        // When not explicitly specified, it defaults to 0xFFFFFFFD, meaning RBF signaling is
        // enabled.
        let n_sequence = match (params.sequence, requirements.csv) {
            // Enable RBF by default.
            (None, None) => Sequence::ENABLE_RBF_NO_LOCKTIME,
            // None requested, use required.
            (None, Some(csv)) => csv,
            // Requested sequence is incompatible with requirements.
            (Some(sequence), Some(csv)) if !check_nsequence_rbf(sequence, csv) => {
                return Err(CreateTxError::RbfSequenceCsv { sequence, csv })
            }
            // Use requested nSequence value.
            (Some(sequence), _) => sequence,
        };

        let (fee_rate, mut fee_amount) = match params.fee_policy.unwrap_or_default() {
            //FIXME: see https://github.com/bitcoindevkit/bdk/issues/256
            FeePolicy::FeeAmount(fee) => {
                if let Some(previous_fee) = params.bumping_fee {
                    if fee < previous_fee.absolute {
                        return Err(CreateTxError::FeeTooLow {
                            required: previous_fee.absolute,
                        });
                    }
                }
                (FeeRate::ZERO, fee)
            }
            FeePolicy::FeeRate(rate) => {
                if let Some(previous_fee) = params.bumping_fee {
                    let required_feerate = FeeRate::from_sat_per_kwu(
                        previous_fee.rate.to_sat_per_kwu()
                            + FeeRate::BROADCAST_MIN.to_sat_per_kwu(), // +1 sat/vb
                    );
                    if rate < required_feerate {
                        return Err(CreateTxError::FeeRateTooLow {
                            required: required_feerate,
                        });
                    }
                }
                (rate, Amount::ZERO)
            }
        };

        let mut tx = Transaction {
            version,
            lock_time,
            input: vec![],
            output: vec![],
        };

        if params.manually_selected_only && params.utxos.is_empty() {
            return Err(CreateTxError::NoUtxosSelected);
        }

        let mut outgoing = Amount::ZERO;
        let recipients = params.recipients.iter().map(|(r, v)| (r, *v));

        for (index, (script_pubkey, value)) in recipients.enumerate() {
            if !params.allow_dust && value.is_dust(script_pubkey) && !script_pubkey.is_op_return() {
                return Err(CreateTxError::OutputBelowDustLimit(index));
            }

            let new_out = TxOut {
                script_pubkey: script_pubkey.clone(),
                value,
            };

            tx.output.push(new_out);

            outgoing += value;
        }

        fee_amount += fee_rate * tx.weight();

        let (required_utxos, optional_utxos) = {
            // NOTE: manual selection overrides unspendable
            let mut required: Vec<WeightedUtxo> = params.utxos.clone();
            let optional = self.filter_utxos(&params, current_height.to_consensus_u32());

            // If `drain_wallet` is true, all UTxOs are required.
            if params.drain_wallet {
                required.extend(optional);
                (required, vec![])
            } else {
                (required, optional)
            }
        };

        // Get drain script.
        let mut drain_index = Option::<(KeychainKind, u32)>::None;
        let drain_script = match params.drain_to {
            Some(ref drain_recipient) => drain_recipient.clone(),
            None => {
                let change_keychain = match self.public_descriptor(KeychainKind::Internal) {
                    Ok(_) => KeychainKind::Internal,
                    Err(_) => KeychainKind::External,
                };
                let (index, spk) = self
                    .tx_graph
                    .index
                    .unused_keychain_spks(change_keychain)
                    .next()
                    .unwrap_or_else(|| {
                        let (next_index, _) = self
                            .tx_graph
                            .index
                            .next_index(change_keychain)
                            .expect("keychain must exist");
                        let spk = self
                            .peek_address(change_keychain, next_index)
                            .expect("keychain must exist")
                            .expect("index must be in bounds")
                            .script_pubkey();
                        (next_index, spk)
                    });
                drain_index = Some((change_keychain, index));
                spk
            }
        };

        let coin_selection = coin_selection
            .coin_select(
                required_utxos,
                optional_utxos,
                fee_rate,
                outgoing + fee_amount,
                &drain_script,
                rng,
            )
            .map_err(CreateTxError::CoinSelection)?;

        let excess = &coin_selection.excess;
        tx.input = coin_selection
            .selected
            .iter()
            .map(|u| bitcoin::TxIn {
                previous_output: u.outpoint(),
                script_sig: ScriptBuf::default(),
                sequence: u.sequence().unwrap_or(n_sequence),
                witness: Witness::new(),
            })
            .collect();

        if tx.output.is_empty() {
            // Uh oh, our transaction has no outputs.
            // We allow this when we have a `drain_to` address and either:
            // - `drain_wallet` is enabled
            // - there are UTXOs we must spend (this happens, for example, when
            // sweeping specific UTXOs to a given address)
            // Otherwise, we don't know who we should send the funds to, and how much
            // we should send!
            if params.drain_to.is_some() && (params.drain_wallet || !params.utxos.is_empty()) {
                if let Excess::NoChange {
                    dust_threshold,
                    remaining_amount,
                    change_fee,
                } = excess
                {
                    return Err(CreateTxError::CoinSelection(InsufficientFunds {
                        needed: *dust_threshold,
                        available: remaining_amount
                            .checked_sub(*change_fee)
                            .unwrap_or_default(),
                    }));
                }
            } else {
                return Err(CreateTxError::NoRecipients);
            }
        }

        // If there's change, create and add a change output.
        if let Excess::Change { amount, .. } = excess {
            // Create drain output.
            let drain_output = TxOut {
                value: *amount,
                script_pubkey: drain_script,
            };

            // TODO: We should pay attention when adding a new output: this might increase
            // the length of the "number of vouts" parameter by 2 bytes, potentially making
            // our feerate too low.
            tx.output.push(drain_output);
        }

        // Sort inputs/outputs according to the chosen algorithm.
        params.ordering.sort_tx_with_aux_rand(&mut tx, rng);

        let psbt = self.complete_transaction(tx, coin_selection.selected, params)?;

        // Recording changes to the change keychain.
        if let (Excess::Change { .. }, Some((keychain, index))) = (excess, drain_index) {
            if let Some((_, index_changeset)) =
                self.tx_graph.index.reveal_to_target(keychain, index)
            {
                self.stage.merge(index_changeset.into());
                self.mark_used(keychain, index)
                    .expect("keychain must exist");
            }
        }

        Ok(psbt)
    }

    /// Bump the fee of a transaction previously created with this wallet.
    ///
    /// Returns an error if the transaction is already confirmed or doesn't explicitly signal
    /// *replace by fee* (RBF). If the transaction can be fee bumped then it returns a [`TxBuilder`]
    /// pre-populated with the inputs and outputs of the original transaction.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// # // TODO: remove norun -- bumping fee seems to need the tx in the wallet database first.
    /// # use std::str::FromStr;
    /// # use bitcoin::*;
    /// # use bdk_wallet::*;
    /// # use bdk_wallet::ChangeSet;
    /// # use bdk_wallet::error::CreateTxError;
    /// # use anyhow::Error;
    /// # let descriptor = "wpkh(tpubD6NzVbkrYhZ4Xferm7Pz4VnjdcDPFyjVu5K4iZXQ4pVN8Cks4pHVowTBXBKRhX64pkRyJZJN5xAKj4UDNnLPb5p2sSKXhewoYx5GbTdUFWq/*)";
    /// # let mut wallet = doctest_wallet!();
    /// # let to_address = Address::from_str("2N4eQYCbKUHCCTUjBJeHcJp9ok6J2GZsTDt").unwrap().assume_checked();
    /// # let external_signers = signer::SignersContainer::default();
    /// # let internal_signers = signer::SignersContainer::default();
    /// let mut psbt = {
    ///     let mut builder = wallet.build_tx();
    ///     builder
    ///         .add_recipient(to_address.script_pubkey(), Amount::from_sat(50_000));
    ///     builder.finish()?
    /// };
    /// let _ = wallet.sign_with_signers(&mut psbt, &[&external_signers, &internal_signers], SignOptions::default())?;
    /// let tx = psbt.clone().extract_tx().expect("tx");
    /// // broadcast tx but it's taking too long to confirm so we want to bump the fee
    /// let mut psbt =  {
    ///     let mut builder = wallet.build_fee_bump(tx.compute_txid())?;
    ///     builder
    ///         .fee_rate(FeeRate::from_sat_per_vb(5).expect("valid feerate"));
    ///     builder.finish()?
    /// };
    ///
    /// let _ = wallet.sign_with_signers(&mut psbt, &[&external_signers, &internal_signers], SignOptions::default())?;
    /// let fee_bumped_tx = psbt.extract_tx();
    /// // broadcast fee_bumped_tx to replace original
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    // TODO: support for merging multiple transactions while bumping the fees
    pub fn build_fee_bump(
        &mut self,
        txid: Txid,
    ) -> Result<TxBuilder<'_, DefaultCoinSelectionAlgorithm>, BuildFeeBumpError> {
        let tx_graph = self.tx_graph.graph();
        let txout_index = &self.tx_graph.index;
        let chain_tip = self.chain.tip().block_id();
        let chain_positions: HashMap<Txid, ChainPosition<_>> = tx_graph
            .list_canonical_txs(&self.chain, chain_tip, CanonicalizationParams::default())
            .map(|canon_tx| (canon_tx.tx_node.txid, canon_tx.chain_position))
            .collect();

        let mut tx = tx_graph
            .get_tx(txid)
            .ok_or(BuildFeeBumpError::TransactionNotFound(txid))?
            .as_ref()
            .clone();

        if chain_positions
            .get(&txid)
            .ok_or(BuildFeeBumpError::TransactionNotFound(txid))?
            .is_confirmed()
        {
            return Err(BuildFeeBumpError::TransactionConfirmed(txid));
        }

        if !tx
            .input
            .iter()
            .any(|txin| txin.sequence.to_consensus_u32() <= 0xFFFFFFFD)
        {
            return Err(BuildFeeBumpError::IrreplaceableTransaction(
                tx.compute_txid(),
            ));
        }

        let fee = self
            .calculate_fee(&tx)
            .map_err(|_| BuildFeeBumpError::FeeRateUnavailable)?;
        let fee_rate = fee / tx.weight();

        // Remove the inputs from the tx and process them.
        let utxos: Vec<WeightedUtxo> = tx
            .input
            .drain(..)
            .map(|txin| -> Result<_, BuildFeeBumpError> {
                let outpoint = txin.previous_output;
                let prev_txout = tx_graph
                    .get_txout(outpoint)
                    .cloned()
                    .ok_or(BuildFeeBumpError::UnknownUtxo(outpoint))?;
                match txout_index.index_of_spk(prev_txout.script_pubkey.clone()) {
                    Some(&(keychain, derivation_index)) => {
                        let txout = prev_txout;
                        let chain_position = chain_positions
                            .get(&outpoint.txid)
                            .cloned()
                            .ok_or(BuildFeeBumpError::TransactionNotFound(outpoint.txid))?;
                        Ok(WeightedUtxo {
                            satisfaction_weight: self
                                .public_descriptor(keychain)
                                .expect("keychain must exist")
                                .max_weight_to_satisfy()
                                .expect("descriptor should be satisfiable"),
                            utxo: Utxo::Local(LocalOutput {
                                outpoint,
                                txout,
                                keychain,
                                is_spent: true,
                                derivation_index,
                                chain_position,
                            }),
                        })
                    }
                    None => Ok(WeightedUtxo {
                        satisfaction_weight: Weight::from_wu_usize(
                            serialize(&txin.script_sig).len() * 4 + serialize(&txin.witness).len(),
                        ),
                        utxo: Utxo::Foreign {
                            outpoint,
                            sequence: txin.sequence,
                            psbt_input: Box::new(psbt::Input {
                                witness_utxo: prev_txout
                                    .script_pubkey
                                    .witness_version()
                                    .map(|_| prev_txout),
                                non_witness_utxo: tx_graph
                                    .get_tx(outpoint.txid)
                                    .map(|tx| tx.as_ref().clone()),
                                ..Default::default()
                            }),
                        },
                    }),
                }
            })
            .collect::<Result<_, _>>()?;

        if tx.output.len() > 1 {
            let mut change_index = None;
            for (index, txout) in tx.output.iter().enumerate() {
                let change_keychain = match self.public_descriptor(KeychainKind::Internal) {
                    Ok(_) => KeychainKind::Internal,
                    Err(_) => KeychainKind::External,
                };
                match txout_index.index_of_spk(txout.script_pubkey.clone()) {
                    Some((keychain, _)) if *keychain == change_keychain => {
                        change_index = Some(index)
                    }
                    _ => {}
                }
            }

            if let Some(change_index) = change_index {
                tx.output.remove(change_index);
            }
        }

        let params = TxParams {
            version: Some(tx.version),
            recipients: tx
                .output
                .into_iter()
                .map(|txout| (txout.script_pubkey, txout.value))
                .collect(),
            utxos,
            bumping_fee: Some(tx_builder::PreviousFee {
                absolute: fee,
                rate: fee_rate,
            }),
            ..Default::default()
        };

        Ok(TxBuilder {
            wallet: self,
            params,
            coin_selection: DefaultCoinSelectionAlgorithm::default(),
        })
    }

    /// Finalize a PSBT, i.e., for each input determine if sufficient data is available to pass
    /// validation and construct the respective `scriptSig` or `scriptWitness`. Please refer to
    /// [BIP174](https://github.com/bitcoin/bips/blob/master/bip-0174.mediawiki#Input_Finalizer),
    /// and [BIP371](https://github.com/bitcoin/bips/blob/master/bip-0371.mediawiki)
    /// for further information.
    ///
    /// Returns `true` if the PSBT could be finalized, and `false` otherwise.
    ///
    /// The [`SignOptions`] can be used to tweak the behavior of the finalizer.
    pub fn finalize_psbt(
        &self,
        psbt: &mut Psbt,
        sign_options: SignOptions,
    ) -> Result<bool, SignerError> {
        let tx = &psbt.unsigned_tx;
        let chain_tip = self.chain.tip().block_id();
        let prev_txids = tx
            .input
            .iter()
            .map(|txin| txin.previous_output.txid)
            .collect::<HashSet<Txid>>();
        let confirmation_heights = self
            .tx_graph
            .graph()
            .list_canonical_txs(&self.chain, chain_tip, CanonicalizationParams::default())
            .filter(|canon_tx| prev_txids.contains(&canon_tx.tx_node.txid))
            // This is for a small performance gain. Although `.filter` filters out excess txs, it
            // will still consume the internal `CanonicalIter` entirely. Having a `.take` here
            // allows us to stop further unnecessary canonicalization.
            .take(prev_txids.len())
            .map(|canon_tx| {
                let txid = canon_tx.tx_node.txid;
                match canon_tx.chain_position {
                    ChainPosition::Confirmed { anchor, .. } => (txid, anchor.block_id.height),
                    ChainPosition::Unconfirmed { .. } => (txid, u32::MAX),
                }
            })
            .collect::<HashMap<Txid, u32>>();

        let mut finished = true;

        for (n, input) in tx.input.iter().enumerate() {
            let psbt_input = &psbt
                .inputs
                .get(n)
                .ok_or(IndexOutOfBoundsError::new(n, psbt.inputs.len()))?;
            if psbt_input.final_script_sig.is_some() || psbt_input.final_script_witness.is_some() {
                continue;
            }
            let confirmation_height = confirmation_heights
                .get(&input.previous_output.txid)
                .copied();
            let current_height = sign_options
                .assume_height
                .unwrap_or_else(|| self.chain.tip().height());

            // - Try to derive the descriptor by looking at the txout. If it's in our database, we
            //   know exactly which `keychain` to use, and which derivation index it is.
            // - If that fails, try to derive it by looking at the psbt input: the complete logic is
            //   in `src/descriptor/mod.rs`, but it will basically look at `bip32_derivation`,
            //   `redeem_script` and `witness_script` to determine the right derivation.
            // - If that also fails, it will try it on the internal descriptor, if present.
            let desc = psbt
                .get_utxo_for(n)
                .and_then(|txout| self.get_descriptor_for_txout(&txout))
                .or_else(|| {
                    self.tx_graph.index.keychains().find_map(|(_, desc)| {
                        desc.derive_from_psbt_input(psbt_input, psbt.get_utxo_for(n), &self.secp)
                    })
                });

            match desc {
                Some(desc) => {
                    let mut tmp_input = bitcoin::TxIn::default();
                    match desc.satisfy(
                        &mut tmp_input,
                        (
                            PsbtInputSatisfier::new(psbt, n),
                            After::new(Some(current_height), false),
                            Older::new(Some(current_height), confirmation_height, false),
                        ),
                    ) {
                        Ok(_) => {
                            let length = psbt.inputs.len();
                            // Set the UTXO fields, final script_sig and witness
                            // and clear everything else.
                            let psbt_input = psbt
                                .inputs
                                .get_mut(n)
                                .ok_or(IndexOutOfBoundsError::new(n, length))?;
                            let original = mem::take(psbt_input);
                            psbt_input.non_witness_utxo = original.non_witness_utxo;
                            psbt_input.witness_utxo = original.witness_utxo;
                            if !tmp_input.script_sig.is_empty() {
                                psbt_input.final_script_sig = Some(tmp_input.script_sig);
                            }
                            if !tmp_input.witness.is_empty() {
                                psbt_input.final_script_witness = Some(tmp_input.witness);
                            }
                        }
                        Err(_) => finished = false,
                    }
                }
                None => finished = false,
            }
        }

        // Clear derivation paths from outputs.
        if finished {
            for output in &mut psbt.outputs {
                output.bip32_derivation.clear();
                output.tap_key_origins.clear();
            }
        }

        Ok(finished)
    }

    /// Sign a transaction with the provided signer containers.
    ///
    /// Signer containers are processed in the order provided. Signers inside each container are
    /// processed according to their [`signer::SignerOrdering`].
    ///
    /// The [`SignOptions`] can be used to tweak the behavior of the software signers, and the way
    /// the transaction is finalized at the end. Note that it can't be guaranteed that *every*
    /// signer will follow the options, but the "software signers" (WIF keys and `xprv`) defined
    /// in this library will.
    ///
    /// Returns true if the PSBT was finalized, or false otherwise.
    ///
    /// ## Example
    ///
    /// ```
    /// # use bdk_wallet::*;
    /// # use bdk_wallet::bitcoin::*;
    /// # use bdk_wallet::bitcoin::{NetworkKind, secp256k1::Secp256k1};
    /// # use bdk_wallet::descriptor::IntoWalletDescriptor;
    /// # use bdk_wallet::signer::SignersContainer;
    /// # let mut wallet = doctest_wallet!();
    /// let signer_descriptor = "tr([73c5da0a/86'/0'/0']tprv8fMn4hSKPRC1oaCPqxDb1JWtgkpeiQvZhsr8W2xuy3GEMkzoArcAWTfJxYb6Wj8XNNDWEjfYKK4wGQXh3ZUXhDF2NcnsALpWTeSwarJt7Vc/0/*)";
    /// let secp = Secp256k1::new();
    /// let (_, keymap) = signer_descriptor
    ///     .into_wallet_descriptor(&secp, NetworkKind::Test)
    ///     .unwrap();
    /// let external_signers = SignersContainer::build(
    ///     keymap,
    ///     wallet.public_descriptor(KeychainKind::External).expect("keychain must exist"),
    ///     wallet.secp_ctx(),
    /// );
    ///
    /// let to_address = wallet.next_unused_address(KeychainKind::External).expect("keychain must exist").address;
    /// let mut psbt = {
    ///     let mut builder = wallet.build_tx();
    ///     builder.drain_to(to_address.script_pubkey()).drain_wallet();
    ///     builder.finish()?
    /// };
    ///
    /// let finalized = wallet.sign_with_signers(
    ///     &mut psbt,
    ///     &[&external_signers],
    ///     SignOptions::default(),
    /// )?;
    /// assert!(finalized);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn sign_with_signers(
        &self,
        psbt: &mut Psbt,
        signers: &[&SignersContainer],
        sign_options: SignOptions,
    ) -> Result<bool, SignerError> {
        // This adds all the PSBT metadata for the inputs, which will help us later figure out how
        // to derive our keys.
        self.update_psbt_with_descriptor(psbt)
            .map_err(SignerError::MiniscriptPsbt)?;

        // If we aren't allowed to use `witness_utxo`, ensure that every input (except p2tr and
        // finalized ones) has the `non_witness_utxo`.
        if !sign_options.trust_witness_utxo
            && psbt
                .inputs
                .iter()
                .filter(|i| i.final_script_witness.is_none() && i.final_script_sig.is_none())
                .filter(|i| i.tap_internal_key.is_none() && i.tap_merkle_root.is_none())
                .any(|i| i.non_witness_utxo.is_none())
        {
            return Err(SignerError::MissingNonWitnessUtxo);
        }

        // If the user hasn't explicitly opted-in, refuse to sign the transaction unless every input
        // is using `SIGHASH_ALL` or `SIGHASH_DEFAULT` for Taproot.
        if !sign_options.allow_all_sighashes
            && !psbt.inputs.iter().all(|i| {
                i.sighash_type.is_none()
                    || i.sighash_type == Some(EcdsaSighashType::All.into())
                    || i.sighash_type == Some(TapSighashType::All.into())
                    || i.sighash_type == Some(TapSighashType::Default.into())
            })
        {
            return Err(SignerError::NonStandardSighash);
        }

        for signer in signers.iter().flat_map(|container| container.signers()) {
            signer.sign_transaction(psbt, &sign_options, &self.secp)?;
        }

        // Attempt to finalize.
        if sign_options.try_finalize {
            self.finalize_psbt(psbt, sign_options)
        } else {
            Ok(false)
        }
    }
}

/// Methods to construct sync/full-scan requests for spk-based chain sources.
impl<K: Ord + Clone + Debug> Wallet<K> {
    /// Create a partial [`SyncRequest`] for all revealed spks at `start_time`.
    ///
    /// The `start_time` is used to record the time that a mempool transaction was last seen
    /// (or evicted). See [`Wallet::start_sync_with_revealed_spks`] for more.
    pub fn start_sync_with_revealed_spks_at(
        &self,
        start_time: u64,
    ) -> SyncRequestBuilder<(K, u32)> {
        use bdk_chain::keychain_txout::SyncRequestBuilderExt;
        SyncRequest::builder_at(start_time)
            .chain_tip(self.chain.tip())
            .revealed_spks_from_indexer(&self.tx_graph.index, ..)
            .expected_spk_txids(self.tx_graph.list_expected_spk_txids(
                &self.chain,
                self.chain.tip().block_id(),
                ..,
            ))
    }

    /// Create a partial [`SyncRequest`] for this wallet for all revealed spks.
    ///
    /// This is the first step when performing a spk-based wallet partial sync, the returned
    /// [`SyncRequest`] collects all revealed script pubkeys from the wallet keychain needed to
    /// start a blockchain sync with a spk based blockchain client.
    ///
    /// The time of the sync is the current system time and is used to record the
    /// tx last-seen for mempool transactions. Or if an expected transaction is missing
    /// or evicted, it is the time of the eviction. Note that timestamps may only increase
    /// to be counted by the tx graph. To supply your own start time see
    /// [`Wallet::start_sync_with_revealed_spks_at`].
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[cfg(feature = "std")]
    pub fn start_sync_with_revealed_spks(&self) -> SyncRequestBuilder<(K, u32)> {
        use bdk_chain::keychain_txout::SyncRequestBuilderExt;
        SyncRequest::builder()
            .chain_tip(self.chain.tip())
            .revealed_spks_from_indexer(&self.tx_graph.index, ..)
            .expected_spk_txids(self.tx_graph.list_expected_spk_txids(
                &self.chain,
                self.chain.tip().block_id(),
                ..,
            ))
    }

    /// Create a [`FullScanRequest] for this wallet.
    ///
    /// This is the first step when performing a spk-based wallet full scan, the returned
    /// [`FullScanRequest] collects iterators for the wallet's keychain script pub keys needed to
    /// start a blockchain full scan with a spk based blockchain client.
    ///
    /// This operation is generally only used when importing or restoring a previously used wallet
    /// in which the list of used scripts is not known.
    ///
    /// The time of the scan is the current system time and is used to record the tx last-seen for
    /// mempool transactions. To supply your own start time see [`Wallet::start_full_scan_at`].
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[cfg(feature = "std")]
    pub fn start_full_scan(&self) -> FullScanRequestBuilder<K> {
        use bdk_chain::keychain_txout::FullScanRequestBuilderExt;
        FullScanRequest::builder()
            .chain_tip(self.chain.tip())
            .spks_from_indexer(&self.tx_graph.index)
    }

    /// Create a [`FullScanRequest`] builder at `start_time`.
    pub fn start_full_scan_at(&self, start_time: u64) -> FullScanRequestBuilder<K> {
        use bdk_chain::keychain_txout::FullScanRequestBuilderExt;
        FullScanRequest::builder_at(start_time)
            .chain_tip(self.chain.tip())
            .spks_from_indexer(&self.tx_graph.index)
    }
}

impl<K: Ord> AsRef<bdk_chain::tx_graph::TxGraph<ConfirmationBlockTime>> for Wallet<K> {
    fn as_ref(&self) -> &bdk_chain::tx_graph::TxGraph<ConfirmationBlockTime> {
        self.tx_graph.graph()
    }
}

/// Generate a deterministic wallet name from the provided descriptors.
///
/// The wallet name is the concatenation of the [checksum] of the external and (if provided)
/// internal public descriptors. If descriptors containing private keys are provided, the name
/// is computed from the corresponding public descriptors; the result is identical to calling
/// this function with the equivalent public (xpub) descriptors.
///
/// # Errors
///
/// If descriptor parsing fails or if checksum computation fails then a [`DescriptorError`] is
/// returned.
///
/// [checksum]: crate::descriptor::checksum::calc_checksum
pub fn wallet_name_from_descriptor<T>(
    descriptor: T,
    change_descriptor: Option<T>,
    network_kind: NetworkKind,
    secp: &SecpCtx,
) -> Result<String, DescriptorError>
where
    T: IntoWalletDescriptor,
{
    // Wallet name is defined by the checksums of the wallet's public descriptors.
    let (descriptor, _keymap) = descriptor.into_wallet_descriptor(secp, network_kind)?;
    let mut wallet_name = calc_checksum(&descriptor.to_string())?;

    if let Some(change_descriptor) = change_descriptor {
        let (change_descriptor, _change_keymap) =
            change_descriptor.into_wallet_descriptor(secp, network_kind)?;
        wallet_name.push_str(&calc_checksum(&change_descriptor.to_string())?);
    }

    Ok(wallet_name)
}

fn new_local_utxo<K>(
    keychain: K,
    derivation_index: u32,
    full_txo: FullTxOut<ConfirmationBlockTime>,
) -> LocalOutput<K> {
    LocalOutput {
        outpoint: full_txo.outpoint,
        txout: full_txo.txout,
        is_spent: full_txo.spent_by.is_some(),
        chain_position: full_txo.chain_position,
        keychain,
        derivation_index,
    }
}

fn make_indexed_graph<K: Ord + Clone + Debug>(
    stage: &mut ChangeSet<K>,
    tx_graph_changeset: chain::tx_graph::ChangeSet<ConfirmationBlockTime>,
    indexer_changeset: chain::keychain_txout::ChangeSet,
    descriptors: BTreeMap<K, ExtendedDescriptor>,
    lookahead: u32,
    use_spk_cache: bool,
) -> Result<IndexedTxGraph<ConfirmationBlockTime, KeychainTxOutIndex<K>>, InsertDescriptorError<K>>
{
    let (indexed_graph, changeset) = IndexedTxGraph::from_changeset(
        chain::indexed_tx_graph::ChangeSet {
            tx_graph: tx_graph_changeset,
            indexer: indexer_changeset,
        },
        |idx_cs| -> Result<KeychainTxOutIndex<K>, InsertDescriptorError<K>> {
            let mut idx = KeychainTxOutIndex::from_changeset(lookahead, use_spk_cache, idx_cs);

            for (keychain, desc) in descriptors {
                let _inserted = idx.insert_descriptor(keychain.clone(), desc.clone())?;
                assert!(
                    _inserted,
                    "this must be the first time we are seeing this descriptor"
                );
            }

            Ok(idx)
        },
    )?;
    stage.tx_graph.merge(changeset.tx_graph);
    stage.indexer.merge(changeset.indexer);
    Ok(indexed_graph)
}

/// Transforms a [`FeeRate`] to `f64` with unit as sat/vb.
#[macro_export]
#[doc(hidden)]
macro_rules! floating_rate {
    ($rate:expr) => {{
        use $crate::bitcoin::constants::WITNESS_SCALE_FACTOR;
        // sat_kwu / 250.0 -> sat_vb
        $rate.to_sat_per_kwu() as f64 / ((1000 / WITNESS_SCALE_FACTOR) as f64)
    }};
}

#[macro_export]
#[doc(hidden)]
/// Macro for getting a [`Wallet`] for use in a doctest.
macro_rules! doctest_wallet {
    () => {{
        use $crate::bitcoin::{BlockHash, Transaction, absolute, TxOut, Network, hashes::Hash};
        use $crate::chain::{ConfirmationBlockTime, BlockId, TxGraph, tx_graph};
        use $crate::{Update, KeychainKind, Wallet, KeyRing};
        use $crate::test_utils::*;
        let descriptor = "tr([73c5da0a/86'/0'/0']tprv8fMn4hSKPRC1oaCPqxDb1JWtgkpeiQvZhsr8W2xuy3GEMkzoArcAWTfJxYb6Wj8XNNDWEjfYKK4wGQXh3ZUXhDF2NcnsALpWTeSwarJt7Vc/0/*)";
        let change_descriptor = "tr([73c5da0a/86'/0'/0']tprv8fMn4hSKPRC1oaCPqxDb1JWtgkpeiQvZhsr8W2xuy3GEMkzoArcAWTfJxYb6Wj8XNNDWEjfYKK4wGQXh3ZUXhDF2NcnsALpWTeSwarJt7Vc/1/*)";

        let mut keyring = KeyRing::new(Network::Regtest);
        keyring.add_descriptor(KeychainKind::External, descriptor).expect("should add keychain");
        keyring.add_descriptor(KeychainKind::Internal, change_descriptor).expect("should add keychain");
        let mut wallet = keyring.into_params().expect("should be a valid keyring")
            .create_wallet_no_persist()
            .unwrap();
        let address = wallet.peek_address(KeychainKind::External, 0).expect("keychain must exist").expect("should have address").address;
        let tx = Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: vec![],
            output: vec![TxOut {
                value: Amount::from_sat(500_000),
                script_pubkey: address.script_pubkey(),
            }],
        };
        let txid = tx.compute_txid();
        let block_id = BlockId { height: 500, hash: BlockHash::all_zeros() };
        insert_checkpoint(&mut wallet, block_id);
        insert_checkpoint(&mut wallet, BlockId { height: 1_000, hash: BlockHash::all_zeros() });
        insert_tx(&mut wallet, tx);
        let anchor = ConfirmationBlockTime {
            confirmation_time: 50_000,
            block_id,
        };
        insert_anchor(&mut wallet, txid, anchor);
        wallet
    }}
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod test {
    use super::*;
    use crate::miniscript::Error::Unexpected;
    use crate::test_utils::get_test_tr_single_sig_xprv_and_change_desc;
    use crate::test_utils::insert_tx;

    #[test]
    fn not_duplicated_utxos_across_optional_and_required() {
        let (external_desc, internal_desc) = get_test_tr_single_sig_xprv_and_change_desc();

        // Create new wallet.
        let mut keyring = KeyRing::new(Network::Testnet);
        keyring
            .add_descriptor(KeychainKind::External, external_desc)
            .expect("should add keychain");
        keyring
            .add_descriptor(KeychainKind::Internal, internal_desc)
            .expect("should add keychain");
        let mut wallet = keyring
            .into_params()
            .expect("should be a valid keyring")
            .create_wallet_no_persist()
            .unwrap();

        let two_output_tx = Transaction {
            input: vec![],
            output: vec![
                TxOut {
                    script_pubkey: wallet
                        .next_unused_address(KeychainKind::External)
                        .expect("keychain must exist")
                        .script_pubkey(),
                    value: Amount::from_sat(25_000),
                },
                TxOut {
                    script_pubkey: wallet
                        .next_unused_address(KeychainKind::External)
                        .expect("keychain must exist")
                        .script_pubkey(),
                    value: Amount::from_sat(75_000),
                },
            ],
            version: transaction::Version::non_standard(0),
            lock_time: absolute::LockTime::ZERO,
        };

        let txid = two_output_tx.compute_txid();
        insert_tx(&mut wallet, two_output_tx);

        let outpoint = OutPoint { txid, vout: 0 };
        let mut builder = wallet.build_tx();
        builder.add_utxo(outpoint).expect("should add local utxo");
        let params = builder.params.clone();
        // enforce selection of first output in transaction
        let received = wallet.filter_utxos(&params, wallet.latest_checkpoint().block_id().height);
        // Notice expected doesn't include the first output from two_output_tx as it should be
        // filtered out.
        let expected = vec![wallet
            .get_utxo(OutPoint { txid, vout: 1 })
            .map(|utxo| WeightedUtxo {
                satisfaction_weight: wallet
                    .public_descriptor(utxo.keychain)
                    .expect("keychain must exist")
                    .max_weight_to_satisfy()
                    .unwrap(),
                utxo: Utxo::Local(utxo),
            })
            .unwrap()];

        assert_eq!(expected, received);
    }

    #[test]
    fn test_create_two_path_wallet() {
        let two_path_descriptor = "wpkh([9a6a2580/84'/1'/0']tpubDDnGNapGEY6AZAdQbfRJgMg9fvz8pUBrLwvyvUqEgcUfgzM6zc2eVK4vY9x9L5FJWdX8WumXuLEDV5zDZnTfbn87vLe9XceCFwTu9so9Kks/<0;1>/*)";

        // Test successful creation of a two-path wallet
        let mut keyring = KeyRing::new(Network::Testnet);
        keyring
            .add_multipath_descriptor(
                two_path_descriptor,
                &[KeychainKind::External, KeychainKind::Internal],
            )
            .expect("should add keychains");
        let wallet = keyring
            .into_params()
            .expect("should be a valid keyring")
            .create_wallet_no_persist();
        assert!(wallet.is_ok());

        let wallet = wallet.unwrap();

        // Verify that the wallet has both external and internal keychains
        let keychains: Vec<_> = wallet.keychains().collect();
        assert_eq!(keychains.len(), 2);

        // Verify that the descriptors are different (receive vs change)
        let external_desc = keychains
            .iter()
            .find(|(k, _)| *k == KeychainKind::External)
            .unwrap()
            .1;
        let internal_desc = keychains
            .iter()
            .find(|(k, _)| *k == KeychainKind::Internal)
            .unwrap()
            .1;
        assert_ne!(external_desc.to_string(), internal_desc.to_string());

        // Verify that addresses can be generated
        let external_addr = wallet
            .peek_address(KeychainKind::External, 0)
            .expect("keychain must exist")
            .expect("index is valid");
        let internal_addr = wallet
            .peek_address(KeychainKind::Internal, 0)
            .expect("keychain must exist")
            .expect("index is valid");
        assert_ne!(external_addr.address, internal_addr.address);
    }

    #[test]
    fn test_create_two_path_wallet_invalid_descriptor() {
        // Test with invalid single-path descriptor
        let single_path_descriptor = "wpkh([9a6a2580/84'/1'/0']tpubDDnGNapGEY6AZAdQbfRJgMg9fvz8pUBrLwvyvUqEgcUfgzM6zc2eVK4vY9x9L5FJWdX8WumXuLEDV5zDZnTfbn87vLe9XceCFwTu9so9Kks/0/*)";
        let mut keyring = KeyRing::new(Network::Testnet);
        let err = keyring.add_multipath_descriptor(
            single_path_descriptor,
            &[KeychainKind::External, KeychainKind::Internal],
        );
        assert!(matches!(
            err,
            Err(InitError::Descriptor(DescriptorError::MultiPath))
        ));

        // Test with a private descriptor
        // You get a Miniscript(Unexpected("Can't make an extended private key with multiple paths
        // into a public key.")) error.
        let private_multipath_descriptor = "wpkh(tprv8ZgxMBicQKsPdWAHbugK2tjtVtRjKGixYVZUdL7xLHMgXZS6BFbFi1UDb1CHT25Z5PU1F9j7wGxwUiRhqz9E3nZRztikGUV6HoRDYcqPhM4/84'/1'/0'/<0;1>/*)";
        let mut keyring = KeyRing::new(Network::Testnet);
        let err = keyring.add_multipath_descriptor(
            private_multipath_descriptor,
            &[KeychainKind::External, KeychainKind::Internal],
        );
        assert!(matches!(
            err,
            Err(InitError::Descriptor(DescriptorError::Miniscript(
                Unexpected(..)
            )))
        ));

        // // Test with invalid 3-path multipath descriptor
        // let three_path_descriptor =
        // "wpkh([9a6a2580/84'/1'/0'
        // ]tpubDDnGNapGEY6AZAdQbfRJgMg9fvz8pUBrLwvyvUqEgcUfgzM6zc2eVK4vY9x9L5FJWdX8WumXuLEDV5zDZnTfbn87vLe9XceCFwTu9so9Kks/
        // <0;1;2>/*)"; let mut keyring = KeyRing::new(Network::Testnet);
        // let err = keyring.add_multipath_descriptor(
        //     three_path_descriptor,
        //     &[KeychainKind::External, KeychainKind::Internal],
        // );
        // assert!(matches!(
        //     err,
        //     Err(InitError::Descriptor(DescriptorError::MultiPath))
        // ));

        // Test with completely invalid descriptor
        let invalid_descriptor = "invalid_descriptor";
        let mut keyring = KeyRing::new(Network::Testnet);
        let err = keyring.add_multipath_descriptor(
            invalid_descriptor,
            &[KeychainKind::External, KeychainKind::Internal],
        );
        assert!(err.is_err());
    }
    #[test]
    fn test_wallet_name_from_descriptor_public_key_check() {
        let secp = SecpCtx::new();

        // Test with a public descriptor
        let public_descriptor = "wpkh([31a30ffd/84'/1'/0']tpubDCG4yNzDpNYw5ZMuR2usfbPKcnaKjFGwgyussBdhjy2mXmLWnzkwUTZBQPrQxPVcfwh6uFPN4Q7Jk2DPRFb2c4xbrStpqCbKzLkGhvcJvSx/1/*)#vn4aqs37";
        let public_result =
            wallet_name_from_descriptor(public_descriptor, None, NetworkKind::Test, &secp);
        assert!(public_result.is_ok());
        let public_name = public_result.unwrap();
        assert_eq!(public_name, "vn4aqs37"); // Checksum of the public descriptor

        // Test with equivalent private descriptor (should produce same name)
        let private_descriptor = "wpkh(tprv8ZgxMBicQKsPctT28ZYaU77s1UFjHv7o7cafmDntdggZ2dFtNn38RYMzJiDVMBqnqBFDP8rHxsiVRudhyrqi6mgPc4gekgxChgnkTSxHAZ5/84'/1'/0'/1/*)#7z7rgndh";
        let private_result =
            wallet_name_from_descriptor(private_descriptor, None, NetworkKind::Test, &secp);
        assert!(private_result.is_ok());
        assert_eq!(public_name, private_result.unwrap()); // Same wallet name

        // Test with change descriptor
        let change_descriptor = "wpkh([76011771/84'/1'/0']tpubDC3fWoucXCvSyfh6YbyHu1mSQdFjCz5Ejx62eUnRkKdr9bsHGgLEjAaCRNNuaeLjCttfz8sXgshqzawtgWvtozE84rH9BvQn2PUyMCiU1fT/1/*)#jgrerlc3";
        let result_with_change = wallet_name_from_descriptor(
            public_descriptor,
            Some(change_descriptor),
            NetworkKind::Test,
            &secp,
        );
        assert!(result_with_change.is_ok());
        // Wallet name should be main_checksum + change_checksum
        let wallet_name = result_with_change.unwrap();
        assert_eq!(wallet_name, "vn4aqs37jgrerlc3");
    }
}
