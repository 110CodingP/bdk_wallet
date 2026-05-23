use alloc::{ collections::btree_map::BTreeMap};

use bdk_chain::keychain_txout::DEFAULT_LOOKAHEAD;
use bitcoin::{BlockHash, Network, constants::genesis_block};
use bitcoin::secp256k1::Secp256k1;
use miniscript::descriptor::KeyMap;
use miniscript::{Descriptor, DescriptorPublicKey};

use crate::{
    descriptor::{DescriptorError, IntoWalletDescriptor, check_wallet_descriptor},
    AsyncWalletPersister, CreateWithPersistError, KeychainKind, LoadWithPersistError, Wallet,
    WalletPersister,
};

use super::{ChangeSet, LoadError, PersistedWallet};

/// Parameters for [`Wallet::create`] or [`PersistedWallet::create`].
#[must_use]
pub struct CreateParams<K> {
    pub(crate) descriptors: BTreeMap<K, Descriptor<DescriptorPublicKey>>,
    pub(crate) network: Network,
    pub(crate) genesis_hash: BlockHash,
    pub(crate) lookahead: u32,
    pub(crate) use_spk_cache: bool,
}

impl<K: Ord> CreateParams<K> {

    /// Construct parameters with provided `descriptor` and `change_descriptor`.
    ///
    /// Default values:
    /// * `network` = [`Network::Bitcoin`]
    /// * `genesis_hash` = `None`
    /// * `lookahead` = [`DEFAULT_LOOKAHEAD`]
    pub fn new(
        network: Network
    ) -> Self {
        Self {
            descriptors: Default::default(),
            network,
            genesis_hash: genesis_block(network).block_hash(),
            lookahead: DEFAULT_LOOKAHEAD,
            use_spk_cache: false,
        }
    }

    /// Use a custom `genesis_hash`.
    pub fn genesis_hash(mut self, genesis_hash: BlockHash) -> Self {
        self.genesis_hash = genesis_hash;
        self
    }

    /// Use a custom `lookahead` value.
    ///
    /// The `lookahead` defines a number of script pubkeys to derive over and above the last
    /// revealed index. Without a lookahead the indexer will miss outputs you own when processing
    /// transactions whose output script pubkeys lie beyond the last revealed index. In most cases
    /// the default value [`DEFAULT_LOOKAHEAD`] is sufficient.
    pub fn lookahead(mut self, lookahead: u32) -> Self {
        self.lookahead = lookahead;
        self
    }

    /// Use a persistent cache of indexed script pubkeys (SPKs).
    ///
    /// **Note:** To persist across restarts, this option must also be set at load time with
    /// [`LoadParams`](LoadParams::use_spk_cache).
    pub fn use_spk_cache(mut self, use_spk_cache: bool) -> Self {
        self.use_spk_cache = use_spk_cache;
        self
    }

    pub fn add_descriptors<D: IntoWalletDescriptor>(mut self, descriptors: impl Into<BTreeMap<K, D>>) -> Result<(), DescriptorError>{
        let secp = Secp256k1::new();
        for (keychain, desc) in descriptors.into() {
            let descriptor = desc.into_wallet_descriptor(&secp, self.network.into())?.0;
            check_wallet_descriptor(&descriptor)?;
            self.descriptors.insert(keychain, descriptor);
        }
        Ok(())
    }

    /// Create [`PersistedWallet`] with the given [`WalletPersister`].
    pub fn create_wallet<P>(
        self,
        persister: &mut P,
    ) -> Result<PersistedWallet<P>, CreateWithPersistError<P::Error>>
    where
        P: WalletPersister,
    {
        PersistedWallet::create(persister, self)
    }

    /// Create [`PersistedWallet`] with the given [`AsyncWalletPersister`].
    pub async fn create_wallet_async<P>(
        self,
        persister: &mut P,
    ) -> Result<PersistedWallet<P>, CreateWithPersistError<P::Error>>
    where
        P: AsyncWalletPersister,
    {
        PersistedWallet::create_async(persister, self).await
    }

    /// Create [`Wallet`] without persistence.
    pub fn create_wallet_no_persist(self) -> Result<Wallet<K>, DescriptorError> {
        Wallet::create_with_params(self)
    }
}

/// Parameters for [`Wallet::load`] or [`PersistedWallet::load`].
#[must_use]
pub struct LoadParams<K> {
    pub(crate) lookahead: u32,
    pub(crate) check_network: Option<Network>,
    pub(crate) check_genesis_hash: Option<BlockHash>,
    pub(crate) check_descriptor: Option<Option<DescriptorToExtract>>,
    pub(crate) check_change_descriptor: Option<Option<DescriptorToExtract>>,
    pub(crate) use_spk_cache: bool,
}

impl<K: Ord> LoadParams<K> {
    /// Construct parameters with default values.
    ///
    /// Default values: `lookahead` = [`DEFAULT_LOOKAHEAD`]
    pub fn new() -> Self {
        Self {
            lookahead: DEFAULT_LOOKAHEAD,
            check_network: None,
            check_genesis_hash: None,
            check_descriptor: None,
            check_change_descriptor: None,
            use_spk_cache: false,
        }
    }

    /// Checks the `expected_descriptor` matches exactly what is loaded for `keychain`.
    ///
    /// # Note
    ///
    /// You must also specify [`extract_keys`](Self::extract_keys) if you wish to add a signer
    /// for an expected descriptor containing secrets.
    pub fn descriptor<D>(mut self, keychain: KeychainKind, expected_descriptor: Option<D>) -> Self
    where
        D: IntoWalletDescriptor + Send + 'static,
    {
        let expected = expected_descriptor.map(|d| make_descriptor_to_extract(d));
        match keychain {
            KeychainKind::External => self.check_descriptor = Some(expected),
            KeychainKind::Internal => self.check_change_descriptor = Some(expected),
        }
        self
    }

    /// Checks that the provided two-path descriptor matches exactly what is loaded for both the
    /// external and internal keychains.
    ///
    /// # Note
    ///
    /// You must also specify [`extract_keys`](Self::extract_keys) if you wish to add a signer
    /// for an expected descriptor containing secrets.
    pub fn two_path_descriptor<D>(mut self, expected_descriptor: D) -> Self
    where
        D: IntoWalletDescriptor + Send + Clone + 'static,
    {
        let external: DescriptorToExtract =
            make_two_path_descriptor_to_extract(expected_descriptor.clone(), 0);
        let internal: DescriptorToExtract =
            make_two_path_descriptor_to_extract(expected_descriptor, 1);

        self.check_descriptor = Some(Some(external));
        self.check_change_descriptor = Some(Some(internal));

        self
    }

    /// Checks that the given network matches the one loaded from persistence.
    pub fn check_network(mut self, network: Network) -> Self {
        self.check_network = Some(network);
        self
    }

    /// Checks that the given `genesis_hash` matches the one loaded from persistence.
    pub fn check_genesis_hash(mut self, genesis_hash: BlockHash) -> Self {
        self.check_genesis_hash = Some(genesis_hash);
        self
    }

    /// Use a custom `lookahead` value.
    ///
    /// The `lookahead` defines a number of script pubkeys to derive over and above the last
    /// revealed index. Without a lookahead the indexer will miss outputs you own when processing
    /// transactions whose output script pubkeys lie beyond the last revealed index. In most cases
    /// the default value [`DEFAULT_LOOKAHEAD`] is sufficient.
    pub fn lookahead(mut self, lookahead: u32) -> Self {
        self.lookahead = lookahead;
        self
    }

    /// Use a persistent cache of indexed script pubkeys (SPKs).
    ///
    /// NOTE: This should only be used if you have previously persisted a cache of script
    /// pubkeys using [`CreateParams::use_spk_cache`].
    pub fn use_spk_cache(mut self, use_spk_cache: bool) -> Self {
        self.use_spk_cache = use_spk_cache;
        self
    }

    /// Load [`PersistedWallet`] with the given [`WalletPersister`].
    pub fn load_wallet<P>(
        self,
        persister: &mut P,
    ) -> Result<Option<PersistedWallet<P>>, LoadWithPersistError<P::Error>>
    where
        P: WalletPersister,
    {
        PersistedWallet::load(persister, self)
    }

    /// Load [`PersistedWallet`] with the given [`AsyncWalletPersister`].
    pub async fn load_wallet_async<P>(
        self,
        persister: &mut P,
    ) -> Result<Option<PersistedWallet<P>>, LoadWithPersistError<P::Error>>
    where
        P: AsyncWalletPersister,
    {
        PersistedWallet::load_async(persister, self).await
    }

    /// Load [`Wallet`] without persistence.
    pub fn load_wallet_no_persist(self, changeset: ChangeSet<K>) -> Result<Option<Wallet<K>>, LoadError> {
        Wallet::load_with_params(changeset, self)
    }
}

impl Default for LoadParams {
    fn default() -> Self {
        Self::new()
    }
}
