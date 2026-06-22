use alloc::{boxed::Box, collections::btree_map::BTreeMap};

use bdk_chain::keychain_txout::DEFAULT_LOOKAHEAD;
use bitcoin::{BlockHash, Network, NetworkKind};
use miniscript::descriptor::{Descriptor, DescriptorPublicKey};

use crate::{
    descriptor::{DescriptorError, ExtendedDescriptor, IntoWalletDescriptor},
    error::InitError,
    utils::SecpCtx,
    AsyncWalletPersister, CreateWithPersistError, LoadWithPersistError, Wallet, WalletPersister,
};

use alloc::{collections::btree_set::BTreeSet, vec::Vec};

use core::fmt::Debug;

use crate::descriptor::check_wallet_descriptor;

use super::{ChangeSet, LoadError, PersistedWallet};

fn make_multi_path_descriptor_to_extract<D>(
    multi_path_descriptor: D,
    index: usize,
    range: core::ops::RangeFull,
) -> DescriptorToExtract
where
    D: IntoWalletDescriptor + Send + 'static,
{
    Box::new(move |secp, network| {
        let desc = multi_path_descriptor
            .into_wallet_descriptor(secp, network)?
            .0;

        if !desc.is_multipath() {
            return Err(DescriptorError::MultiPath);
        }

        let descriptors = desc
            .into_single_descriptors()
            .map_err(DescriptorError::Miniscript)?
            .get(range)
            .ok_or(DescriptorError::MultiPath)?
            .to_vec();

        if descriptors.len() <= index {
            return Err(DescriptorError::MultiPath);
        }

        check_wallet_descriptor(&descriptors[index])?;

        Ok(descriptors[index].clone())
    })
}

/// This atrocity is to avoid having type parameters on [`CreateParams`] and [`LoadParams`].
///
/// The better option would be to do `Box<dyn IntoWalletDescriptor>`, but we cannot due to Rust's
/// [object safety rules](https://doc.rust-lang.org/reference/items/traits.html#object-safety).
pub(crate) type DescriptorToExtract = Box<
    dyn FnOnce(&SecpCtx, NetworkKind) -> Result<ExtendedDescriptor, DescriptorError>
        + Send
        + 'static,
>;

fn make_descriptor_to_extract<D>(descriptor: D) -> DescriptorToExtract
where
    D: IntoWalletDescriptor + Send + 'static,
{
    Box::new(|secp, network_kind| {
        descriptor
            .into_wallet_descriptor(secp, network_kind)
            .map(|res| res.0)
    })
}

/// Parameters for [`Wallet::create_with_params`] or [`PersistedWallet::create`].
#[must_use]
pub struct CreateParams<K> {
    pub(crate) secp: SecpCtx,
    pub(crate) descriptors: BTreeMap<K, Descriptor<DescriptorPublicKey>>,
    pub(crate) network: Network,
    pub(crate) genesis_hash: Option<BlockHash>,
    pub(crate) lookahead: u32,
    pub(crate) use_spk_cache: bool,
}

impl<K: Ord + Clone + Debug> CreateParams<K> {
    /// Use a custom `genesis_hash`.
    pub fn genesis_hash(mut self, genesis_hash: BlockHash) -> Self {
        self.genesis_hash = Some(genesis_hash);
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

    /// Create [`PersistedWallet`] with the given [`WalletPersister`].
    pub fn create_wallet<P>(
        self,
        persister: &mut P,
    ) -> Result<PersistedWallet<K, P>, CreateWithPersistError<K, P::Error>>
    where
        P: WalletPersister<K>,
    {
        PersistedWallet::create(persister, self)
    }

    /// Create [`PersistedWallet`] with the given [`AsyncWalletPersister`].
    pub async fn create_wallet_async<P>(
        self,
        persister: &mut P,
    ) -> Result<PersistedWallet<K, P>, CreateWithPersistError<K, P::Error>>
    where
        P: AsyncWalletPersister<K>,
    {
        PersistedWallet::create_async(persister, self).await
    }

    /// Create [`Wallet`] without persistence.
    pub fn create_wallet_no_persist(self) -> Result<Wallet<K>, InitError<K>> {
        Wallet::create_with_params(self)
    }
}

/// Parameters for [`Wallet::load`] or [`PersistedWallet::load`].
#[must_use]
pub struct LoadParams<K> {
    pub(crate) lookahead: u32,
    pub(crate) check_network: Option<Network>,
    pub(crate) check_genesis_hash: Option<BlockHash>,
    pub(crate) check_descriptors: BTreeMap<K, Option<DescriptorToExtract>>,
    pub(crate) use_spk_cache: bool,
}

/// To avoid clippy's type complexity error.
pub type PersistedWalletLoadOption<K, P> = Option<PersistedWallet<K, P>>;

impl<K: Ord + Clone + Debug> LoadParams<K> {
    /// Construct parameters with default values.
    ///
    /// Default values: `lookahead` = [`DEFAULT_LOOKAHEAD`]
    pub fn new() -> Self {
        Self {
            lookahead: DEFAULT_LOOKAHEAD,
            check_network: None,
            check_genesis_hash: None,
            check_descriptors: BTreeMap::default(),
            use_spk_cache: false,
        }
    }

    /// Checks the `expected_descriptor` matches exactly what is loaded for `keychain`.
    ///
    /// Note: If `expected_descriptor` is `None`, it just checks if the keychain
    /// has some corresponding descriptor after loading.
    pub fn descriptor<D>(mut self, keychain: K, expected_descriptor: Option<D>) -> Self
    where
        D: IntoWalletDescriptor + Send + 'static,
    {
        let expected = expected_descriptor.map(|d| make_descriptor_to_extract(d));
        self.check_descriptors.insert(keychain, expected);
        self
    }

    /// Checks that the provided multi-path descriptor matches exactly what is loaded.
    pub fn multi_path_descriptor<D>(self, expected_descriptor: D, keychains: &[K]) -> Self
    where
        D: IntoWalletDescriptor + Send + Clone + 'static,
    {
        self.multi_path_descriptor_with_range(expected_descriptor, keychains, ..)
    }

    fn multi_path_descriptor_with_range<D>(
        mut self,
        expected_descriptor: D,
        keychains: &[K],
        range: core::ops::RangeFull,
    ) -> Self
    where
        D: IntoWalletDescriptor + Send + Clone + 'static,
    {
        let mut descriptors = BTreeMap::default();

        for (i, keychain) in keychains.iter().enumerate() {
            descriptors.insert(
                keychain.clone(),
                Some(make_multi_path_descriptor_to_extract(
                    expected_descriptor.clone(),
                    i,
                    range,
                )),
            );
        }

        self.check_descriptors.extend(descriptors);

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
    ) -> Result<PersistedWalletLoadOption<K, P>, LoadWithPersistError<K, P::Error>>
    where
        P: WalletPersister<K>,
    {
        PersistedWallet::load(persister, self)
    }

    /// Load [`PersistedWallet`] with the given [`AsyncWalletPersister`].
    pub async fn load_wallet_async<P>(
        self,
        persister: &mut P,
    ) -> Result<Option<PersistedWallet<K, P>>, LoadWithPersistError<K, P::Error>>
    where
        P: AsyncWalletPersister<K>,
    {
        PersistedWallet::load_async(persister, self).await
    }

    /// Load [`Wallet`] without persistence.
    pub fn load_wallet_no_persist(
        self,
        changeset: ChangeSet<K>,
    ) -> Result<Option<Wallet<K>>, LoadError<K>> {
        Wallet::load_with_params(changeset, self)
    }
}

impl<K: Ord + Clone + Debug> Default for LoadParams<K> {
    fn default() -> Self {
        Self::new()
    }
}

/// Container for the wallet descriptors and network during wallet creation.
#[derive(Debug, Clone)]
pub struct KeyRing<K> {
    // The secp context.
    secp: SecpCtx,
    // [`Wallet`]'s network.
    network: Network,
    // [`Wallet`]'s descriptors.
    keychains: BTreeMap<K, Descriptor<DescriptorPublicKey>>,
    // For quick membership check when adding descriptors.
    // Not expecting this to be large.
    descriptors: BTreeSet<Descriptor<DescriptorPublicKey>>,
}

impl<K> KeyRing<K>
where
    K: Ord + Clone + Debug,
{
    /// Construct a new [`KeyRing`] with the provided network.
    ///
    /// To add descriptors use [`KeyRing::add_descriptor`].
    pub fn new(network: Network) -> Self {
        Self {
            secp: SecpCtx::new(),
            network,
            keychains: BTreeMap::default(),
            descriptors: BTreeSet::default(),
        }
    }

    /// Get the [`Network`] corresponding to the [`KeyRing`]
    pub fn network(&self) -> Network {
        self.network
    }

    /// Adds a descriptor (non-multipath) to the [`KeyRing`].
    ///
    /// This method returns an error if the provided descriptor is multipath,
    /// contains hardened derivation steps (in case of public descriptors) or
    /// fails miniscripts sanity checks. It also returns an error when
    /// one of `keychain` or `descriptor` is already in the keyring.
    pub fn add_descriptor(
        &mut self,
        keychain: K,
        descriptor: impl IntoWalletDescriptor,
    ) -> Result<(), InitError<K>> {
        let descriptor = descriptor
            .into_wallet_descriptor(&self.secp, self.network.into())?
            .0;
        check_wallet_descriptor(&descriptor)?;

        if self.keychains.contains_key(&keychain) {
            return Err(InitError::KeychainAlreadyExists(Box::new(keychain)));
        }

        if self.descriptors.contains(&descriptor) {
            return Err(InitError::DescAlreadyExists(Box::new(descriptor)));
        }

        self.keychains.insert(keychain, descriptor.clone());
        self.descriptors.insert(descriptor);

        Ok(())
    }

    /// Adds a multipath descriptor to the [`KeyRing`] where each descriptor extracted
    /// is paired with a keychain in `keychains` in order.
    ///
    /// Note: It is guaranteed that the addition of the single path keychains to the keyring is
    /// atomic.
    ///
    /// This method returns an error if the provided descriptor is not multipath,
    /// contains hardened derivation steps (in case of public descriptors) or
    /// fails miniscripts sanity checks. It also returns an error when one of `keychain`
    /// or one of the extracted descriptors is already in the keyring or when the multipath
    /// `descriptor` cannot be expanded to as many single path descriptors as `keychains`.
    pub fn add_multipath_descriptor(
        &mut self,
        descriptor: impl IntoWalletDescriptor,
        keychains: &[K],
    ) -> Result<(), InitError<K>> {
        self.add_multipath_descriptor_with_range(descriptor, keychains, ..)
    }

    /// Adds a multipath descriptor to the [`KeyRing`] where descriptors in the given `range` are
    /// extracted and are paired with a keychain in `keychains` in order.
    ///
    /// This method returns an error if the provided descriptor is not multipath,
    /// contains hardened derivation steps (in case of public descriptors) or
    /// fails miniscripts sanity checks. It also returns an error when one of `keychain`
    /// or one of the extracted descriptors is already in the keyring, when the multipath
    /// `descriptor` cannot be expanded to as many single path descriptors as `keychains`
    /// or when the provided `range` is out of bounds.
    fn add_multipath_descriptor_with_range(
        &mut self,
        descriptor: impl IntoWalletDescriptor,
        keychains: &[K],
        range: core::ops::RangeFull,
    ) -> Result<(), InitError<K>> {
        let descriptor = descriptor
            .into_wallet_descriptor(&self.secp, self.network.into())?
            .0;

        if !descriptor.is_multipath() {
            return Err(DescriptorError::MultiPath)?;
        }

        let descriptors = extract_from_multipath(descriptor)?
            .get(range)
            .ok_or(DescriptorError::MultiPath)?
            .to_vec();

        if descriptors.len() < keychains.len() {
            return Err(DescriptorError::MultiPath)?;
        }

        for keychain in keychains {
            if self.keychains.contains_key(keychain) {
                return Err(InitError::KeychainAlreadyExists(Box::new(keychain.clone())));
            }
        }

        for descriptor in descriptors.iter() {
            if self.descriptors.contains(descriptor) {
                return Err(InitError::DescAlreadyExists(Box::new(descriptor.clone())));
            }
        }

        for (keychain, descriptor) in keychains.iter().zip(descriptors) {
            self.keychains.insert(keychain.clone(), descriptor.clone());
            self.descriptors.insert(descriptor);
        }

        Ok(())
    }

    /// Obtain corresponding [`CreateParams`] from the [`KeyRing`]
    ///
    /// Returns an error if the keyring does not contain any keychains.
    pub fn into_params(self) -> Result<CreateParams<K>, InitError<K>> {
        // Guard against no-keychain case.
        if self.descriptors.is_empty() {
            return Err(InitError::NoKeychains);
        };

        Ok(CreateParams {
            secp: self.secp,
            descriptors: self.keychains,
            network: self.network,
            genesis_hash: None,
            lookahead: DEFAULT_LOOKAHEAD,
            use_spk_cache: false,
        })
    }

    /// Get all the keychains on this [`KeyRing`].
    pub fn list_keychains(&self) -> &BTreeMap<K, Descriptor<DescriptorPublicKey>> {
        &self.keychains
    }
}

// Extract single path descriptors from the `multipath_descriptor` in the given range.
fn extract_from_multipath(
    multipath_descriptor: Descriptor<DescriptorPublicKey>,
) -> Result<Vec<Descriptor<DescriptorPublicKey>>, DescriptorError> {
    let descriptors = multipath_descriptor
        .into_single_descriptors()
        .map_err(DescriptorError::Miniscript)?;

    for descriptor in descriptors.iter() {
        check_wallet_descriptor(descriptor)?;
    }

    Ok(descriptors)
}
