use alloc::boxed::Box;

use bdk_chain::keychain_txout::DEFAULT_LOOKAHEAD;
use bitcoin::{BlockHash, Network, NetworkKind};
use miniscript::descriptor::KeyMap;

use crate::{
    descriptor::{DescriptorError, ExtendedDescriptor, IntoWalletDescriptor},
    utils::SecpCtx,
    AsyncWalletPersister, CreateWithPersistError, KeychainKind, LoadWithPersistError, Wallet,
    WalletPersister,
};

use super::{ChangeSet, LoadError, PersistedWallet};

fn make_two_path_descriptor_to_extract<D>(
    two_path_descriptor: D,
    index: usize,
) -> DescriptorToExtract
where
    D: IntoWalletDescriptor + Send + 'static,
{
    Box::new(move |secp, network| {
        let (desc, keymap) = two_path_descriptor.into_wallet_descriptor(secp, network)?;

        if !desc.is_multipath() {
            return Err(DescriptorError::MultiPath);
        }

        let descriptors = desc
            .into_single_descriptors()
            .map_err(DescriptorError::Miniscript)?;

        if descriptors.len() != 2 {
            return Err(DescriptorError::MultiPath);
        }

        Ok((descriptors[index].clone(), keymap))
    })
}

/// This atrocity is to avoid having type parameters on [`CreateParams`] and [`LoadParams`].
///
/// The better option would be to do `Box<dyn IntoWalletDescriptor>`, but we cannot due to Rust's
/// [object safety rules](https://doc.rust-lang.org/reference/items/traits.html#object-safety).
type DescriptorToExtract = Box<
    dyn FnOnce(&SecpCtx, NetworkKind) -> Result<(ExtendedDescriptor, KeyMap), DescriptorError>
        + Send
        + 'static,
>;

fn make_descriptor_to_extract<D>(descriptor: D) -> DescriptorToExtract
where
    D: IntoWalletDescriptor + Send + 'static,
{
    Box::new(|secp, network_kind| descriptor.into_wallet_descriptor(secp, network_kind))
}

/// Parameters for [`Wallet::create`] or [`PersistedWallet::create`].
#[must_use]
pub struct CreateParams {
    pub(crate) descriptor: DescriptorToExtract,
    pub(crate) descriptor_keymap: KeyMap,
    pub(crate) change_descriptor: Option<DescriptorToExtract>,
    pub(crate) change_descriptor_keymap: KeyMap,
    pub(crate) network: Network,
    pub(crate) genesis_hash: Option<BlockHash>,
    pub(crate) lookahead: u32,
    pub(crate) use_spk_cache: bool,
}

impl CreateParams {
    /// Construct parameters with provided `descriptor`.
    ///
    /// Default values:
    /// * `change_descriptor` = `None`
    /// * `network` = [`Network::Bitcoin`]
    /// * `genesis_hash` = `None`
    /// * `lookahead` = [`DEFAULT_LOOKAHEAD`]
    ///
    /// Use this method only when building a wallet with a single descriptor. See
    /// also [`Wallet::create_single`].
    pub fn new_single<D: IntoWalletDescriptor + Send + 'static>(descriptor: D) -> Self {
        Self {
            descriptor: make_descriptor_to_extract(descriptor),
            descriptor_keymap: KeyMap::default(),
            change_descriptor: None,
            change_descriptor_keymap: KeyMap::default(),
            network: Network::Bitcoin,
            genesis_hash: None,
            lookahead: DEFAULT_LOOKAHEAD,
            use_spk_cache: false,
        }
    }

    /// Construct parameters with provided `descriptor` and `change_descriptor`.
    ///
    /// Default values:
    /// * `network` = [`Network::Bitcoin`]
    /// * `genesis_hash` = `None`
    /// * `lookahead` = [`DEFAULT_LOOKAHEAD`]
    pub fn new<D: IntoWalletDescriptor + Send + 'static>(
        descriptor: D,
        change_descriptor: D,
    ) -> Self {
        Self {
            descriptor: make_descriptor_to_extract(descriptor),
            descriptor_keymap: KeyMap::default(),
            change_descriptor: Some(make_descriptor_to_extract(change_descriptor)),
            change_descriptor_keymap: KeyMap::default(),
            network: Network::Bitcoin,
            genesis_hash: None,
            lookahead: DEFAULT_LOOKAHEAD,
            use_spk_cache: false,
        }
    }

    /// Construct parameters with a two-path descriptor that will be parsed into receive and change
    /// descriptors.
    ///
    /// This function parses a two-path descriptor (receive and change) and creates parameters
    /// using the existing receive and change wallet creation logic.
    ///
    /// Default values:
    /// * `network` = [`Network::Bitcoin`]
    /// * `genesis_hash` = `None`
    /// * `lookahead` = [`DEFAULT_LOOKAHEAD`]
    pub fn new_two_path<D: IntoWalletDescriptor + Send + Clone + 'static>(
        two_path_descriptor: D,
    ) -> Self {
        Self {
            descriptor: make_two_path_descriptor_to_extract(two_path_descriptor.clone(), 0),
            descriptor_keymap: KeyMap::default(),
            change_descriptor: Some(make_two_path_descriptor_to_extract(two_path_descriptor, 1)),
            change_descriptor_keymap: KeyMap::default(),
            network: Network::Bitcoin,
            genesis_hash: None,
            lookahead: DEFAULT_LOOKAHEAD,
            use_spk_cache: false,
        }
    }

    /// Extend the given `keychain`'s `keymap`.
    pub fn keymap(mut self, keychain: KeychainKind, keymap: KeyMap) -> Self {
        match keychain {
            KeychainKind::External => &mut self.descriptor_keymap,
            KeychainKind::Internal => &mut self.change_descriptor_keymap,
        }
        .extend(keymap);
        self
    }

    /// Set [`Self::network`].
    pub fn network(mut self, network: Network) -> Self {
        self.network = network;
        self
    }

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
    pub fn create_wallet_no_persist(self) -> Result<Wallet, DescriptorError> {
        Wallet::create_with_params(self)
    }
}

/// Parameters for [`Wallet::load`] or [`PersistedWallet::load`].
#[must_use]
pub struct LoadParams {
    pub(crate) descriptor_keymap: KeyMap,
    pub(crate) change_descriptor_keymap: KeyMap,
    pub(crate) lookahead: u32,
    pub(crate) check_network: Option<Network>,
    pub(crate) check_genesis_hash: Option<BlockHash>,
    pub(crate) check_descriptor: Option<Option<DescriptorToExtract>>,
    pub(crate) check_change_descriptor: Option<Option<DescriptorToExtract>>,
    pub(crate) extract_keys: bool,
    pub(crate) use_spk_cache: bool,
}

impl LoadParams {
    /// Construct parameters with default values.
    ///
    /// Default values: `lookahead` = [`DEFAULT_LOOKAHEAD`]
    pub fn new() -> Self {
        Self {
            descriptor_keymap: KeyMap::default(),
            change_descriptor_keymap: KeyMap::default(),
            lookahead: DEFAULT_LOOKAHEAD,
            check_network: None,
            check_genesis_hash: None,
            check_descriptor: None,
            check_change_descriptor: None,
            extract_keys: false,
            use_spk_cache: false,
        }
    }

    /// Extend the given `keychain`'s `keymap`.
    pub fn keymap(mut self, keychain: KeychainKind, keymap: KeyMap) -> Self {
        match keychain {
            KeychainKind::External => &mut self.descriptor_keymap,
            KeychainKind::Internal => &mut self.change_descriptor_keymap,
        }
        .extend(keymap);
        self
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

    /// Whether to try extracting private keys from the *provided descriptors* upon loading.
    /// See also [`LoadParams::descriptor`].
    pub fn extract_keys(mut self) -> Self {
        self.extract_keys = true;
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
    pub fn load_wallet_no_persist(self, changeset: ChangeSet) -> Result<Option<Wallet>, LoadError> {
        Wallet::load_with_params(changeset, self)
    }
}

impl Default for LoadParams {
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
