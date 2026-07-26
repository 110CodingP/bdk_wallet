use alloc::collections::btree_map::BTreeMap;
use bdk_chain::{
    indexed_tx_graph, keychain_txout, local_chain, tx_graph, ConfirmationBlockTime, Merge,
};
use miniscript::{Descriptor, DescriptorPublicKey};
use serde::{Deserialize, Serialize};

use crate::locked_outpoints;

type IndexedTxGraphChangeSet =
    indexed_tx_graph::ChangeSet<ConfirmationBlockTime, keychain_txout::ChangeSet>;

/// A change set for [`Wallet`].
///
/// ## Definition
///
/// The change set is responsible for transmitting data between the persistent storage layer and the
/// core library components. Specifically, it serves two primary functions:
///
/// 1) Recording incremental changes to the in-memory representation that need to be persisted to
///    disk
/// 2) Applying aggregate changes from the persistence layer to the in-memory representation at
///    startup
///
/// ## Contract
///
/// The change set maintains and enforces the following properties:
///
/// * Change sets must implement [`Serialize`] and [`Deserialize`] to meet the definition from
///   above.
/// * Change sets must implement [`Default`] as a way of instantiating new empty objects.
/// * Change sets must implement [`Merge`] so that many instances can be aggregated into a single
///   instance.
/// * A change set is composed of a number of individual "sub-change sets" that adhere to the same
///   rules as above. This is for increased modularity and portability. For example the core modules
///   each have their own change set (`tx_graph`, `local_chain`, etc).
///
/// ## Members and required fields
///
/// The change set has certain required fields without which a [`Wallet`] cannot function.
/// These include the [`descriptors`] and the [`bitcoin::Network`] in use. These are required to be
/// non-empty *in the aggregate*, meaning the field must be present and non-null in the union of all
/// persisted changes, but may be empty in any one change set, where "empty" is defined by the
/// [`Merge`](Merge::is_empty) implementation of that change set. This requirement also applies to
/// the [`local_chain`] field in that the aggregate change set must include a genesis block.
///
/// For example, the [`descriptors`] and [`bitcoin::Network`] are present in the first change set
/// after wallet creation, but are usually omitted in subsequent updates, as they are not permitted
/// to change at any point thereafter.
///
/// Other fields of the change set are not required to be non-empty, that is they may be empty even
/// in the aggregate. However, in practice they should contain the data needed to recover a wallet
/// state between sessions. These include:
/// * [`tx_graph`](Self::tx_graph)
/// * [`indexer`](Self::indexer)
///
/// ## Staging
///
/// For greater efficiency the [`Wallet`] is able to *stage* the to-be-persisted changes. Many
/// operations result in staged changes which require persistence on the part of the user. These
/// include address revelation, applying an [`Update`], and introducing transactions and chain
/// data to the wallet. To get the staged changes see [`Wallet::staged`] and similar methods. Once
/// the changes are committed to the persistence layer the contents of the stage should be
/// discarded.
///
/// Users should persist early and often generally speaking, however in principle there is no
/// limit to the number or type of changes that can be staged prior to persisting or the order in
/// which they're staged. This is because change sets are designed to be [merged]. The change
/// that is ultimately persisted will encompass the combined effect of each change individually.
///
/// ## Extensibility
///
/// Existing fields may be extended in the future with additional sub-fields. New top-level fields
/// are likely to be added as new features and core components are implemented. Existing fields may
/// be removed in future versions of the library following the deprecation policy below.
///
/// ## Version Compatibility
///
/// Any change to the [`ChangeSet`] data structure MUST correlate with a major version bump per
/// [Semantic Versioning]. We guarantee that version N can read and
/// deserialize [`ChangeSet`] data written by version N-1 (one major version back), but this
/// guarantee does NOT extend to version N-2 or earlier. New fields added in version N must
/// implement [`Default`] so that when reading N-1 data, absent fields are populated with default
/// values.
///
/// Limited forward compatibility is provided for downgrades: version N-1 will successfully
/// deserialize version N data without errors by ignoring unknown fields. Users should be aware that
/// features introduced in version N will not be available when downgrading to N-1, and that
/// downgrading can result in loss of data if not backed up. For this reason we recommend carefully
/// planning major upgrades and backing up necessary data to avoid compatibility issues.
///
/// Fields can be removed using a 3-version deprecation cycle: fields are marked deprecated in
/// version N with a reason and instructions for migrating, the field is retained in version N+1
/// for compatibility where it deserializes but may not be used, and finally removed in version
/// N+2. This ensures the standard backwards compatibility guarantees while allowing the removal of
/// deprecated fields.
///
/// ### Responsibilities
///
/// Library authors SHOULD test all upgrade paths using the persistence test suite and in CI.
/// Library authors MUST document API changes prominently in the release notes and CHANGELOG,
/// clearly mark deprecated fields including migration instructions, and follow the 3-version
/// deprecation cycle before removing fields.
///
/// Users SHOULD back up wallet data before major version upgrades, test upgrades in non-production
/// environments first, and monitor the release notes for warnings and updates. Users MUST complete
/// migrations within the compatibility window, and not skip major versions (i.e. upgrade major
/// versions sequentially).
///
/// ### Custom Persistence Implementations
///
/// The resulting interface is designed to give the user more control of what to persist and when
/// to persist it. Custom implementations should consider and account for the possibility of
/// partial or repeat writes, the atomicity of persistence operations, and the order of reads and
/// writes among the fields of the change set. BDK comes with support for [SQLite] that handles
/// the details for you and is recommended for many users. If implementing your own persistence,
/// please refer to the documentation for [`WalletPersister`] and [`PersistedWallet`] for more
/// information.
///
/// [`descriptors`]: Self::descriptors
/// [`local_chain`]: Self::local_chain
/// [merged]: bdk_chain::Merge
/// [`network`]: Self::network
/// [`PersistedWallet`]: crate::PersistedWallet
/// [SQLite]: bdk_chain::rusqlite
/// [`Update`]: crate::Update
/// [`WalletPersister`]: crate::WalletPersister
/// [`Wallet::staged`]: crate::Wallet::staged
/// [`Wallet`]: crate::Wallet
/// [Semantic Versioning]: <https://doc.rust-lang.org/cargo/reference/semver.html>
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: Ord + serde::de::DeserializeOwned"))]
pub struct ChangeSet<K> {
    /// Descriptor for recipient addresses. (to be deprecated)
    pub descriptor: Option<Descriptor<DescriptorPublicKey>>,
    /// Descriptor for change addresses. (to be deprecated)
    pub change_descriptor: Option<Descriptor<DescriptorPublicKey>>,
    /// Stores the network type of the transaction data.
    pub network: Option<bitcoin::Network>,
    /// Changes to the [`LocalChain`](local_chain::LocalChain).
    pub local_chain: local_chain::ChangeSet,
    /// Changes to [`TxGraph`](tx_graph::TxGraph).
    pub tx_graph: tx_graph::ChangeSet<ConfirmationBlockTime>,
    /// Changes to [`KeychainTxOutIndex`](keychain_txout::KeychainTxOutIndex).
    pub indexer: keychain_txout::ChangeSet,
    /// Changes to locked outpoints.
    #[serde(default)]
    pub locked_outpoints: locked_outpoints::ChangeSet,
    /// Descriptors corresponding to each keychain.
    #[serde(default)]
    pub descriptors: BTreeMap<K, Descriptor<DescriptorPublicKey>>,
}

impl<K> Default for ChangeSet<K> {
    fn default() -> Self {
        Self {
            descriptor: None,
            change_descriptor: None,
            network: None,
            local_chain: local_chain::ChangeSet::default(),
            tx_graph: tx_graph::ChangeSet::default(),
            indexer: keychain_txout::ChangeSet::default(),
            locked_outpoints: locked_outpoints::ChangeSet::default(),
            descriptors: BTreeMap::default(),
        }
    }
}

impl<K: Ord> Merge for ChangeSet<K> {
    /// Merge another [`ChangeSet`] into itself.
    fn merge(&mut self, other: Self) {
        // ignore descriptor and change_descriptor fields.

        if other.network.is_some() {
            debug_assert!(
                self.network.is_none() || self.network == other.network,
                "network must never change"
            );
            self.network = other.network;
        }

        // Currently we do not allow addition of descriptors to the wallet.
        if !other.descriptors.is_empty() {
            if !self.descriptors.is_empty() {
                debug_assert!(
                    self.descriptors == other.descriptors,
                    "Descriptors cannot be added, removed or reassigned to a different keychain."
                )
            } else {
                self.descriptors = other.descriptors;
            }
        }

        // merge locked outpoints
        self.locked_outpoints.merge(other.locked_outpoints);

        Merge::merge(&mut self.local_chain, other.local_chain);
        Merge::merge(&mut self.tx_graph, other.tx_graph);
        Merge::merge(&mut self.indexer, other.indexer);
    }

    fn is_empty(&self) -> bool {
        // ignore descriptor and change_descriptor fields.
        self.network.is_none()
            && self.local_chain.is_empty()
            && self.tx_graph.is_empty()
            && self.indexer.is_empty()
            && self.locked_outpoints.is_empty()
            && self.descriptors.is_empty()
    }
}
use crate::KeychainKind;

// Contains methods to move back and forth between the v3 and v4 [`ChangeSets`].
impl ChangeSet<KeychainKind> {
    /// Populate `descriptors` using `descriptor` and `change_descriptor` if they exist.
    ///
    /// Note: Original `descriptors` is discarded. Other fields are copied as it is.
    pub fn from_v3(self) -> Self {
        let mut descriptors = BTreeMap::new();

        if let Some(descriptor) = &self.descriptor {
            descriptors.insert(KeychainKind::External, descriptor.clone());
        }

        if let Some(change_descriptor) = &self.change_descriptor {
            descriptors.insert(KeychainKind::Internal, change_descriptor.clone());
        }

        Self {
            descriptors,
            ..self
        }
    }

    /// Populate `descriptor` and `change_descriptor` values using `descriptors`.
    ///
    /// Note: Original `descriptor` and `change_descriptor` are discarded. Other fields are copied
    /// as it is.
    pub fn to_v3(self) -> Self {
        let descriptor = self.descriptors.get(&KeychainKind::External).cloned();
        let change_descriptor = self.descriptors.get(&KeychainKind::Internal).cloned();
        Self {
            descriptor,
            change_descriptor,
            ..self
        }
    }
}

#[cfg(feature = "rusqlite")]
use chain::rusqlite::{types::FromSql, ToSql};

#[cfg(feature = "rusqlite")]
impl<K> ChangeSet<K>
where
    K: Ord + Clone + ToSql + FromSql,
{
    /// Schema name for wallet.
    pub const WALLET_SCHEMA_NAME: &'static str = "bdk_wallet";
    /// Name of table to store the network and the to-be-deprecated fields(descriptor and
    /// change_descriptor)
    pub const WALLET_TABLE_NAME: &'static str = "bdk_wallet";
    /// Name of table to store wallet locked outpoints.
    pub const WALLET_OUTPOINT_LOCK_TABLE_NAME: &'static str = "bdk_wallet_locked_outpoints";
    /// Name of table to store wallet public descriptors.
    pub const WALLET_DESC_TABLE_NAME: &'static str = "bdk_wallet_descriptors";

    /// Get v0 sqlite [ChangeSet] schema
    pub fn schema_v0() -> alloc::string::String {
        format!(
            "CREATE TABLE {} ( \
                id INTEGER PRIMARY KEY NOT NULL CHECK (id = 0), \
                descriptor TEXT, \
                change_descriptor TEXT, \
                network TEXT \
                ) STRICT;",
            Self::WALLET_TABLE_NAME,
        )
    }

    /// Get v1 sqlite [`ChangeSet`] schema. Schema v1 adds a table for locked outpoints.
    pub fn schema_v1() -> alloc::string::String {
        format!(
            "CREATE TABLE {} ( \
                txid TEXT NOT NULL, \
                vout INTEGER NOT NULL, \
                PRIMARY KEY(txid, vout) \
                ) STRICT;",
            Self::WALLET_OUTPOINT_LOCK_TABLE_NAME,
        )
    }

    /// Get v2 sqlite [`ChangeSet`] schema. Schema v2 adds a table for wallet descriptors.
    pub fn schema_v2() -> alloc::string::String {
        format!(
            "CREATE TABLE {} ( \
               keychain TEXT PRIMARY KEY NOT NULL, \
               descriptor TEXT UNIQUE NOT NULL \
            ) STRICT;",
            Self::WALLET_DESC_TABLE_NAME,
        )
    }

    /// Initialize sqlite tables for wallet tables.
    pub fn init_sqlite_tables(db_tx: &chain::rusqlite::Transaction) -> chain::rusqlite::Result<()> {
        crate::rusqlite_impl::migrate_schema(
            db_tx,
            Self::WALLET_SCHEMA_NAME,
            &[&Self::schema_v0(), &Self::schema_v1(), &Self::schema_v2()],
        )?;

        bdk_chain::local_chain::ChangeSet::init_sqlite_tables(db_tx)?;
        bdk_chain::tx_graph::ChangeSet::<ConfirmationBlockTime>::init_sqlite_tables(db_tx)?;
        bdk_chain::keychain_txout::ChangeSet::init_sqlite_tables(db_tx)?;

        Ok(())
    }

    /// Recover a [`ChangeSet`] from sqlite database.
    pub fn from_sqlite(db_tx: &chain::rusqlite::Transaction) -> chain::rusqlite::Result<Self> {
        use bitcoin::{OutPoint, Txid};
        use chain::rusqlite::OptionalExtension;
        use chain::Impl;

        let mut changeset = Self::default();

        let mut wallet_statement = db_tx.prepare(&format!(
            "SELECT descriptor, change_descriptor, network FROM {}",
            Self::WALLET_TABLE_NAME,
        ))?;
        let row = wallet_statement
            .query_row([], |row| {
                Ok((
                    row.get::<_, Option<Impl<Descriptor<DescriptorPublicKey>>>>("descriptor")?,
                    row.get::<_, Option<Impl<Descriptor<DescriptorPublicKey>>>>(
                        "change_descriptor",
                    )?,
                    row.get::<_, Option<Impl<bitcoin::Network>>>("network")?,
                ))
            })
            .optional()?;
        if let Some((desc, change_desc, network)) = row {
            changeset.descriptor = desc.map(Impl::into_inner);
            changeset.change_descriptor = change_desc.map(Impl::into_inner);
            changeset.network = network.map(Impl::into_inner);
        }

        // Select the descriptors
        let mut descriptors_stmt = db_tx.prepare(&format!(
            "SELECT keychain, descriptor FROM {}",
            Self::WALLET_DESC_TABLE_NAME,
        ))?;

        let rows = descriptors_stmt.query_map([], |row| {
            Ok((
                row.get::<_, K>("keychain")?,
                row.get::<_, Impl<Descriptor<DescriptorPublicKey>>>("descriptor")?,
            ))
        })?;

        for row in rows {
            let (keychain, Impl(descriptor)) = row?;
            changeset.descriptors.insert(keychain, descriptor);
        }

        // Select locked outpoints.
        let mut stmt = db_tx.prepare(&format!(
            "SELECT txid, vout FROM {}",
            Self::WALLET_OUTPOINT_LOCK_TABLE_NAME,
        ))?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Impl<Txid>>("txid")?,
                row.get::<_, u32>("vout")?,
            ))
        })?;
        let locked_outpoints = &mut changeset.locked_outpoints.outpoints;
        for row in rows {
            let (Impl(txid), vout) = row?;
            let outpoint = OutPoint::new(txid, vout);
            locked_outpoints.insert(outpoint, true);
        }

        changeset.local_chain = local_chain::ChangeSet::from_sqlite(db_tx)?;
        changeset.tx_graph = tx_graph::ChangeSet::<_>::from_sqlite(db_tx)?;
        changeset.indexer = keychain_txout::ChangeSet::from_sqlite(db_tx)?;

        Ok(changeset)
    }

    /// Persist [`ChangeSet`] to sqlite database.
    pub fn persist_to_sqlite(
        &self,
        db_tx: &chain::rusqlite::Transaction,
    ) -> chain::rusqlite::Result<()> {
        use chain::rusqlite::named_params;
        use chain::Impl;

        let mut descriptor_statement = db_tx.prepare_cached(&format!(
            "INSERT INTO {}(id, descriptor) VALUES(:id, :descriptor) ON CONFLICT(id) DO UPDATE SET descriptor=:descriptor",
            Self::WALLET_TABLE_NAME,
        ))?;
        if let Some(descriptor) = &self.descriptor {
            descriptor_statement.execute(named_params! {
                ":id": 0,
                ":descriptor": Impl(descriptor.clone()),
            })?;
        }

        let mut change_descriptor_statement = db_tx.prepare_cached(&format!(
            "INSERT INTO {}(id, change_descriptor) VALUES(:id, :change_descriptor) ON CONFLICT(id) DO UPDATE SET change_descriptor=:change_descriptor",
            Self::WALLET_TABLE_NAME,
        ))?;
        if let Some(change_descriptor) = &self.change_descriptor {
            change_descriptor_statement.execute(named_params! {
                ":id": 0,
                ":change_descriptor": Impl(change_descriptor.clone()),
            })?;
        }

        let mut network_statement = db_tx.prepare_cached(&format!(
            "INSERT INTO {}(id, network) VALUES(:id, :network) ON CONFLICT(id) DO UPDATE SET network=:network",
            Self::WALLET_TABLE_NAME,
        ))?;
        if let Some(network) = self.network {
            network_statement.execute(named_params! {
                ":id": 0,
                ":network": Impl(network),
            })?;
        }

        // Persist descriptors.
        let mut descriptor_stmt = db_tx.prepare_cached(&format!(
            "INSERT OR IGNORE INTO {}(keychain, descriptor) VALUES(:keychain, :desc)",
            Self::WALLET_DESC_TABLE_NAME
        ))?;

        for (keychain, desc) in &self.descriptors {
            descriptor_stmt.execute(named_params! {
                ":keychain": keychain.clone(),
                ":desc": Impl(desc.clone()),
            })?;
        }

        // Insert or delete locked outpoints.
        let mut insert_stmt = db_tx.prepare_cached(&format!(
            "INSERT OR IGNORE INTO {}(txid, vout) VALUES(:txid, :vout)",
            Self::WALLET_OUTPOINT_LOCK_TABLE_NAME
        ))?;
        let mut delete_stmt = db_tx.prepare_cached(&format!(
            "DELETE FROM {} WHERE txid=:txid AND vout=:vout",
            Self::WALLET_OUTPOINT_LOCK_TABLE_NAME,
        ))?;
        for (&outpoint, &is_locked) in &self.locked_outpoints.outpoints {
            let bitcoin::OutPoint { txid, vout } = outpoint;
            if is_locked {
                insert_stmt.execute(named_params! {
                    ":txid": Impl(txid),
                    ":vout": vout,
                })?;
            } else {
                delete_stmt.execute(named_params! {
                    ":txid": Impl(txid),
                    ":vout": vout,
                })?;
            }
        }

        self.local_chain.persist_to_sqlite(db_tx)?;
        self.tx_graph.persist_to_sqlite(db_tx)?;
        self.indexer.persist_to_sqlite(db_tx)?;
        Ok(())
    }
}

impl<K: Ord> From<local_chain::ChangeSet> for ChangeSet<K> {
    fn from(chain: local_chain::ChangeSet) -> Self {
        Self {
            local_chain: chain,
            ..Default::default()
        }
    }
}

impl<K: Ord> From<IndexedTxGraphChangeSet> for ChangeSet<K> {
    fn from(indexed_tx_graph: IndexedTxGraphChangeSet) -> Self {
        Self {
            tx_graph: indexed_tx_graph.tx_graph,
            indexer: indexed_tx_graph.indexer,
            ..Default::default()
        }
    }
}

impl<K: Ord> From<tx_graph::ChangeSet<ConfirmationBlockTime>> for ChangeSet<K> {
    fn from(tx_graph: tx_graph::ChangeSet<ConfirmationBlockTime>) -> Self {
        Self {
            tx_graph,
            ..Default::default()
        }
    }
}

impl<K: Ord> From<keychain_txout::ChangeSet> for ChangeSet<K> {
    fn from(indexer: keychain_txout::ChangeSet) -> Self {
        Self {
            indexer,
            ..Default::default()
        }
    }
}

impl<K: Ord> From<locked_outpoints::ChangeSet> for ChangeSet<K> {
    fn from(locked_outpoints: locked_outpoints::ChangeSet) -> Self {
        Self {
            locked_outpoints,
            ..Default::default()
        }
    }
}
