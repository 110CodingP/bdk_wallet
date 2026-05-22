use alloc::collections::btree_map::BTreeMap;
use bdk_chain::{
    indexed_tx_graph, keychain_txout, local_chain, tx_graph, ConfirmationBlockTime, Merge,
};
use miniscript::{Descriptor, DescriptorPublicKey};
use serde::{Deserialize, Serialize};

use crate::locked_outpoints;

type IndexedTxGraphChangeSet =
    indexed_tx_graph::ChangeSet<ConfirmationBlockTime, keychain_txout::ChangeSet>;


#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ChangeSet<K: Ord> {
    /// Wallet descriptors
    pub descriptors: BTreeMap<K, Descriptor<DescriptorPublicKey>>,
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
}

impl<K: Ord> Default for ChangeSet<K> {
    fn default() -> Self {
        Self {
            network: None,
            descriptors: Default::default(),
            local_chain: local_chain::ChangeSet::default(),
            tx_graph: tx_graph::ChangeSet::<ConfirmationBlockTime>::default(),
            indexer: keychain_txout::ChangeSet::default(),
            locked_outpoints: locked_outpoints::ChangeSet::default(),
        }
    }
}

impl<K> Merge for ChangeSet<K>
where K: Ord
{
    /// Merge another [`ChangeSet`] into itself.
    fn merge(&mut self, other: Self) {
        // merge network
        if other.network.is_some() {
            debug_assert!(
                self.network.is_none() || self.network == other.network,
                "network must never change"
            );
            self.network = other.network;
        }

        // merge descriptors
        self.descriptors.extend(other.descriptors);

        // merge locked outpoints
        self.locked_outpoints.merge(other.locked_outpoints);

        Merge::merge(&mut self.local_chain, other.local_chain);
        Merge::merge(&mut self.tx_graph, other.tx_graph);
        Merge::merge(&mut self.indexer, other.indexer);
    }

    fn is_empty(&self) -> bool {
        self.descriptors.is_empty()
            && self.network.is_none()
            && self.local_chain.is_empty()
            && self.tx_graph.is_empty()
            && self.indexer.is_empty()
            && self.locked_outpoints.is_empty()
    }
}

#[cfg(feature = "rusqlite")]
use chain::{
    rusqlite::{self, types::FromSql, OptionalExtension, ToSql},
    Impl,
};

#[cfg(feature = "rusqlite")]
impl<K> ChangeSet<K>
where K: Ord + Clone + ToSql + FromSql,
{
    /// Schema name for wallet.
    pub const WALLET_SCHEMA_NAME: &'static str = "bdk_wallet";
    /// Name of table to store wallet descriptors and network.
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

    pub fn schema_v2() -> alloc::string::String {
        let create_desc_table = format!(
            "CREATE TABLE {} ( \
               keychain TEXT PRIMARY KEY NOT NULL, \
               descriptor TEXT UNIQUE NOT NULL \
            ) STRICT;",
            Self::WALLET_DESC_TABLE_NAME,
        );
        let extract_descriptor = format!(
            "INSERT INTO {} (keychain, descriptor) \
            SELECT 0, {}.descriptor \
            FROM {};",
            Self::WALLET_TABLE_NAME,
            Self::WALLET_DESC_TABLE_NAME,
            Self::WALLET_DESC_TABLE_NAME
        );
        let extract_change_descriptor = format!(
            "INSERT INTO {} (keychain, descriptor) \
            SELECT 1, {}.change_descriptor \
            FROM {} \
            WHERE {}.change_descriptor IS NOT NULL;",
            Self::WALLET_TABLE_NAME,
            Self::WALLET_DESC_TABLE_NAME,
            Self::WALLET_DESC_TABLE_NAME,
            Self::WALLET_DESC_TABLE_NAME
        );
        let drop_desc_col = format!("ALTER TABLE {} DROP COLUMN descriptor;", Self::WALLET_TABLE_NAME);
        let drop_change_desc_col = format!("ALTER TABLE {} DROP COLUMN change_descriptor;", Self::WALLET_TABLE_NAME);
        format!("{create_desc_table} {extract_descriptor} {extract_change_descriptor} {drop_desc_col} {drop_change_desc_col}")
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
            "SELECT network FROM {}",
            Self::WALLET_TABLE_NAME,
        ))?;
        let row = wallet_statement
            .query_row([], |row|
                    row.get::<_, Option<Impl<bitcoin::Network>>>("network") )
            .optional()?;
        if let Some(network) = row {
            changeset.network = network.map(Impl::into_inner);
        }

        let mut descriptor_stmt = db_tx.prepare(&format!(
            "SELECT keychain, descriptor FROM {}",
            Self::WALLET_TABLE_NAME
        ))?;

        let rows = descriptor_stmt.query_map([], |row| {
            Ok((
                row.get::<_, K>("keychain")?,
                row.get::<_, Impl<Descriptor<DescriptorPublicKey>>>("descriptor")?,
            ))
        })?;

        for row in rows {
            let (keychain, Impl(descriptor)) = row?;
            changeset.descriptors.insert(keychain.clone(), descriptor);
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
