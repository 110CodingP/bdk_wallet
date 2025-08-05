use crate::{
    bitcoin::{
        absolute, hashes::Hash, transaction, Amount, BlockHash, Network, OutPoint, ScriptBuf,
        Transaction, TxIn, TxOut, Txid,
    },
    chain::{keychain_txout, local_chain, tx_graph, ConfirmationBlockTime, DescriptorExt, Merge},
    miniscript::descriptor::{Descriptor, DescriptorPublicKey},
    ChangeSet, WalletPersister,
};
use bdk_testenv::{block_id, hash};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

pub const DESCRIPTORS: [&str; 4] = [
    "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/1/*)#ypcpw2dr",
    "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/0/*)#44aqnlam",
    "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/1/*)#emtwewtk",
    "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/0/*)#g0w0ymmw",
];

pub fn create_one_inp_one_out_tx(txid: Txid, amount: u64) -> Transaction {
    Transaction {
        version: transaction::Version::ONE,
        lock_time: absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::new(txid, 0),
            ..TxIn::default()
        }],
        output: vec![TxOut {
            value: Amount::from_sat(amount),
            script_pubkey: ScriptBuf::new(),
        }],
    }
}

pub fn persist_wallet_changeset<Db, CreateDb>(filename: &str, create_db: CreateDb)
where
    CreateDb: Fn(&Path) -> anyhow::Result<Db>,
    Db: WalletPersister,
    Db::Error: Debug,
{
    let temp_dir = tempfile::tempdir().expect("must create tempdir");
    let file_path = temp_dir.path().join(filename);

    let mut db = create_db(&file_path).expect("db should get created");

    let changeset =
        WalletPersister::initialize(&mut db).expect("empty changeset should get loaded");
    assert_eq!(changeset, ChangeSet::default());

    let descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[0].parse().unwrap();
    let change_descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[1].parse().unwrap();

    let mut blocks: BTreeMap<u32, Option<BlockHash>> = BTreeMap::new();
    blocks.insert(0u32, Some(hash!("B")));
    blocks.insert(1u32, Some(hash!("T")));
    blocks.insert(2u32, Some(hash!("C")));
    let local_chain_changeset = local_chain::ChangeSet { blocks };

    let tx1 = Arc::new(create_one_inp_one_out_tx(
        Txid::from_byte_array([0; 32]),
        30_000,
    ));
    let tx2 = Arc::new(create_one_inp_one_out_tx(tx1.compute_txid(), 20_000));

    let block_id = block_id!(1, "BDK");

    let conf_anchor: ConfirmationBlockTime = ConfirmationBlockTime {
        block_id,
        confirmation_time: 123,
    };

    let tx_graph_changeset = tx_graph::ChangeSet::<ConfirmationBlockTime> {
        txs: [tx1.clone()].into(),
        txouts: [].into(),
        anchors: [(conf_anchor, tx1.compute_txid())].into(),
        last_seen: [(tx1.compute_txid(), 100)].into(),
        first_seen: [(tx1.compute_txid(), 80)].into(),
        last_evicted: [(tx1.compute_txid(), 150)].into(),
    };

    let keychain_txout_changeset = keychain_txout::ChangeSet {
        last_revealed: [
            (descriptor.descriptor_id(), 12),
            (change_descriptor.descriptor_id(), 10),
        ]
        .into(),
        spk_cache: [
            (
                descriptor.descriptor_id(),
                [(0u32, ScriptBuf::from_bytes(vec![245, 123, 112]))].into(),
            ),
            (
                change_descriptor.descriptor_id(),
                [
                    (100u32, ScriptBuf::from_bytes(vec![145, 234, 98])),
                    (1000u32, ScriptBuf::from_bytes(vec![5, 6, 8])),
                ]
                .into(),
            ),
        ]
        .into(),
    };

    let mut changeset = ChangeSet {
        descriptor: Some(descriptor.clone()),
        change_descriptor: Some(change_descriptor.clone()),
        network: Some(Network::Bitcoin),
        local_chain: local_chain_changeset,
        tx_graph: tx_graph_changeset,
        indexer: keychain_txout_changeset,
    };

    WalletPersister::persist(&mut db, &changeset).expect("changeset should get persisted");

    let changeset_read = WalletPersister::initialize(&mut db).expect("changeset should get loaded");

    assert_eq!(changeset, changeset_read);

    let mut blocks: BTreeMap<u32, Option<BlockHash>> = BTreeMap::new();
    blocks.insert(4u32, Some(hash!("RE")));
    blocks.insert(5u32, Some(hash!("DB")));
    let local_chain_changeset = local_chain::ChangeSet { blocks };

    let block_id = block_id!(2, "Bitcoin");

    let conf_anchor: ConfirmationBlockTime = ConfirmationBlockTime {
        block_id,
        confirmation_time: 214,
    };

    let tx_graph_changeset = tx_graph::ChangeSet::<ConfirmationBlockTime> {
        txs: [tx2.clone()].into(),
        txouts: [].into(),
        anchors: [(conf_anchor, tx2.compute_txid())].into(),
        last_seen: [(tx2.compute_txid(), 200)].into(),
        first_seen: [(tx2.compute_txid(), 160)].into(),
        last_evicted: [(tx2.compute_txid(), 300)].into(),
    };

    let keychain_txout_changeset = keychain_txout::ChangeSet {
        last_revealed: [(descriptor.descriptor_id(), 14)].into(),
        spk_cache: [(
            change_descriptor.descriptor_id(),
            [
                (102u32, ScriptBuf::from_bytes(vec![8, 45, 78])),
                (1001u32, ScriptBuf::from_bytes(vec![29, 56, 47])),
            ]
            .into(),
        )]
        .into(),
    };

    let changeset_new = ChangeSet {
        descriptor: Some(descriptor),
        change_descriptor: Some(change_descriptor),
        network: Some(Network::Bitcoin),
        local_chain: local_chain_changeset,
        tx_graph: tx_graph_changeset,
        indexer: keychain_txout_changeset,
    };

    WalletPersister::persist(&mut db, &changeset_new).expect("changeset should get persisted");
    let changeset_read_new = WalletPersister::initialize(&mut db).unwrap();

    changeset.merge(changeset_new);

    assert_eq!(changeset, changeset_read_new);
}

pub fn persist_network<Db, CreateDb>(filename: &str, create_db: CreateDb)
where
    CreateDb: Fn(&Path) -> anyhow::Result<Db>,
    Db: WalletPersister,
    Db::Error: Debug,
{
    // create db
    let temp_dir = tempfile::tempdir().expect("must create tempdir");
    let file_path = temp_dir.path().join(filename);
    let mut db = create_db(&file_path).expect("db should get created");

    // initialize db
    let changeset =
        WalletPersister::initialize(&mut db).expect("should initialize and load empty changeset");
    assert_eq!(changeset, ChangeSet::default());

    // persist the network
    let changeset = ChangeSet {
        network: Some(Network::Bitcoin),
        ..ChangeSet::default()
    };
    WalletPersister::persist(&mut db, &changeset).expect("should persist changeset");

    // read the persisted changeset
    let changeset_read =
        WalletPersister::initialize(&mut db).expect("should load persisted changeset");

    assert_eq!(changeset_read.network, Some(Network::Bitcoin));
}

pub fn persist_keychains<Db, CreateDb>(filename: &str, create_db: CreateDb)
where
    CreateDb: Fn(&Path) -> anyhow::Result<Db>,
    Db: WalletPersister,
    Db::Error: Debug,
{
    // create db
    let temp_dir = tempfile::tempdir().expect("must create tempdir");
    let file_path = temp_dir.path().join(filename);
    let mut db = create_db(&file_path).expect("db should get created");

    // initialize db
    let changeset =
        WalletPersister::initialize(&mut db).expect("should initialize and load empty changeset");
    assert_eq!(changeset, ChangeSet::default());

    let descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[0].parse().unwrap();
    let change_descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[1].parse().unwrap();

    let changeset = ChangeSet {
        descriptor: Some(descriptor.clone()),
        change_descriptor: Some(change_descriptor.clone()),
        ..ChangeSet::default()
    };

    WalletPersister::persist(&mut db, &changeset).expect("should persist descriptors");

    let changeset_read =
        WalletPersister::initialize(&mut db).expect("should read persisted changeset");

    assert_eq!(changeset_read.descriptor.unwrap(), descriptor);
    assert_eq!(changeset_read.change_descriptor.unwrap(), change_descriptor);
}

pub fn persist_keychains_reversed<Db, CreateDb>(filename: &str, create_db: CreateDb)
where
    CreateDb: Fn(&Path) -> anyhow::Result<Db>,
    Db: WalletPersister,
    Db::Error: Debug,
{
    // create db
    let temp_dir = tempfile::tempdir().expect("must create tempdir");
    let file_path = temp_dir.path().join(filename);
    let mut db = create_db(&file_path).expect("db should get created");

    // initialize db
    let changeset =
        WalletPersister::initialize(&mut db).expect("should initialize and load empty changeset");
    assert_eq!(changeset, ChangeSet::default());

    let descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[1].parse().unwrap();
    let change_descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[0].parse().unwrap();

    let changeset = ChangeSet {
        descriptor: Some(descriptor.clone()),
        change_descriptor: Some(change_descriptor.clone()),
        ..ChangeSet::default()
    };

    WalletPersister::persist(&mut db, &changeset).expect("should persist descriptors");

    let changeset_read =
        WalletPersister::initialize(&mut db).expect("should read persisted changeset");

    assert_eq!(changeset_read.descriptor.unwrap(), descriptor);
    assert_eq!(changeset_read.change_descriptor.unwrap(), change_descriptor);
}


pub fn persist_single_keychain<Db, CreateDb>(filename: &str, create_db: CreateDb)
where
    CreateDb: Fn(&Path) -> anyhow::Result<Db>,
    Db: WalletPersister,
    Db::Error: Debug,
{
    // create db
    let temp_dir = tempfile::tempdir().expect("must create tempdir");
    let file_path = temp_dir.path().join(filename);
    let mut db = create_db(&file_path).expect("db should get created");

    // initialize db
    let changeset =
        WalletPersister::initialize(&mut db).expect("should initialize and load empty changeset");
    assert_eq!(changeset, ChangeSet::default());

    let descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[1].parse().unwrap();

    let changeset = ChangeSet {
        descriptor: Some(descriptor.clone()),
        ..ChangeSet::default()
    };

    WalletPersister::persist(&mut db, &changeset).expect("should persist descriptors");

    let changeset_read =
        WalletPersister::initialize(&mut db).expect("should read persisted changeset");

    assert_eq!(changeset_read.descriptor.unwrap(), descriptor);
    assert_eq!(changeset_read.change_descriptor, None);
}


