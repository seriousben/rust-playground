#![feature(test)]
#![warn(clippy::pedantic)]
#![allow(clippy::similar_names)]
#![allow(clippy::single_match_else)]
#![allow(clippy::too_many_lines)]

extern crate test;

#[cfg(test)]
mod tests {
    use test::{black_box, Bencher};

    use std::{fs, sync::Arc};

    use anyhow::{anyhow, Context, Ok, Result};
    use rocksdb::{
        BoundColumnFamily, ColumnFamilyDescriptor, OptimisticTransactionDB, Options, TransactionDB,
        TransactionDBOptions, WriteBatchWithTransaction,
    };
    use strum::IntoEnumIterator;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

    // const env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
    //     .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // tracing_subscriber::registry()
    //     .with(tracing_subscriber::fmt::layer().with_filter(env_filter))
    //     .init();

    pub trait OptionExtensions<T> {
        fn expect_lazy<F: FnOnce() -> String>(self, msg_getter: F) -> T;
    }
    impl<T> OptionExtensions<T> for Option<T> {
        fn expect_lazy<F: FnOnce() -> String>(self, msg_getter: F) -> T {
            match self {
                Some(t) => t,
                None => {
                    let msg = msg_getter();
                    panic!("{}", msg);
                }
            }
        }
    }

    #[derive(strum::AsRefStr, strum::Display, strum::EnumIter)]
    pub enum DBColumnFamilies {
        User,
    }

    impl DBColumnFamilies {
        pub fn cf<'a>(&'a self, db: &'a OptimisticTransactionDB) -> Arc<BoundColumnFamily> {
            db.cf_handle(self.as_ref())
                .expect_lazy(|| format!("failed to get column family handle for {}", self.as_ref()))
        }

        pub fn cf_db<'a>(&'a self, db: &'a TransactionDB) -> Arc<BoundColumnFamily> {
            db.cf_handle(self.as_ref())
                .expect_lazy(|| format!("failed to get column family handle for {}", self.as_ref()))
        }
    }

    #[bench]
    fn bench_single_batch_put_no_txn(b: &mut Bencher) {
        let path = ".rocksdb_storage_batch_put_no_txn";
        if fs::exists(path).unwrap() {
            fs::remove_dir_all(path).unwrap();
        }
        fs::create_dir_all(path).unwrap();

        let sm_column_families = DBColumnFamilies::iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf.as_ref(), Options::default()));
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let txn_opts = TransactionDBOptions::default();

        let db = Arc::new(
            TransactionDB::open_cf_descriptors(&db_opts, &txn_opts, path, sm_column_families)
                .unwrap(),
        );

        let data: Vec<u8> = vec![0; 1000];

        b.iter(|| {
            let mut batch_write = WriteBatchWithTransaction::<true>::default();
            let cf = DBColumnFamilies::User.cf_db(&db);
            for i in black_box(0..10000) {
                batch_write.put_cf(&cf, format!("key_{}", i).as_bytes(), &data);
            }
            db.write(batch_write).unwrap();
        });

        b.bytes = 1005 * 10000;
    }

    #[bench]
    fn bench_single_put(b: &mut Bencher) {
        let path = ".rocksdb_storage_single_put";
        if fs::exists(path).unwrap() {
            fs::remove_dir_all(path).unwrap();
        }
        fs::create_dir_all(path).unwrap();

        let sm_column_families = DBColumnFamilies::iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf.as_ref(), Options::default()));
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let txn_opts = TransactionDBOptions::default();

        let db = Arc::new(
            TransactionDB::open_cf_descriptors(&db_opts, &txn_opts, path, sm_column_families)
                .unwrap(),
        );

        let data: Vec<u8> = vec![0; 1000];

        b.iter(|| {
            let txn = db.transaction();
            let cf = DBColumnFamilies::User.cf_db(&db);
            for i in black_box(0..10000) {
                txn.put_cf(&cf, format!("key_{}", i).as_bytes(), &data)
                    .context("failed to put data")
                    .unwrap();
            }
            txn.commit().unwrap();
        });

        b.bytes = 1005 * 10000;
    }

    #[bench]
    fn bench_single_batch_put(b: &mut Bencher) {
        let path = ".rocksdb_storage_batch_put";
        if fs::exists(path).unwrap() {
            fs::remove_dir_all(path).unwrap();
        }
        fs::create_dir_all(path).unwrap();

        let sm_column_families = DBColumnFamilies::iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf.as_ref(), Options::default()));
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let txn_opts = TransactionDBOptions::default();

        let db = Arc::new(
            TransactionDB::open_cf_descriptors(&db_opts, &txn_opts, path, sm_column_families)
                .unwrap(),
        );

        let data: Vec<u8> = vec![0; 1000];

        b.iter(|| {
            let txn = db.transaction();
            let mut batch_write = WriteBatchWithTransaction::<true>::default();
            let cf = DBColumnFamilies::User.cf_db(&db);
            for i in black_box(0..10000) {
                batch_write.put_cf(&cf, format!("key_{}", i).as_bytes(), &data);
            }
            txn.rebuild_from_writebatch(&batch_write).unwrap();
            txn.commit().unwrap();
        });

        b.bytes = 1005 * 10000;
    }
}
