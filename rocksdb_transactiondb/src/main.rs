#![warn(clippy::pedantic)]
#![allow(clippy::similar_names)]
#![allow(clippy::single_match_else)]
#![allow(clippy::too_many_lines)]
use std::{fs, sync::Arc};

use anyhow::{anyhow, Context, Ok, Result};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, OptimisticTransactionDB, Options, TransactionDB,
    TransactionDBOptions,
};
use strum::IntoEnumIterator;
use tokio::sync::oneshot;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

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

#[tokio::main]
async fn main() -> Result<()> {
    {
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_filter(env_filter))
            .init();
    }

    let path = ".rocksdb_storage";
    fs::create_dir_all(path)?;

    let sm_column_families = DBColumnFamilies::iter()
        .map(|cf| ColumnFamilyDescriptor::new(cf.as_ref(), Options::default()));
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let txn_opts = TransactionDBOptions::default();

    let db = Arc::new(TransactionDB::open_cf_descriptors(
        &db_opts,
        &txn_opts,
        path,
        sm_column_families,
    )?);

    // ################################################################
    // setup database
    // ################################################################
    {
        let txn_setup = db.transaction();

        txn_setup.put_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", b"user1")?;
        txn_setup.put_cf(&DBColumnFamilies::User.cf_db(&db), b"user2", b"user2")?;
        txn_setup.put_cf(&DBColumnFamilies::User.cf_db(&db), b"user3", b"user3")?;

        txn_setup.commit()?;
    }

    // ################################################################
    // get_cf does not lock key in transaction
    // ################################################################
    {
        let txn1 = db.transaction();
        let txn2 = db.transaction();

        txn1.get_cf(&DBColumnFamilies::User.cf_db(&db), b"user1")?;
        let res = txn2.put_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", b"user1-txn2");
        assert!(res.is_ok(), "putting in txn2 should work");
    }

    // ################################################################
    // ERROR: try overwrites to same key in different transactions
    // since transactions are serialized, this causes a timeout.
    // ################################################################
    {
        let txn1 = db.transaction();
        let txn2 = db.transaction();

        txn1.put_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", b"user1-txn1")?;
        let res = txn2.put_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", b"user1-txn2");
        assert!(res.is_err(), "put in txn2 should fail");
        assert_eq!(
            res.err().unwrap().to_string(),
            "Operation timed out: Timeout waiting to lock key"
        );
    }

    // ################################################################
    // get_for_update EXCLUSIVE=false does not prevent get in other txn
    // ################################################################
    {
        let txn1 = db.transaction();
        let txn2 = db.transaction();

        txn1.get_for_update_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", false)?;
        let res = txn2.get_cf(&DBColumnFamilies::User.cf_db(&db), b"user1");
        assert!(res.is_ok(), "get in txn2 should work");
    }

    // ################################################################
    // ERROR: get_for_update EXCLUSIVE=false locks the key for puts in other txn
    // ################################################################
    {
        let txn1 = db.transaction();
        let txn2 = db.transaction();

        txn1.get_for_update_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", false)?;
        let res = txn2.put_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", b"user1-txn2");
        assert!(res.is_err(), "put in txn2 should fail");
        assert_eq!(
            res.err().unwrap().to_string(),
            "Operation timed out: Timeout waiting to lock key"
        );
    }

    // ################################################################
    // ERROR: get_for_update EXCLUSIVE=true prevents any get_for_update in other txn
    // ################################################################
    {
        let txn1 = db.transaction();
        let txn2 = db.transaction();

        txn1.get_for_update_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", true)?;
        let res = txn2.get_for_update_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", true);
        assert!(res.is_err(), "put in txn2 should fail");
        assert_eq!(
            res.err().unwrap().to_string(),
            "Operation timed out: Timeout waiting to lock key"
        );

        let res = txn2.get_for_update_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", false);
        assert!(res.is_err(), "put in txn2 should fail");
        assert_eq!(
            res.err().unwrap().to_string(),
            "Operation timed out: Timeout waiting to lock key"
        );
    }

    // ################################################################
    // try overwrites to different keys in different transactions (with tasks)
    // ################################################################
    {
        let mut task_handles = vec![];
        let db_task = db.clone();
        let (tx, mut rx) = oneshot::channel();
        task_handles.push(tokio::spawn(async move {
            let result = async move {
                let txn1 = db_task.transaction();

                txn1.put_cf(
                    &DBColumnFamilies::User.cf_db(&db_task),
                    b"user1",
                    b"user1-txn1",
                )?;

                txn1.commit()?;
                tx.send(()).map_err(|e| anyhow!("send error: {e:?}"))?;

                Ok(())
            }
            .await;

            match result {
                Result::Ok(()) => (),
                Err(err) => panic!("txn1 error: {err:?}"),
            }
        }));

        let db_task2 = db.clone();
        task_handles.push(tokio::spawn(async move {
            let result = async move {
                tokio::select! {
                    _ = &mut rx => (),
                    () = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                        panic!("timeout");
                    }
                }
                let txn2 = db_task2.transaction();

                txn2.put_cf(
                    &DBColumnFamilies::User.cf_db(&db_task2),
                    b"user1",
                    b"user1-txn2",
                )
                .context("cannot put in txn2")?;

                txn2.commit()?;

                Ok(())
            }
            .await;

            match result {
                Result::Ok(()) => (),
                Err(err) => panic!("txn1 error: {err:?}"),
            }
        }));

        for handle in task_handles {
            handle.await?;
        }

        let raw_user = db
            .get_cf(&DBColumnFamilies::User.cf_db(&db), b"user1")?
            .expect("user1 not found");

        let user = String::from_utf8(raw_user).unwrap();
        assert_eq!("user1-txn2", user);
    }

    // ################################################################
    // ERROR: get_for_update EXCLUSIVE=true prevents any get_for_update in other txn
    // ################################################################
    {
        let txn1 = db.transaction();
        let txn2 = db.transaction();

        txn1.get_for_update_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", true)?;
        let res = txn2.get_for_update_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", true);
        assert!(res.is_err(), "put in txn2 should fail");
        assert_eq!(
            res.err().unwrap().to_string(),
            "Operation timed out: Timeout waiting to lock key"
        );

        let res = txn2.get_for_update_cf(&DBColumnFamilies::User.cf_db(&db), b"user1", false);
        assert!(res.is_err(), "put in txn2 should fail");
        assert_eq!(
            res.err().unwrap().to_string(),
            "Operation timed out: Timeout waiting to lock key"
        );
    }

    Ok(())
}
