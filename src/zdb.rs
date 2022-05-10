//! This crate provides a wrapper for a client to a 0-db running as a separate process on the same
//! system. The 0-db must be running in user mode

use redis::ConnectionLike;
use redis::RedisError;
use scheduled_thread_pool::ScheduledThreadPool;

use async_trait::async_trait;

use crate::{Error, Export, ExportStore, NbdError};

use std::sync::Arc;

pub type Key = u32;

#[allow(missing_debug_implementations)]
#[derive(Clone)]
pub struct Zdb {
    client: redis::Client,
    // Thread pool used by r2d2 to spawn collections. We share one to avoid every connection pool
    // in every namespace allocating one.
    spawn_pool: Arc<ScheduledThreadPool>,
    // upon connection we are always connected to the default namespace
    default_namespace: Collection,
}

// impl std::fmt::Debug for Zdb {
//     // TODO
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         write!(f, "zdb debug - TODO")
//     }
// }

#[async_trait]
impl ExportStore for Zdb {
    type Export = Collection;
    type Error = Error;

    async fn list_exports(&self) -> Result<Vec<String>, Self::Error> {
        Ok(vec![
            "disk1".to_string(),
            "disk2".to_string(),
            "disk3".to_string(),
        ])
    }

    async fn get_export(&self, name: &str) -> Result<Option<Self::Export>, Self::Error> {
        Ok(Some(self.collection(name)))
    }

    // TODO: metadata
}

#[derive(Debug, Clone)]
pub struct Collection {
    pool: r2d2::Pool<ZdbConnectionManager>,
}

const SECTOR_SIZE: u64 = 4 * 1024;

#[async_trait]
impl Export for Collection {
    type Error = NbdError;

    async fn read(&mut self, start: u64, end: u64) -> Result<Vec<u8>, Self::Error> {
        let start_sector = start / SECTOR_SIZE;
        let end_sector = end / SECTOR_SIZE;

        let mut data = Vec::with_capacity((end - start) as usize);
        for sector in start_sector..=end_sector {
            data.extend_from_slice(
                &redis::cmd("GET")
                    .arg(&sector.to_be_bytes())
                    .query::<Option<Vec<u8>>>(&mut *self.pool.get()?)?
                    .or(Some(vec![0; SECTOR_SIZE as usize].into()))
                    .map(|v| {
                        // drop first bytes of the first sector
                        if sector == start_sector {
                            Vec::from(&v[(start % SECTOR_SIZE) as usize..])
                        } else {
                            Vec::from(&v[..])
                        }
                    })
                    .map(|mut v| {
                        // drop last bytes of the last sector
                        // if we only read in 1 sector it might have been reduced already when
                        // cutting to the start
                        if sector == end_sector {
                            v.drain(
                                ((end % SECTOR_SIZE) - (SECTOR_SIZE - v.len() as u64)) as usize..,
                            );
                            v
                        } else {
                            v
                        }
                    })
                    .unwrap(),
            );
        }

        debug_assert!(data.len() as u64 == end - start);

        Ok(data)
    }

    async fn write(&mut self, start: u64, data: &[u8]) -> Result<(), Self::Error> {
        let mut data = data;
        let mut start_sector = start / SECTOR_SIZE;
        let end_sector = (start + data.len() as u64) / SECTOR_SIZE;

        let single_sector_write = start / SECTOR_SIZE == (start + data.len() as u64) / SECTOR_SIZE;

        // if the data is not aligned, read the block for the first sector, modify the bytes
        // which need to be written, and write the block back
        if start % SECTOR_SIZE != 0 {
            let start_block = self
                .get(start_sector)?
                .or(Some(vec![0u8; SECTOR_SIZE as usize]))
                .unwrap();
            let boundary = if single_sector_write {
                ((start + data.len() as u64) % SECTOR_SIZE) as usize
            } else {
                SECTOR_SIZE as usize
            };
            let (new_data, data_rest) = data.split_at(boundary - (start % SECTOR_SIZE) as usize);
            data = data_rest;
            let mut block = start_block
                .clone() // TODO: we can actually split off here
                .into_iter()
                .take((start % SECTOR_SIZE) as usize)
                .collect::<Vec<_>>();
            block.extend_from_slice(new_data);
            if single_sector_write {
                block.extend(start_block.into_iter().skip(boundary));
            }
            debug_assert!(block.len() == SECTOR_SIZE as usize);
            self.set(start_sector, data)?;
            start_sector += 1;
        }

        // data is now aligned to sector size
        debug_assert!(data.len() as u64 % SECTOR_SIZE == 0);

        for (idx, sector) in (start_sector..end_sector).enumerate() {
            let sector_data = &data[idx * SECTOR_SIZE as usize..(idx + 1) * SECTOR_SIZE as usize];
            self.set(sector, sector_data)?;
        }

        // handle end sector
        if !single_sector_write && (start + data.len() as u64) % SECTOR_SIZE != 0 {
            let last_sector_data = self
                .get(end_sector)?
                .or(Some(vec![0u8; SECTOR_SIZE as usize]))
                .unwrap();
            // boundary in the block, past here we need to extend from the read data
            let boundary = (data.len() as u64 % SECTOR_SIZE) as usize;
            let mut block = Vec::from(&data[data.len() - boundary..]);
            block.extend_from_slice(&last_sector_data[boundary..]);
            debug_assert!(block.len() as u64 == SECTOR_SIZE);
            self.set(end_sector, data)?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        // everything is flushed automatically anyhow
        Ok(())
    }

    async fn trim(&mut self) -> Result<(), Self::Error> {
        // TODO
        Err(NbdError::NotSup)
    }

    async fn cache(&mut self) -> Result<(), Self::Error> {
        // TODO
        Err(NbdError::NotSup)
    }

    async fn write_zeroes(&mut self, start: u64, end: u64) -> Result<(), Self::Error> {
        Err(NbdError::NotSup)
    }

    async fn block_status(&mut self) -> Result<(), Self::Error> {
        // TODO
        Err(NbdError::NotSup)
    }

    async fn resize(&mut self) -> Result<(), Self::Error> {
        // TODO
        Err(NbdError::NotSup)
    }
}

enum Direction {
    Forward,
    Backward,
}

/// An iterator over all keys in a collection.
/// Leaking this value will cause the underlying connection to leak.
#[allow(missing_debug_implementations)]
pub struct CollectionKeys {
    conn: r2d2::PooledConnection<ZdbConnectionManager>,
    cursor: Option<Vec<u8>>,
    /// since the scan method returns more than 1 key, yet does not specify exactly how many,
    /// we keep track of an internal buffer
    buffer: Vec<ScanEntry>,
    /// keep track of which entry in the buffer we are at
    buffer_idx: usize,
    /// direction of iteration
    direction: Direction,
}

// Yes, this is a vec, and yes, this only represents a single element. It is what it is.
type ScanEntry = Vec<(Vec<u8>, u32, u32)>;

/// Zdb connection manager to be used by the r2d2 crate. Because namespaces in zdb are tied to the
/// actual connection, we need to manage connection pools on namespace lvl.
#[derive(Debug, Clone)]
struct ZdbConnectionManager {
    namespace: Option<String>,
    client: redis::Client,
}

impl Zdb {
    pub fn new(port: u16) -> Zdb {
        let client = redis::Client::open(format!("redis://localhost:{}", port))
            .expect("Could not connect to zdb");
        let spawn_pool = Arc::new(ScheduledThreadPool::new(2));
        let default_namespace = Collection::new(client.clone(), None, spawn_pool.clone());
        Zdb {
            client,
            spawn_pool,
            default_namespace,
        }
    }

    /// Get a reference to a `Collection`.
    pub fn collection(&self, name: &str) -> Collection {
        Collection::new(
            self.client.clone(),
            Some(name.into()),
            self.spawn_pool.clone(),
        )
    }
}

impl Default for Zdb {
    fn default() -> Self {
        // default port 9900
        Self::new(9900)
    }
}

impl Collection {
    fn new(
        client: redis::Client,
        namespace: Option<String>,
        spawn_pool: Arc<ScheduledThreadPool>,
    ) -> Collection {
        let manager = ZdbConnectionManager::new(client, namespace);
        // All these options should be verified and refined once we have some load estimate
        let pool = r2d2::Builder::new()
            .max_size(10)
            .min_idle(Some(1))
            .thread_pool(spawn_pool)
            // TODO: set connection lifetimes
            // TODO: other configuration
            .build_unchecked(manager);
        Collection { pool }
    }
}

impl Collection {
    fn set(&mut self, sector: u64, data: &[u8]) -> Result<(), NbdError> {
        debug_assert!(data.len() as u64 == SECTOR_SIZE);
        Ok(redis::cmd("SET")
            .arg(&sector.to_be_bytes()[..])
            .arg(data)
            .query(&mut *self.pool.get()?)?)
    }

    fn get(&mut self, sector: u64) -> Result<Option<Vec<u8>>, NbdError> {
        Ok(redis::cmd("GET")
            .arg(&sector.to_be_bytes())
            .query(&mut *self.pool.get()?)?)
    }

    // fn keys(&mut self) -> Result<Box<dyn Iterator<Item = Record> + Send>, StorageError> {
    //     Ok(Box::new(CollectionKeys {
    //         conn: self.pool.get()?,
    //         cursor: None,
    //         buffer: Vec::new(),
    //         buffer_idx: 0,
    //         direction: Direction::Forward,
    //     }))
    // }

    // fn rev(&mut self) -> Result<Box<dyn Iterator<Item = Record> + Send>, StorageError> {
    //     Ok(Box::new(CollectionKeys {
    //         conn: self.pool.get()?,
    //         cursor: None,
    //         buffer: Vec::new(),
    //         buffer_idx: 0,
    //         direction: Direction::Backward,
    //     }))
    // }
}

// impl Iterator for CollectionKeys {
//     type Item = Record;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         // Note that entries are actually single element vectors of tuples, not regular tuples.
//         // If we have a buffer from a previous cmd execution, check for an entry there first
//         if self.buffer_idx < self.buffer.len() {
//             self.buffer_idx += 1;
//             let rec = &self.buffer[self.buffer_idx - 1][0];
//             let key = read_le_key(&rec.0);
//             return Some(Record {
//                 key: key,
//                 size: Some(rec.1),
//                 timestamp: Some(rec.2),
//             });
//         }
//         // No more keys in buffer, fetch a new buffer from the remote
//         let mut scan_cmd = match self.direction {
//             Direction::Forward => redis::cmd("SCAN"),   //forward scan
//             Direction::Backward => redis::cmd("RSCAN"), //backward scan
//         };
//         // If we have a cursor (from a previous execution), set it
//         // Annoyingly, the redis lib does not allow to set a reference to a vec as argument, so
//         // we take here, but that's fine, since a result will update the cursor anyway.
//         if let Some(cur) = self.cursor.take() {
//             scan_cmd.arg(cur);
//         }
//         let res: (Vec<u8>, Vec<ScanEntry>) = match scan_cmd.query(&mut *self.conn) {
//             Ok(r) => Some(r),
//             Err(_) => None,
//         }?;
//
//         // set the new cursor
//         self.cursor = Some(res.0);
//         // set the buffer
//         self.buffer = res.1;
//         // reset buffer pointer, set it to one as we will return the first element
//         self.buffer_idx = 1;
//
//         let rec = &self.buffer[0][0];
//         let key = read_le_key(&rec.0);
//         Some(Record {
//             key: key,
//             size: Some(rec.1),
//             timestamp: Some(rec.2),
//         })
//     }
// }
//
// fn read_le_key(input: &[u8]) -> Key {
//     let (int_bytes, _) = input.split_at(std::mem::size_of::<Key>());
//     Key::from_le_bytes(
//         int_bytes
//             .try_into()
//             .expect("could not convert bytes to key"),
//     )
// }

impl ZdbConnectionManager {
    fn new(client: redis::Client, namespace: Option<String>) -> ZdbConnectionManager {
        ZdbConnectionManager { client, namespace }
    }
}

// This implementation is mainly taken from the r2d2 redis implementation, with the difference
// being that we set the namespace on the connections
impl r2d2::ManageConnection for ZdbConnectionManager {
    type Connection = redis::Connection;
    type Error = redis::RedisError;

    fn connect(&self) -> Result<redis::Connection, redis::RedisError> {
        let mut conn = self.client.get_connection()?;
        if let Some(ref ns) = self.namespace {
            let namespaces: Vec<String> = redis::cmd("NSLIST").query(&mut conn)?;
            let mut exists = false;
            for existing_ns in namespaces {
                if &existing_ns == ns {
                    exists = true;
                    break;
                }
            }
            if !exists {
                redis::cmd("NSNEW").arg(ns).query(&mut conn)?;
            }
            redis::cmd("SELECT").arg(ns).query(&mut conn)?;
        }
        Ok(conn)
    }

    fn is_valid(&self, conn: &mut redis::Connection) -> Result<(), redis::RedisError> {
        // Don't send ping here as that spamms the connection on our use case
        // redis::cmd("PING").query(conn)
        if conn.is_open() {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "connection broken",
            ))?
        }
    }

    fn has_broken(&self, conn: &mut redis::Connection) -> bool {
        !conn.is_open()
    }
}

impl From<RedisError> for NbdError {
    fn from(_: RedisError) -> Self {
        NbdError::Io
    }
}

impl From<r2d2::Error> for NbdError {
    fn from(_: r2d2::Error) -> Self {
        NbdError::Io
    }
}
