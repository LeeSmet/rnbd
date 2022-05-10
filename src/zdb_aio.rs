//! This crate provides a wrapper for a client to a 0-db running as a separate process on the same
//! system. The 0-db must be running in user mode

use redis::{RedisConnectionInfo, RedisError};

use async_trait::async_trait;

use crate::{Error, Export, ExportStore, NbdError};

#[allow(missing_debug_implementations)]
#[derive(Clone)]
pub struct Zdb {
    addr: String,
    // client: redis::aio::Client,
    // Thread pool used by r2d2 to spawn collections. We share one to avoid every connection pool
    // in every namespace allocating one.
    // spawn_pool: Arc<ScheduledThreadPool>,
    // upon connection we are always connected to the default namespace
    default_namespace: Collection,
}

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
        Ok(Some(self.collection(name).await))
    }

    // TODO: metadata
}

#[allow(missing_debug_implementations)]
#[derive(Clone)]
pub struct Collection {
    conn: redis::aio::MultiplexedConnection,
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
                &self
                    .get(sector)
                    .await?
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
                .get(start_sector)
                .await?
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
            self.set(start_sector, data).await?;
            start_sector += 1;
        }

        // data is now aligned to sector size
        debug_assert!(data.len() as u64 % SECTOR_SIZE == 0);

        for (idx, sector) in (start_sector..end_sector).enumerate() {
            let sector_data = &data[idx * SECTOR_SIZE as usize..(idx + 1) * SECTOR_SIZE as usize];
            self.set(sector, sector_data).await?;
        }

        // handle end sector
        if !single_sector_write && (start + data.len() as u64) % SECTOR_SIZE != 0 {
            let last_sector_data = self
                .get(end_sector)
                .await?
                .or(Some(vec![0u8; SECTOR_SIZE as usize]))
                .unwrap();
            // boundary in the block, past here we need to extend from the read data
            let boundary = (data.len() as u64 % SECTOR_SIZE) as usize;
            let mut block = Vec::from(&data[data.len() - boundary..]);
            block.extend_from_slice(&last_sector_data[boundary..]);
            debug_assert!(block.len() as u64 == SECTOR_SIZE);
            self.set(end_sector, data).await?;
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

impl Zdb {
    pub async fn new(port: u16) -> Zdb {
        let addr = format!("localhost:{}", port);
        let default_namespace = Collection::new(addr.clone(), None).await;
        Zdb {
            addr,
            default_namespace,
        }
    }

    /// Get a reference to a `Collection`.
    pub async fn collection(&self, name: &str) -> Collection {
        Collection::new(self.addr.clone(), Some(name.into())).await
    }
}
impl Collection {
    async fn new(addr: String, namespace: Option<String>) -> Collection {
        let stream = tokio::net::TcpStream::connect(&addr)
            .await
            .expect("can create tcp connection to 0-db");
        let (mut conn, driver) =
            redis::aio::MultiplexedConnection::new(&RedisConnectionInfo::default(), stream)
                .await
                .expect("can create multiplexed connection to 0-db");
        // Permanently poll the driver, TODO: is this good enough?
        tokio::spawn(driver);

        if let Some(ref ns) = namespace {
            let namespaces: Vec<String> =
                redis::cmd("NSLIST").query_async(&mut conn).await.unwrap();
            let mut exists = false;
            for existing_ns in namespaces {
                if &existing_ns == ns {
                    exists = true;
                    break;
                }
            }
            if !exists {
                let _: () = redis::cmd("NSNEW")
                    .arg(ns)
                    .query_async(&mut conn)
                    .await
                    .expect("could not create new namespace");
            }
            redis::cmd("SELECT")
                .arg(ns)
                .query_async::<_, ()>(&mut conn)
                .await
                .expect("could not select namespace");
        }
        Collection { conn }
    }
}

impl Collection {
    async fn set(&mut self, sector: u64, data: &[u8]) -> Result<(), NbdError> {
        debug_assert!(data.len() as u64 == SECTOR_SIZE);
        Ok(redis::cmd("SET")
            .arg(&sector.to_be_bytes()[..])
            .arg(data)
            .query_async(&mut self.conn.clone())
            .await?)
    }

    async fn get(&mut self, sector: u64) -> Result<Option<Vec<u8>>, NbdError> {
        Ok(redis::cmd("GET")
            .arg(&sector.to_be_bytes())
            .query_async(&mut self.conn.clone())
            .await?)
    }
}

impl From<RedisError> for NbdError {
    fn from(_: RedisError) -> Self {
        NbdError::Io
    }
}

// impl From<r2d2::Error> for NbdError {
//     fn from(_: r2d2::Error) -> Self {
//         NbdError::Io
//     }
// }
