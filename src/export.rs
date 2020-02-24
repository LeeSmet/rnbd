use async_trait::async_trait;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use std::io::SeekFrom;

use super::Error;
use super::NbdError;

#[async_trait]
pub trait ExportStore {
    type Export: Export;
    type Error: Into<Error>;

    async fn list_exports(&self) -> Result<Vec<String>, Self::Error>;
    async fn get_export(&self, name: &str) -> Result<Option<Self::Export>, Self::Error>;

    // TODO: metadata
}

#[async_trait]
pub trait Export {
    type Error: Into<NbdError>;

    async fn read(&mut self, start: u64, end: u64) -> Result<Vec<u8>, Self::Error>;
    async fn write(&mut self, start: u64, data: &[u8]) -> Result<(), Self::Error>;
    async fn flush(&mut self) -> Result<(), Self::Error>;
    async fn trim(&mut self) -> Result<(), Self::Error>; // TODO
    async fn cache(&mut self) -> Result<(), Self::Error>; // TODO
    async fn write_zeroes(&mut self, start: u64, end: u64) -> Result<(), Self::Error>;
    async fn block_status(&mut self) -> Result<(), Self::Error>; // TODO
    async fn resize(&mut self) -> Result<(), Self::Error>; // TODO
}

#[async_trait]
impl<T> Export for T
where
    T: AsyncRead + AsyncWrite + AsyncSeek + Unpin + Send,
{
    type Error = NbdError;

    async fn read(&mut self, start: u64, end: u64) -> Result<Vec<u8>, Self::Error> {
        self.seek(SeekFrom::Start(start)).await?;
        let mut contents = vec![0; (end - start) as usize];
        self.read_exact(&mut contents[..]).await?;
        Ok(contents)
    }

    async fn write(&mut self, start: u64, data: &[u8]) -> Result<(), Self::Error> {
        self.seek(SeekFrom::Start(start)).await?;
        Ok(self.write_all(data).await?)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(tokio::io::AsyncWriteExt::flush(self).await?)
    }

    async fn trim(&mut self) -> Result<(), Self::Error> {
        Err(NbdError::NotSup)
    }

    async fn cache(&mut self) -> Result<(), Self::Error> {
        Err(NbdError::NotSup)
    }

    async fn write_zeroes(&mut self, start: u64, end: u64) -> Result<(), Self::Error> {
        self.seek(SeekFrom::Start(start)).await?;
        let data = vec![0u8; (start - end) as usize];
        Ok(self.write_all(&data).await?)
    }

    async fn block_status(&mut self) -> Result<(), Self::Error> {
        Err(NbdError::NotSup)
    } // TODO

    async fn resize(&mut self) -> Result<(), Self::Error> {
        Err(NbdError::NotSup)
    } // TODO
}

#[derive(Debug, Default, Clone)]
pub struct TmpStore;

#[async_trait]
impl ExportStore for TmpStore {
    type Export = tokio::fs::File;
    type Error = Error;

    async fn list_exports(&self) -> Result<Vec<String>, Self::Error> {
        Ok(vec!["disk1".to_owned()])
    }

    async fn get_export(&self, name: &str) -> Result<Option<Self::Export>, Self::Error> {
        let mut export = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(name)
            .await?;
        export.set_len(1024 * 1024 * 1024).await?;
        Ok(Some(export))
    }
}

#[derive(Debug, Clone)]
pub struct SledStore {
    db: sled::Db,
}

impl SledStore {
    pub fn new(db: sled::Db) -> SledStore {
        SledStore { db }
    }
}

#[async_trait]
impl ExportStore for SledStore {
    type Export = SledExport;
    type Error = Error;

    async fn list_exports(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self
            .db
            .tree_names()
            .into_iter()
            .map(|b| String::from_utf8(Vec::from(&*b)).expect("tree name must be valid utf-8"))
            .collect())
    }

    async fn get_export(&self, name: &str) -> Result<Option<Self::Export>, Self::Error> {
        // TODO check if tree exists
        Ok(Some(SledExport::new(self.db.open_tree(name).unwrap())))
    }
}

#[derive(Debug)]
pub struct SledExport {
    db: sled::Tree,
}

impl SledExport {
    fn new(db: sled::Tree) -> SledExport {
        SledExport { db }
    }
}

const SECTOR_SIZE: u64 = 4 * 1024;
#[async_trait]
impl Export for SledExport {
    type Error = NbdError;

    async fn read(&mut self, start: u64, end: u64) -> Result<Vec<u8>, Self::Error> {
        //    let start_sector
        let start_sector = start / SECTOR_SIZE;
        let end_sector = end / SECTOR_SIZE;

        let mut data = Vec::with_capacity((end - start) as usize);
        for sector in start_sector..=end_sector {
            data.extend_from_slice(
                &self
                    .db
                    .get(sector.to_be_bytes())?
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

        let mut batch = sled::Batch::default();

        let single_sector_write = start / SECTOR_SIZE == (start + data.len() as u64) / SECTOR_SIZE;

        // if the data is not aligned, read the block for the first sector, modify the bytes
        // which need to be written, and write the block back
        if start % SECTOR_SIZE != 0 {
            let start_block = self
                .db
                .get(start_sector.to_be_bytes())?
                .and_then(|v| Some(Vec::from(&*v)))
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
                .clone()
                .into_iter()
                .take((start % SECTOR_SIZE) as usize)
                .collect::<Vec<_>>();
            block.extend_from_slice(new_data);
            if single_sector_write {
                block.extend(start_block.into_iter().skip(boundary));
            }
            debug_assert!(block.len() == SECTOR_SIZE as usize);
            batch.insert(&start_sector.to_be_bytes(), block);
            start_sector += 1;
        }

        // data is now aligned to sector size
        debug_assert!(data.len() as u64 % SECTOR_SIZE == 0);

        for (idx, sector) in (start_sector..end_sector).enumerate() {
            let sector_data = &data[idx * SECTOR_SIZE as usize..(idx + 1) * SECTOR_SIZE as usize];
            batch.insert(&sector.to_be_bytes(), sector_data);
        }

        // handle end sector
        if !single_sector_write && (start + data.len() as u64) % SECTOR_SIZE != 0 {
            let last_sector_data = self
                .db
                .get(end_sector.to_be_bytes())?
                .and_then(|v| Some(Vec::from(&*v)))
                .or(Some(vec![0u8; SECTOR_SIZE as usize]))
                .unwrap();
            // boundary in the block, past here we need to extend from the read data
            let boundary = (data.len() as u64 % SECTOR_SIZE) as usize;
            let mut block = Vec::from(&data[data.len() - boundary..]);
            block.extend_from_slice(&last_sector_data[boundary..]);
            debug_assert!(block.len() as u64 == SECTOR_SIZE);
            batch.insert(&end_sector.to_be_bytes(), block);
        }

        Ok(self.db.apply_batch(batch)?)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.db.flush_async().await?;
        Ok(())
    }

    async fn trim(&mut self) -> Result<(), Self::Error> {
        Err(NbdError::NotSup)
    }

    async fn cache(&mut self) -> Result<(), Self::Error> {
        Err(NbdError::NotSup)
    }

    async fn write_zeroes(&mut self, start: u64, end: u64) -> Result<(), Self::Error> {
        Err(NbdError::NotSup)
        //self.seek(SeekFrom::Start(start)).await?;
        //let data = vec![0u8; (start - end) as usize];
        //Ok(self.write_all(&data).await?)
    }

    async fn block_status(&mut self) -> Result<(), Self::Error> {
        Err(NbdError::NotSup)
    } // TODO

    async fn resize(&mut self) -> Result<(), Self::Error> {
        Err(NbdError::NotSup)
    } // TODO
}

impl From<sled::Error> for NbdError {
    fn from(e: sled::Error) -> NbdError {
        match e {
            sled::Error::Io(_) => NbdError::Io,
            sled::Error::Unsupported(_) => NbdError::NotSup,
            _ => NbdError::Inval, // TODO
        }
    }
}
