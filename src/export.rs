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
