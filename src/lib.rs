#![allow(dead_code)]
#![deny(missing_debug_implementations)]

pub mod export;

use log::{debug, error, info, trace, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::prelude::*;

use std::convert::{TryFrom, TryInto};

use export::{Export, ExportStore};

const NBD_MAGIC: u64 = 0x4e42444d41474943;
const CLISERV_MAGIC: u64 = 0x00420281861253;
const IHAVEOPT: u64 = 0x49484156454F5054;
const SERVER_REPLY_MAGIC: u64 = 0x3e889045565a9;

const REQUEST_MAGIC: u32 = 0x25609513;
const SIMPLE_REPLY_MAGIC: u32 = 0x67446698;
const STRUCTURED_REPLY_MAGIC: u32 = 0x668e33ef;

fn oldstyle_handshake() {
    // write 64 bits "NBDMAGIC"
    // write 64 bits "CLISERV_MAGIC"
    // write 64 bits "export size"
    // write 32 bits flags
    // write 124 bytes zeroes
}

struct Request {
    flags: u16,
    cmd: Command,
    handle: u64,
    offset: u64,
    length: u32,
    data: Vec<u8>,
}

// Custom debug impl to properly format request bytes
impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Request {{ flags: {:016b}, cmd: {:?}, handle: {}, offset: {}, length: {}, data: {} bytes }}", self.flags, self.cmd, self.handle, self.offset, self.length, self.data.len())
    }
}

#[derive(Debug)]
pub struct HandshakeCon<T, E> {
    con: T,

    write_zeroes: bool,
    structured_replies: bool,

    export_store: E,
}

#[derive(Debug)]
pub struct ExportCon<T, E> {
    con: T,

    write_zeroes: bool,
    structured_replies: bool,

    export: E,
}

impl<T, E> HandshakeCon<T, E>
where
    T: AsyncRead + AsyncWrite + Unpin,
    E: ExportStore,
    E::Error: Into<Error>,
{
    pub fn new(con: T, export_store: E) -> Self {
        HandshakeCon {
            con,

            write_zeroes: true,
            structured_replies: false,

            export_store,
        }
    }

    async fn oldstyle_handshake(&mut self) -> Result<(), Error> {
        unimplemented!();
    }

    pub async fn newstyle_handshake(mut self) -> Result<Option<ExportCon<T, E::Export>>, Error> {
        let mut opt_data_buf = [0u8; 1024];

        trace!("Sending newstyle handshake");
        self.con.write_u64(NBD_MAGIC).await?;
        self.con.write_u64(IHAVEOPT).await?;
        self.con
            .write_u16(
                ServerHandshakeFlags::FixedNewstyle as u16 | ServerHandshakeFlags::NoZeroes as u16,
            )
            .await?;
        let cl_flags = self.con.read_u32().await?;
        if cl_flags & ClientHandShakeFlags::NoZeroes as u32 != 0 {
            self.write_zeroes = false;
        }
        // close connection on unrecognized flags
        if (cl_flags
            & !(ClientHandShakeFlags::FixedNewstyle as u32 | ClientHandShakeFlags::NoZeroes as u32))
            != 0
        {
            // unrecognized flags -> close connection
            debug!("Got unrecognized client flags, {:032b}", cl_flags);
            return Err(Error::UnrecognizedClientFlags(cl_flags));
        }
        trace!("Newsytle handshake finshed, client flags {:032b}", cl_flags);

        // at this point client starts sending options
        loop {
            let ihaveopt = self.con.read_u64().await?;
            if ihaveopt != IHAVEOPT {
                debug!(
                    "Client sent invalid header ({:x}) during option haggling",
                    ihaveopt
                );
            }
            let option = Options::try_from(self.con.read_u32().await?);
            let option_length = self.con.read_u32().await?;
            // vec with capacity and passing a slice to read exact fails, because the vec actually
            // has a len of 0
            let read = self
                .con
                .read_exact(&mut opt_data_buf[..option_length as usize])
                .await?;
            if read != option_length as usize {
                // TODO: is this needed? or only in debug?
                error!(
                    "Invalid read ({} bytes), expected {} bytes",
                    read, option_length
                );
                unreachable!();
            }
            // handle option
            match option {
                Ok(opt) => {
                    trace!(
                        "Client sent option {:?}, with length {}",
                        opt,
                        option_length
                    );
                    match &opt {
                        Options::ExportName => {
                            let name = String::from_utf8(
                                (&opt_data_buf[..option_length as usize]).to_vec(),
                            )?;
                            // TODO: such 1GB file
                            let export = self
                                .export_store
                                .get_export(&name)
                                .await
                                .map_err(|e| e.into())?;
                            self.write_opt_export_reply(1024 * 1024 * 1024).await?;
                            return Ok(Some(ExportCon::new(self, export.unwrap())));
                        }
                        Options::Abort => {
                            trace!("Dropping connection on client request");
                            // TODO: shutdown TLS, if available
                            // drop connection
                            break;
                        }
                        Options::List => {
                            if option_length != 0 {
                                self.write_opt_err_invalid(opt).await?;
                            }
                            // if data => send invalid
                            let disks = ["disk1", "disk2", "disk3"];
                            for disk in &disks {
                                self.write_opt_server_rep(opt, disk).await?;
                            }
                            self.write_opt_ack(opt).await?;
                        }
                        // TODO: implement these
                        Options::PeekExport => self.write_unsupported_option(opt).await?,
                        Options::StartTLS => self.write_unsupported_option(opt).await?,
                        Options::Info => {
                            // TODO
                            // length needs to be at least 6 bytes: 4 bytes name length, 0 length
                            // empty name, 2 bytes number of information requests
                            if option_length <= 6 {
                                // TODO: invalid
                            }
                            let (length_bytes, data) = opt_data_buf.split_at(4);
                            let name_length = u32::from_be_bytes(length_bytes.try_into()?);
                            if name_length > option_length - 6 {
                                // TODO: invalid
                            }
                            if option_length - 6 - name_length % 2 != 0 {
                                // TODO: invalid
                            }
                            let (name_bytes, data) = data.split_at(name_length as usize);
                            let name = String::from_utf8(name_bytes.to_vec())?;
                            let (request_count_bytes, data) = data.split_at(2);
                            let request_count = u16::from_be_bytes(request_count_bytes.try_into()?);
                            if data.len() != request_count as usize * 2 {
                                // TODO: invalid
                            }
                            let mut request_vec: Vec<InfoType> =
                                Vec::with_capacity(request_count as usize);
                            for idx in 0..request_count {
                                request_vec.push(
                                    u16::from_be_bytes(
                                        data[idx as usize * 2..(idx + 1) as usize * 2]
                                            .try_into()?,
                                    )
                                    .try_into()?,
                                );
                            }
                            // TODO: handle requests
                            self.write_unsupported_option(opt).await?;
                        }
                        Options::Go => {
                            //TODO
                            // length needs to be at least 6 bytes: 4 bytes name length, 0 length
                            // empty name, 2 bytes number of information requests
                            if option_length <= 6 {
                                // TODO: invalid
                            }
                            let (length_bytes, data) = opt_data_buf.split_at(4);
                            let name_length = u32::from_be_bytes(length_bytes.try_into()?);
                            if name_length > option_length - 6 {
                                // TODO: invalid
                            }
                            if option_length - 6 - name_length % 2 != 0 {
                                // TODO: invalid
                            }
                            let (name_bytes, data) = data.split_at(name_length as usize);
                            let name = String::from_utf8(name_bytes.to_vec())?;
                            let (request_count_bytes, data) = data.split_at(2);
                            let request_count = u16::from_be_bytes(request_count_bytes.try_into()?);
                            if data.len() != request_count as usize * 2 {
                                // TODO: invalid
                            }
                            let mut request_vec: Vec<InfoType> =
                                Vec::with_capacity(request_count as usize);
                            for idx in 0..request_count {
                                request_vec.push(
                                    u16::from_be_bytes(
                                        data[idx as usize * 2..(idx + 1) as usize * 2]
                                            .try_into()?,
                                    )
                                    .try_into()?,
                                );
                            }
                            for it in &request_vec {
                                trace!("{:?}", it);
                            }
                            self.write_unsupported_option(opt).await?;
                        }
                        Options::StructuredReply => self.write_unsupported_option(opt).await?,
                        Options::MetaContext => self.write_unsupported_option(opt).await?,
                        Options::SetMetaContext => self.write_unsupported_option(opt).await?,
                    };
                }
                Err(e) => {
                    trace!("Client sent unknown option {}", e);
                    // drop option bytes
                    continue;
                }
            };
        }

        Ok(None)
    }

    async fn write_opt_export_reply(&mut self, size: u64) -> Result<(), Error> {
        // TODO proper flaggies
        let mut flags = ServerTransmissionFlags::HasFlags as u16;
        flags |= ServerTransmissionFlags::WriteZeroes as u16;
        flags |= ServerTransmissionFlags::SendFlush as u16;
        self.con.write_u64(size).await?;
        self.con.write_u16(flags).await?;

        if self.write_zeroes {
            self.con.write_all(&[0u8; 124]).await?;
        }

        Ok(())
    }

    async fn write_opt_ack(&mut self, opt: Options) -> Result<(), Error> {
        Ok(self
            .write_opt_reply_header(opt, OptionsReply::Ack, 0)
            .await?)
    }

    async fn write_opt_server_rep(&mut self, opt: Options, name: &str) -> Result<(), Error> {
        // 4 byte length field + actual name
        self.write_opt_reply_header(opt, OptionsReply::Server, 4 + name.len() as u32)
            .await?;
        // Data
        // 32 bits length of name
        self.con.write_u32(name.len() as u32).await?;
        // name
        self.con.write_all(name.as_bytes()).await?;
        Ok(())
    }

    async fn write_opt_err_invalid(&mut self, opt: Options) -> Result<(), Error> {
        Ok(self
            .write_opt_reply_header(opt, OptionsReply::ErrInvalid, 0)
            .await?)
    }

    async fn write_unsupported_option(&mut self, opt: Options) -> Result<(), Error> {
        trace!("writing \"unsupported option\" to client");
        Ok(self
            .write_opt_reply_header(opt, OptionsReply::ErrUnsup, 0)
            .await?)
    }

    async fn write_opt_reply_header(
        &mut self,
        opt: Options,
        optreply: OptionsReply,
        len: u32,
    ) -> Result<(), Error> {
        self.con.write_u64(SERVER_REPLY_MAGIC).await?;
        self.con.write_u32(opt as u32).await?;
        self.con.write_u32(optreply as u32).await?;
        self.con.write_u32(len).await?;
        Ok(())
    }
}

impl<T, E> ExportCon<T, E>
where
    T: AsyncRead + AsyncWrite + Unpin,
    E: Export,
{
    // We don't need to set the bound of S::Export: E, since we don't care about S
    pub fn new<S>(hs: HandshakeCon<T, S>, export: E) -> ExportCon<T, E> {
        ExportCon {
            con: hs.con,

            write_zeroes: hs.write_zeroes,
            structured_replies: hs.structured_replies,

            export,
        }
    }

    pub async fn serve_export(&mut self) -> Result<(), Error> {
        info!("Serving export to client");

        // TODO
        let export_len = 1024 * 1024 * 1024;

        loop {
            let req = self.read_request().await?;
            trace!("Client sent {:?}", req);
            // TODO
            match req.cmd {
                Command::Read => {
                    // bounds check
                    if req.offset + req.length as u64 > export_len {
                        debug!("Sendig ENOSPC to client");
                        self.write_simple_reply(NbdError::NoSpc, req.handle, None)
                            .await?;
                        continue;
                    }

                    let result = self
                        .export
                        .read(req.offset, req.offset + req.length as u64)
                        .await;

                    match result {
                        Err(nbde) => {
                            self.write_simple_reply(nbde.into(), req.handle, None)
                                .await?
                        }
                        Ok(contents) => {
                            self.write_simple_reply(NbdError::None, req.handle, Some(contents))
                                .await?
                        }
                    };
                }
                Command::Write => {
                    // bounds check
                    if req.offset + req.length as u64 > export_len {
                        debug!("Sendig ENOSPC to client");
                        self.write_simple_reply(NbdError::NoSpc, req.handle, None)
                            .await?;
                        continue;
                    }

                    let err = self
                        .export
                        .write(req.offset, &req.data)
                        .await
                        .map_err(|e| e.into())
                        .and::<()>(Err(NbdError::None))
                        .unwrap_err();

                    self.write_simple_reply(err, req.handle, None).await?;
                }
                Command::Disc => {
                    info!("Disconnecting client");
                    return Ok(());
                }
                Command::Flush => {
                    if req.offset != 0 || req.length != 0 {
                        self.write_simple_reply(NbdError::Inval, req.handle, None)
                            .await?;
                        continue;
                    }

                    let err = self
                        .export
                        .flush()
                        .await
                        .map_err(|e| e.into())
                        .and::<()>(Err(NbdError::None))
                        .unwrap_err();

                    self.write_simple_reply(err, req.handle, None).await?;
                }
                Command::Trim => {
                    // TODO
                    warn!("Writing operation not supported to client");
                    self.write_simple_reply(NbdError::NotSup, req.handle, None)
                        .await?;
                }
                Command::Cache => {
                    //TODO
                    warn!("Writing operation not supported to client");
                    self.write_simple_reply(NbdError::NotSup, req.handle, None)
                        .await?;
                }
                // TODO
                Command::WriteZeroes => {
                    if req.offset != 0 || req.length != 0 {
                        self.write_simple_reply(NbdError::Inval, req.handle, None)
                            .await?;
                        continue;
                    }

                    let err = self
                        .export
                        .write_zeroes(req.offset, req.offset + req.length as u64)
                        .await
                        .map_err(|e| e.into())
                        .and::<()>(Err(NbdError::None))
                        .unwrap_err();

                    self.write_simple_reply(err, req.handle, None).await?;
                }
                Command::BlockStatus => {
                    // TODO
                    warn!("Writing operation not supported to client");
                    self.write_simple_reply(NbdError::NotSup, req.handle, None)
                        .await?;
                }
                Command::Resize => {
                    // TODO
                    warn!("Writing operation not supported to client");
                    self.write_simple_reply(NbdError::NotSup, req.handle, None)
                        .await?;
                }
            };
        }
    }

    async fn write_structured_reply_chunk(
        &mut self,
        flags: u16,
        typ: StructuredReplyTypes,
        handle: u64,
        payload: &[u8],
    ) -> Result<(), Error> {
        self.con.write_u32(STRUCTURED_REPLY_MAGIC).await?;
        self.con.write_u16(flags).await?;
        self.con.write_u16(typ as u16).await?;
        self.con.write_u64(handle).await?;
        self.con.write_u32(payload.len() as u32).await?;
        if payload.is_empty() {
            self.con.write_all(payload).await?;
        }
        Ok(())
    }

    async fn write_simple_reply(
        &mut self,
        error: NbdError,
        handle: u64,
        data: Option<Vec<u8>>,
    ) -> Result<(), Error> {
        self.con.write_u32(SIMPLE_REPLY_MAGIC).await?;
        self.con.write_i32(error as i32).await?;
        self.con.write_u64(handle).await?;
        if let Some(data) = data {
            self.con.write_all(&data[..]).await?;
        };
        Ok(())
    }

    async fn read_request(&mut self) -> Result<Request, Error> {
        let magic = self.con.read_u32().await?;
        if magic != REQUEST_MAGIC {
            return Err(Error::InvalidRequestMagic(magic));
        }
        let flags = self.con.read_u16().await?;
        let cmd = Command::try_from(self.con.read_u16().await?)?;
        let handle = self.con.read_u64().await?;
        let offset = self.con.read_u64().await?;
        let length = self.con.read_u32().await?;
        let data = if cmd == Command::Write {
            let mut buf = vec![0; length as usize];
            let read = self.con.read_exact(&mut buf[..]).await?;
            if read != length as usize {
                error!(
                    "Mismatched read data length ({}) vs received ({})",
                    read, length
                );
            }
            buf
        } else {
            Vec::with_capacity(0)
        };

        Ok(Request {
            flags,
            cmd,
            handle,
            offset,
            length,
            data,
        })
    }
}

#[derive(Debug)]
enum ClientState {
    Handshake,
    Options,
    Transmission,
}

#[repr(u16)]
#[derive(Debug)]
enum ServerHandshakeFlags {
    FixedNewstyle = 1,
    NoZeroes = 1 << 1,
}

#[repr(u32)]
#[derive(Debug)]
enum ClientHandShakeFlags {
    FixedNewstyle = 1,
    NoZeroes = 1 << 1,
}

#[repr(u16)]
#[derive(Debug)]
enum ServerTransmissionFlags {
    HasFlags = 1,
    ReadOnly = 1 << 1,
    SendFlush = 1 << 2,
    SendFua = 1 << 3,
    Rotational = 1 << 4,
    SendTrim = 1 << 5,
    WriteZeroes = 1 << 6,
    SendDF = 1 << 7,
    CanMultiCon = 1 << 8,
    SendResize = 1 << 9,
    SendCache = 1 << 10,
    SendFastZero = 1 << 11,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq)]
enum Options {
    ExportName = 1,
    Abort = 2,
    List = 3,
    PeekExport = 4,
    StartTLS = 5,
    Info = 6,
    Go = 7,
    StructuredReply = 8,
    MetaContext = 9,
    SetMetaContext = 10,
}

impl TryFrom<u32> for Options {
    type Error = Error;

    fn try_from(u: u32) -> Result<Self, Self::Error> {
        match u {
            1 => Ok(Options::ExportName),
            2 => Ok(Options::Abort),
            3 => Ok(Options::List),
            4 => Ok(Options::PeekExport),
            5 => Ok(Options::StartTLS),
            6 => Ok(Options::Info),
            7 => Ok(Options::Go),
            8 => Ok(Options::StructuredReply),
            9 => Ok(Options::MetaContext),
            10 => Ok(Options::SetMetaContext),
            _ => Err(Error::UnknownOption(u)),
        }
    }
}

// TODO: is this options reply or general reply
#[repr(u32)]
#[derive(Debug)]
enum OptionsReply {
    Ack = 1,
    Server = 2,
    Info = 3,
    MetaContext = 4,
    ErrUnsup = 1 << 31 | 1,
    ErrPolicy = 1 << 31 | 2,
    ErrInvalid = 1 << 31 | 3,
    ErrPlatform = 1 << 31 | 4,
    ErrTlsReqd = 1 << 31 | 5,
    ErrUnknown = 1 << 31 | 6,
    ErrShutDown = 1 << 31 | 7,
    ErrBlockSizeReqd = 1 << 31 | 8,
    ErrTooBig = 1 << 31 | 9,
}

#[repr(u16)]
#[derive(Debug, PartialEq)]
enum InfoType {
    Export = 0,
    Name = 1,
    Description = 2,
    BlockSize = 3,
    MetaContext = 4,
}

impl TryFrom<u16> for InfoType {
    type Error = Error;

    fn try_from(u: u16) -> Result<Self, Self::Error> {
        match u {
            0 => Ok(InfoType::Export),
            1 => Ok(InfoType::Name),
            2 => Ok(InfoType::Description),
            3 => Ok(InfoType::BlockSize),
            4 => Ok(InfoType::MetaContext),
            _ => Err(Error::UnknownInfoType(u)),
        }
    }
}

// #[repr(u32)]
// #[derive(Debug)]
// enum ErrorReply {
//     Unsup = 1 << 31 | 1,
//     Policy = 1 << 31 | 2,
//     Invalid = 1 << 31 | 3,
//     Platform = 1 << 31 | 4,
//     TlsReqd = 1 << 31 | 5,
//     Unknown = 1 << 31 | 6,
//     ShutDown = 1 << 31 | 7,
//     BlockSizeReqd = 1 << 31 | 8,
//     TooBig = 1 << 31 | 9,
// }

#[repr(u16)]
#[derive(Debug)]
enum CommandFlags {
    Fua = 1,
    NoHole = 1 << 1,
    DF = 1 << 2,
    ReqOne = 1 << 3,
    FastZero = 1 << 4,
}

#[repr(u16)]
#[derive(Debug)]
enum StructuredReplyFlags {
    Done = 1,
}

// TODO: right size??
#[repr(u32)]
#[derive(Debug)]
enum StructuredReplyTypes {
    None = 0,
    OffsetData = 1,
    OffsetHole = 2,
    BlockStatus = 5,
    Error = 1 << 31 | 1,
    ErrorOffset = 1 << 31 | 2,
}

// #[repr(u16)]
// #[derive(Debug)]
// enum StructuredReplyErrors {
//     Error = 1,
//     ErrorOffset = 2,
// }

#[repr(u16)]
#[derive(Debug, PartialEq)]
enum Command {
    Read = 0,
    Write = 1,
    Disc = 2,
    Flush = 3,
    Trim = 4,
    Cache = 5,
    WriteZeroes = 6,
    BlockStatus = 7,
    Resize = 8,
}

impl TryFrom<u16> for Command {
    type Error = Error;

    fn try_from(u: u16) -> Result<Self, Self::Error> {
        match u {
            0 => Ok(Command::Read),
            1 => Ok(Command::Write),
            2 => Ok(Command::Disc),
            3 => Ok(Command::Flush),
            4 => Ok(Command::Trim),
            5 => Ok(Command::Cache),
            6 => Ok(Command::WriteZeroes),
            7 => Ok(Command::BlockStatus),
            8 => Ok(Command::Resize),
            _ => Err(Error::UnknownCommand(u)),
        }
    }
}

#[repr(i32)]
#[derive(Debug)]
pub enum NbdError {
    /// No error encountered during operation.
    None = 0,
    /// Operation not permitted.
    Perm = 1,
    /// Input/output error.
    Io = 5,
    /// Cannot allocate memory.
    Nomem = 12,
    /// Invalid argument.
    Inval = 22,
    /// No space left on device.
    NoSpc = 28,
    /// Value too large.
    Overflow = 75,
    /// Operation not supported.
    NotSup = 95,
    /// Server is in the process of being shut down.
    ShutDown = 108,
}

impl From<std::io::Error> for NbdError {
    fn from(e: std::io::Error) -> NbdError {
        NbdError::Io
    }
}

#[derive(Debug)]
pub enum Error {
    Unknown,
    /// An unknown option.
    UnknownOption(u32),
    /// An unknown command.
    UnknownCommand(u16),
    /// An unknown info type
    UnknownInfoType(u16),
    UnrecognizedClientFlags(u32),
    UnsupportedExport(String),
    /// IO error while writing to the client / server connection.
    IO(std::io::ErrorKind),
    HandshakeFinished,
    InvalidRequestMagic(u32),
    ConversionFailed,
    NonUtf8Name,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Unknown => write!(f, "Unknown error"),
            Error::UnknownOption(opt) => write!(f, "Unknown option {}", opt),
            Error::UnknownCommand(cmd) => write!(f, "Unknown command {}", cmd),
            Error::UnknownInfoType(it) => write!(f, "Unknown info type {}", it),
            Error::UnrecognizedClientFlags(flags) => {
                write!(f, "Unknown client flags {:032b}", flags)
            }
            Error::UnsupportedExport(name) => write!(f, "Export with name {} not supported", name),
            Error::IO(kind) => write!(f, "IO error on client <-> server connection: {:?}", kind),
            Error::HandshakeFinished => write!(
                f,
                "Try to perform handshake action, but handshake already finsihed"
            ),
            Error::InvalidRequestMagic(magic) => write!(f, "Got invalid request magic {}", magic),
            Error::ConversionFailed => write!(f, "Failed to convert byte slice"),
            Error::NonUtf8Name => write!(f, "Export name is not valid utf-8"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::IO(e.kind())
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(_: std::array::TryFromSliceError) -> Error {
        Error::ConversionFailed
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(_: std::string::FromUtf8Error) -> Error {
        Error::NonUtf8Name
    }
}

#[cfg(test)]
mod tests {
    use super::Command;
    use super::InfoType;
    use super::Options;
    use std::convert::TryFrom;

    #[test]
    fn option_from_u32() {
        assert_eq!(Options::try_from(1).unwrap(), Options::ExportName);
        assert_eq!(Options::try_from(2).unwrap(), Options::Abort);
        assert_eq!(Options::try_from(3).unwrap(), Options::List);
        assert_eq!(Options::try_from(4).unwrap(), Options::PeekExport);
        assert_eq!(Options::try_from(5).unwrap(), Options::StartTLS);
        assert_eq!(Options::try_from(6).unwrap(), Options::Info);
        assert_eq!(Options::try_from(7).unwrap(), Options::Go);
        assert_eq!(Options::try_from(8).unwrap(), Options::StructuredReply);
        assert_eq!(Options::try_from(9).unwrap(), Options::MetaContext);
        assert_eq!(Options::try_from(10).unwrap(), Options::SetMetaContext);
        assert!(Options::try_from(11).is_err());
        assert!(Options::try_from(1134).is_err());
        assert!(Options::try_from(45098).is_err());
        assert!(Options::try_from(24_357_099).is_err());
    }

    #[test]
    fn command_from_u16() {
        assert_eq!(Command::try_from(0).unwrap(), Command::Read);
        assert_eq!(Command::try_from(1).unwrap(), Command::Write);
        assert_eq!(Command::try_from(2).unwrap(), Command::Disc);
        assert_eq!(Command::try_from(3).unwrap(), Command::Flush);
        assert_eq!(Command::try_from(4).unwrap(), Command::Trim);
        assert_eq!(Command::try_from(5).unwrap(), Command::Cache);
        assert_eq!(Command::try_from(6).unwrap(), Command::WriteZeroes);
        assert_eq!(Command::try_from(7).unwrap(), Command::BlockStatus);
        assert_eq!(Command::try_from(8).unwrap(), Command::Resize);
        assert!(Command::try_from(9).is_err());
        assert!(Command::try_from(15).is_err());
        assert!(Command::try_from(177).is_err());
    }

    #[test]
    fn info_type_from_u16() {
        assert_eq!(InfoType::try_from(0).unwrap(), InfoType::Export);
        assert_eq!(InfoType::try_from(1).unwrap(), InfoType::Name);
        assert_eq!(InfoType::try_from(2).unwrap(), InfoType::Description);
        assert_eq!(InfoType::try_from(3).unwrap(), InfoType::BlockSize);
        assert_eq!(InfoType::try_from(4).unwrap(), InfoType::MetaContext);
        assert!(InfoType::try_from(9).is_err());
        assert!(InfoType::try_from(15).is_err());
        assert!(InfoType::try_from(177).is_err());
    }
}
