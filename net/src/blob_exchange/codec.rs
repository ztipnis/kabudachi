//! The `/kabudachi/blob/1` `request_response` protocol: a length-prefixed,
//! prost-encoded `Digest` (a worker asking a named peer for the blob it
//! names) as the request, and a length-prefixed, prost-encoded `BlobRecord`
//! as the response: the blob, or an empty record from a peer that holds none.
//! The frame limit is the message limit, far above one blob.
//!
//! A separate `request_response::Behaviour` from the others, for the same
//! reason the steal protocol is: the asker needs the reply to its own
//! request. Neither message has a field a reader cannot handle the absence
//! of, so the codec only decodes.

use std::io;

use kabudachi_core::protocol::generated::{BlobRecord, Digest};
use libp2p::StreamProtocol;
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;
use prost::Message as _;

use crate::framing::{encode_length_prefixed, read_frame};

/// The sole protocol this codec negotiates.
pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/kabudachi/blob/1");

/// `request_response::Codec` for [`PROTOCOL`]. Stateless, so one instance (or
/// any number of clones) can be shared across connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlobCodec;

impl request_response::Codec for BlobCodec {
    type Protocol = StreamProtocol;
    type Request = Digest;
    type Response = BlobRecord;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Digest>
    where
        T: AsyncRead + Unpin + Send,
    {
        Digest::decode(&read_frame(io).await?[..]).map_err(io::Error::other)
    }

    async fn read_response<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<BlobRecord>
    where
        T: AsyncRead + Unpin + Send,
    {
        BlobRecord::decode(&read_frame(io).await?[..]).map_err(io::Error::other)
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(&encode_length_prefixed(&req)).await
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(&encode_length_prefixed(&res)).await
    }
}
