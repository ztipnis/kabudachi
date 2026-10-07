//! The `/kabudachi/steal/1` `request_response` protocol: a length-prefixed,
//! prost-encoded `StealRequest` (a worker asking a shard peer for the tasks
//! it holds records of that look claimable) as the request, and a
//! length-prefixed, prost-encoded `StealResponse` (their ids) as the
//! response.
//!
//! A separate `request_response::Behaviour` from the others, for the same
//! reason the reconcile protocol is: the asker needs the reply to its own
//! request. Neither message has a field a reader cannot handle the absence
//! of, so the codec only decodes.

use std::io;

use kabudachi_core::protocol::messages::{StealRequest, StealResponse};
use libp2p::StreamProtocol;
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;
use prost::Message as _;

use crate::framing::{encode_length_prefixed, read_frame};

/// The sole protocol this codec negotiates.
pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/kabudachi/steal/1");

/// `request_response::Codec` for [`PROTOCOL`]. Stateless, so one instance (or
/// any number of clones) can be shared across connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct StealCodec;

impl request_response::Codec for StealCodec {
    type Protocol = StreamProtocol;
    type Request = StealRequest;
    type Response = StealResponse;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<StealRequest>
    where
        T: AsyncRead + Unpin + Send,
    {
        StealRequest::decode(&read_frame(io).await?[..]).map_err(io::Error::other)
    }

    async fn read_response<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<StealResponse>
    where
        T: AsyncRead + Unpin + Send,
    {
        StealResponse::decode(&read_frame(io).await?[..]).map_err(io::Error::other)
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
