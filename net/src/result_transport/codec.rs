//! The `/kabudachi/result/1` `request_response` protocol: a length-prefixed,
//! prost-encoded `ResultMessage` both ways: a delivery, certification or
//! conclusion as the request, its acknowledgement as the response.
//!
//! A separate behaviour, because a sender needs the answer to its own
//! message.

use std::io;

use kabudachi_core::protocol::messages::ResultMessage;
use libp2p::StreamProtocol;
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;
use prost::Message as _;

use crate::framing::{encode_length_prefixed, read_frame};

/// The sole protocol this codec negotiates.
pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/kabudachi/result/1");

/// `request_response::Codec` for [`PROTOCOL`]. Stateless, so one instance (or
/// any number of clones) can be shared across connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct ResultCodec;

impl request_response::Codec for ResultCodec {
    type Protocol = StreamProtocol;
    type Request = ResultMessage;
    type Response = ResultMessage;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<ResultMessage>
    where
        T: AsyncRead + Unpin + Send,
    {
        ResultMessage::decode(&read_frame(io).await?[..]).map_err(io::Error::other)
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<ResultMessage>
    where
        T: AsyncRead + Unpin + Send,
    {
        ResultMessage::decode(&read_frame(io).await?[..]).map_err(io::Error::other)
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
