//! The `/kabudachi/election/2` `request_response` protocol: a
//! length-prefixed, prost-encoded `ElectionMessage` as the request, and a
//! trivial zero-byte acknowledgement as the response.
//!
//! Version 2 is the gossip roll call's schema. It gives several
//! of version 1's field numbers and payload tags, from the ring roll call,
//! new meanings, so a version 1 peer must never negotiate a stream with a
//! version 2 one: it would misread every message, or read one as another
//! kind. The new name keeps them apart.
//!
//! The response carries no information. `crate::messenger::Net::send`
//! documents delivery as unreliable and fire-and-forget — callers
//! never await a reply — so the ack exists only to let the request/response
//! substream close cleanly instead of every exchange ending in
//! `InboundFailure::ResponseOmission` on the sender's side.

use std::io;

use kabudachi_core::protocol::messages::ElectionMessage;
use libp2p::StreamProtocol;
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;

use crate::framing::{decode_election, encode_length_prefixed, read_frame};

/// The sole protocol this codec negotiates.
pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/kabudachi/election/2");

/// The response half of the protocol. It carries no data — see the module
/// doc for why a real ack payload isn't needed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Ack;

/// `request_response::Codec` for [`PROTOCOL`]. Stateless, so one instance
/// (or any number of clones) can be shared across connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct ElectionCodec;

impl request_response::Codec for ElectionCodec {
    type Protocol = StreamProtocol;
    type Request = ElectionMessage;
    type Response = Ack;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<ElectionMessage>
    where
        T: AsyncRead + Unpin + Send,
    {
        Ok(decode_election(&read_frame(io).await?)?.into_message())
    }

    async fn read_response<T>(&mut self, _: &Self::Protocol, _io: &mut T) -> io::Result<Ack>
    where
        T: AsyncRead + Unpin + Send,
    {
        // No bytes to read: the ack is empty by construction (see module doc).
        Ok(Ack)
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
        _io: &mut T,
        _res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // Nothing to write: the ack is empty by construction (see module doc).
        Ok(())
    }
}
