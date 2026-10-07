//! The `/kabudachi/join/1` `request_response` protocol (bootstrap
//! join): a length-prefixed, prost-encoded `JoinRequest` as the
//! request, and a length-prefixed, prost-encoded `JoinResponse` (the
//! responder's pointer to the shard's leader) as the response.
//!
//! A separate `request_response::Behaviour` from the election protocol's
//! (`crate::codec`) — see `net/src/swarm.rs`'s `Behaviour` — because a join
//! exchange is a real, correlated request/response (the caller needs the
//! specific reply to its own request, unlike `Net::send`'s
//! fire-and-forget, uncorrelated delivery). `JoinRequest`/`JoinResponse` are
//! themselves the codec's `Request`/`Response` types directly: unlike
//! `ElectionMessage`, there's no wrapping oneof envelope, because there's
//! only ever exactly one request shape and one response shape.

use std::io;

use kabudachi_core::protocol::messages::{JoinRequest, JoinResponse};
use libp2p::StreamProtocol;
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;

use crate::framing::{encode_length_prefixed, read_message};

/// The sole protocol this codec negotiates.
pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/kabudachi/join/1");

/// `request_response::Codec` for [`PROTOCOL`]. Stateless, so one instance
/// (or any number of clones) can be shared across connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct JoinCodec;

impl request_response::Codec for JoinCodec {
    type Protocol = StreamProtocol;
    type Request = JoinRequest;
    type Response = JoinResponse;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<JoinRequest>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_message(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<JoinResponse>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_message(io).await
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



#[cfg(test)]
mod tests {
    use kabudachi_core::protocol::ids::WorkerId;
    use libp2p::futures::io::Cursor;
    use libp2p::request_response::Codec as _;

    use super::*;

    #[tokio::test]
    async fn read_response_rejects_a_leader_pointer_missing_its_address() {
        let mut codec = JoinCodec;
        let response = JoinResponse {
            leader_id: Some(WorkerId::new("worker-a").into()),
            leader_multiaddr: String::new(),
            term: 3,
            recovery_epoch: 1,
            recovery_epoch_lineage: 0,
        };

        let mut written = Cursor::new(Vec::new());
        codec
            .write_response(&PROTOCOL, &mut written, response)
            .await
            .expect("writing a response never fails against an in-memory buffer");
        let mut to_read = Cursor::new(written.into_inner());
        let result = codec.read_response(&PROTOCOL, &mut to_read).await;

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_response_decodes_a_retired_member_list_as_no_leader_known() {
        // A JoinResponse from before JOIN became a leader pointer: field 1
        // held one member, {worker_id: {value: "w"}, multiaddr: "/m"}.
        let member = [0x0a, 0x03, 0x0a, 0x01, b'w', 0x12, 0x02, b'/', b'm'];
        let mut body = vec![0x0a, member.len() as u8];
        body.extend_from_slice(&member);
        let mut framed = (body.len() as u32).to_be_bytes().to_vec();
        framed.extend_from_slice(&body);

        let mut codec = JoinCodec;
        let decoded = codec
            .read_response(&PROTOCOL, &mut Cursor::new(framed))
            .await
            .expect("a retired field is skipped, not rejected");

        assert_eq!(decoded, JoinResponse::default());
    }
}
