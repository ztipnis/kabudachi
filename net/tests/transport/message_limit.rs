//! The size limit on a message a peer sends, as the election codec enforces
//! it before allocating anything for the body.

use std::io;

use kabudachi_core::scheduler::MAX_CLAIM_FRAME_BYTES;
use kabudachi_net::codec::{ElectionCodec, PROTOCOL};
use libp2p::futures::io::Cursor;
use libp2p::request_response::Codec as _;

#[tokio::test]
async fn a_request_whose_length_prefix_is_over_the_maximum_is_rejected_as_invalid() {
    let over = u32::try_from(MAX_CLAIM_FRAME_BYTES + 1).expect("the limit fits a length prefix");
    let mut oversized_prefix = Cursor::new(over.to_be_bytes().to_vec());

    let result = ElectionCodec
        .read_request(&PROTOCOL, &mut oversized_prefix)
        .await;

    // Not an end of input while reading a body: the length alone is refused.
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
}
