//! Traits for encoding and decoding Kafka protocol messages

use bytes::{Buf, BufMut};

use crate::error::ServerError;

/// A type that can be decoded from a buffer.
pub trait Decodable: Sized {
    /// Decode a value from a buffer.
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError>;
}

/// A type that can be encoded into a buffer.
pub trait Encodable {
    /// Encode a value into a buffer.
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError>;
}
