//! Traits for encoding and decoding Kafka protocol messages

use anyhow::Result;
use bytes::{Buf, BufMut};

/// A type that can be encoded into a buffer.
/// 
/// Implementors of this trait should provide a way to encode their values into a buffer.
pub trait Encodable {
    /// Encode a value into a buffer.
    /// 
    /// This method should write the encoded value into the provided buffer.
    fn encode(&self, buf: &mut impl BufMut) -> Result<()>;
}

/// A type that can be decoded from a buffer.
/// 
/// Implementors of this trait should provide a way to decode their values from a buffer.
pub trait Decodable: Sized {
    /// Decode a value from a buffer.
    /// 
    /// This method should read the encoded value from the provided buffer and return the decoded value.
    fn decode(buf: &mut impl Buf) -> Result<Self>;
}
