//! Primitive types used in the Kafka protocol

use std::fmt;
use std::str;

use bytes::{Buf, BufMut};

use crate::error::ServerError;
use crate::protocol::{Decodable, Encodable};

/// A Kafka string (UTF-8 encoded)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct KafkaString(pub String);

impl KafkaString {
    /// Create a new Kafka string
    #[allow(dead_code)]
    pub fn new(s: String) -> Self {
        Self(s)
    }

    /// Get the string as a slice
    #[allow(dead_code)]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the string as bytes
    #[allow(dead_code)]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Convert the Kafka string into a String
    #[allow(dead_code)]
    pub fn into_string(self) -> String {
        self.0
    }

    /// Convert the Kafka string into a String
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl From<String> for KafkaString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for KafkaString {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl Encodable for KafkaString {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        (self.0.len() as i16).encode(buf)?;
        buf.put_slice(self.0.as_bytes());
        Ok(())
    }
}

impl Decodable for KafkaString {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let len = i16::decode(buf)? as usize;
        if buf.remaining() < len {
            return Err(ServerError::DecodingError("not enough bytes for KafkaString".into()));
        }
        let str_slice = &buf.chunk()[..len];
        let s = std::str::from_utf8(str_slice)?.to_string();
        buf.advance(len);
        Ok(KafkaString(s))
    }
}

impl fmt::Display for KafkaString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A Kafka byte array
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Bytes(pub Vec<u8>);

impl Bytes {
    /// Create a new byte array
    #[allow(dead_code)]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Get the byte array as a slice
    #[allow(dead_code)]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Convert the byte array into a Vec<u8>
    #[allow(dead_code)]
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    /// Get the length of the byte array
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the byte array is empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<&[u8]> for Bytes {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

impl From<String> for Bytes {
    fn from(s: String) -> Self {
        Self(s.into_bytes())
    }
}

impl From<&str> for Bytes {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

impl Encodable for Bytes {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        (self.0.len() as i32).encode(buf)?;
        buf.put_slice(&self.0);
        Ok(())
    }
}

/// A Kafka nullable string (UTF-8 encoded or null)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct NullableString(Option<String>);

impl NullableString {
    /// Create a new non-null string
    #[allow(dead_code)]
    pub fn new(s: String) -> Self {
        Self(Some(s))
    }

    /// Create a new null string
    #[allow(dead_code)]
    pub fn null() -> Self {
        Self(None)
    }

    /// Check if the string is null
    #[allow(dead_code)]
    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }

    /// Get the string as a slice
    #[allow(dead_code)]
    pub fn as_deref(&self) -> Option<&str> {
        self.0.as_deref()
    }

    /// Convert the string into an Option<String>
    #[allow(dead_code)]
    pub fn into_inner(self) -> Option<String> {
        self.0
    }
}

impl From<String> for NullableString {
    fn from(s: String) -> Self {
        Self(Some(s))
    }
}

impl From<&str> for NullableString {
    fn from(s: &str) -> Self {
        Self(Some(s.to_string()))
    }
}

impl From<Option<String>> for NullableString {
    fn from(s: Option<String>) -> Self {
        Self(s)
    }
}

impl Encodable for NullableString {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        match &self.0 {
            Some(s) => {
                (s.len() as i16).encode(buf)?;
                buf.put_slice(s.as_bytes());
            }
            None => {
                (-1i16).encode(buf)?;
            }
        }
        Ok(())
    }
}

impl Decodable for NullableString {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let len = i16::decode(buf)?;
        if len == -1 {
            Ok(NullableString(None))
        } else {
            let len = len as usize;
            if buf.remaining() < len {
                return Err(ServerError::DecodingError("not enough bytes for NullableString".into()));
            }
            let str_slice = &buf.chunk()[..len];
            let s = std::str::from_utf8(str_slice)?.to_string();
            buf.advance(len);
            Ok(NullableString(Some(s)))
        }
    }
}

/// A Kafka array of a specific type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct KafkaArray<T>(pub Vec<T>);

impl<T> KafkaArray<T> {
    /// Create a new array
    #[allow(dead_code)]
    pub fn new(items: Vec<T>) -> Self {
        Self(items)
    }

    /// Get the array as a slice
    #[allow(dead_code)]
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    /// Convert the array into a Vec<T>
    #[allow(dead_code)]
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }

    /// Get the length of the array
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the array is empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<T> From<Vec<T>> for KafkaArray<T> {
    fn from(items: Vec<T>) -> Self {
        Self(items)
    }
}

impl<T> std::ops::Deref for KafkaArray<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for KafkaArray<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Encodable> Encodable for KafkaArray<T> {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        (self.0.len() as i32).encode(buf)?;
        for item in &self.0 {
            item.encode(buf)?;
        }
        Ok(())
    }
}

impl<T: Decodable> Decodable for KafkaArray<T> {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let len = i32::decode(buf)?;
        if len < 0 {
            return Ok(Self(vec![]));
        }
        let mut items = Vec::with_capacity(len as usize);
        for _ in 0..len {
            items.push(T::decode(buf)?);
        }
        Ok(Self(items))
    }
}

/// A Kafka boolean (1 byte, 0 for false, non-zero for true)
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
pub struct KafkaBool(pub bool);

impl KafkaBool {
    /// Create a new boolean
    #[allow(dead_code)]
    pub fn new(b: bool) -> Self {
        Self(b)
    }

    /// Get the boolean value
    #[allow(dead_code)]
    pub fn get(&self) -> bool {
        self.0
    }
}

impl From<bool> for KafkaBool {
    fn from(b: bool) -> Self {
        Self(b)
    }
}

impl From<KafkaBool> for bool {
    fn from(b: KafkaBool) -> Self {
        b.0
    }
}

impl Decodable for KafkaBool {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let val = i8::decode(buf)?;
        Ok(Self(val != 0))
    }
}

impl Encodable for KafkaBool {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        (self.0 as u8).encode(buf)
    }
}

/// A Kafka varint (variable-length integer)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VarInt(i32);

impl VarInt {
    /// Create a new varint
    #[allow(dead_code)]
    pub fn new(value: i32) -> Self {
        Self(value)
    }

    /// Get the value as i32
    #[allow(dead_code)]
    pub fn get(&self) -> i32 {
        self.0
    }
}

impl From<i32> for VarInt {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl From<VarInt> for i32 {
    fn from(value: VarInt) -> Self {
        value.0
    }
}

impl Encodable for VarInt {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.0.encode(buf)
    }
}

/// A Kafka varlong (variable-length long integer)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VarLong(i64);

impl VarLong {
    /// Create a new varlong
    #[allow(dead_code)]
    pub fn new(value: i64) -> Self {
        Self(value)
    }

    /// Get the value as i64
    #[allow(dead_code)]
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl From<i64> for VarLong {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<VarLong> for i64 {
    fn from(value: VarLong) -> Self {
        value.0
    }
}

impl Encodable for VarLong {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.0.encode(buf)
    }
}

impl Encodable for i8 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        buf.put_i8(*self);
        Ok(())
    }
}

impl Decodable for i8 {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        if buf.remaining() < 1 {
            return Err(ServerError::DecodingError("not enough bytes for i8".into()));
        }
        Ok(buf.get_i8())
    }
}

impl Encodable for i16 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        buf.put_i16(*self);
        Ok(())
    }
}

impl Decodable for i16 {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        if buf.remaining() < 2 {
            return Err(ServerError::DecodingError("not enough bytes for i16".into()));
        }
        Ok(buf.get_i16())
    }
}

impl Encodable for i32 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        buf.put_i32(*self);
        Ok(())
    }
}

impl Decodable for i32 {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        if buf.remaining() < 4 {
            return Err(ServerError::DecodingError("not enough bytes for i32".into()));
        }
        Ok(buf.get_i32())
    }
}

impl Encodable for i64 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        buf.put_i64(*self);
        Ok(())
    }
}

impl Decodable for i64 {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        if buf.remaining() < 8 {
            return Err(ServerError::DecodingError("not enough bytes for i64".into()));
        }
        Ok(buf.get_i64())
    }
}

impl Encodable for u8 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        buf.put_u8(*self);
        Ok(())
    }
}

impl Encodable for u16 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        buf.put_u16(*self);
        Ok(())
    }
}

impl Encodable for u32 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        buf.put_u32(*self);
        Ok(())
    }
}

impl Encodable for f64 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        buf.put_f64(*self);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_string() {
        let s = KafkaString::new("test".to_string());
        assert_eq!(s.as_str(), "test");
        assert_eq!(s.into_string(), "test");

        let s: KafkaString = "test".into();
        assert_eq!(s.as_str(), "test");
    }

    #[test]
    fn test_bytes() {
        let b = Bytes::new(vec![1, 2, 3]);
        assert_eq!(b.as_slice(), &[1, 2, 3]);
        assert_eq!(b.len(), 3);
        assert!(!b.is_empty());

        let b: Bytes = vec![1, 2, 3].into();
        assert_eq!(b.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_nullable_string() {
        let s = NullableString::new("test".to_string());
        assert!(!s.is_null());
        assert_eq!(s.as_deref(), Some("test"));
        assert_eq!(s.into_inner(), Some("test".to_string()));

        let s = NullableString::null();
        assert!(s.is_null());
        assert_eq!(s.as_deref(), None);
        assert_eq!(s.into_inner(), None);
    }

    #[test]
    fn test_kafka_array() {
        let a = KafkaArray::new(vec![1, 2, 3]);
        assert_eq!(a.as_slice(), &[1, 2, 3]);
        assert_eq!(a.len(), 3);
        assert!(!a.is_empty());
        assert_eq!(a.into_inner(), vec![1, 2, 3]);

        let a: KafkaArray<_> = vec![1, 2, 3].into();
        assert_eq!(a.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_kafka_bool() {
        let b = KafkaBool::new(true);
        assert!(b.get());
        assert_eq!(bool::from(b), true);

        let b: KafkaBool = false.into();
        assert!(!b.get());
    }
}
