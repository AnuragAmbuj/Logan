//! Primitive types used in the Kafka protocol

use std::str;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};

use crate::codec::{Decodable, Encodable};

// --- KafkaString ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct KafkaString(pub String);

impl Encodable for KafkaString {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.0.len() as i16).encode(buf)?;
        buf.put_slice(self.0.as_bytes());
        Ok(())
    }
}

impl Decodable for KafkaString {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let len = i16::decode(buf)?;
        if len < 0 {
            return Ok(KafkaString(String::new()));
        }
        let len = len as usize;
        let mut bytes = vec![0; len];
        buf.copy_to_slice(&mut bytes);
        let s = str::from_utf8(&bytes)?.to_string();
        Ok(KafkaString(s))
    }
}

impl From<&str> for KafkaString {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl KafkaString {
    pub fn into_inner(self) -> String {
        self.0
    }
}

// --- KafkaArray ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct KafkaArray<T>(pub Vec<T>);

impl<T: Encodable> Encodable for KafkaArray<T> {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.0.len() as i32).encode(buf)?;
        for item in &self.0 {
            item.encode(buf)?;
        }
        Ok(())
    }
}

impl<T: Decodable> Decodable for KafkaArray<T> {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let len = i32::decode(buf)?;
        if len < 0 {
            return Ok(KafkaArray(vec![]));
        }
        let len = len as usize;
        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(T::decode(buf)?);
        }
        Ok(KafkaArray(items))
    }
}

// --- KafkaBool ---

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
pub struct KafkaBool(pub bool);

impl Encodable for KafkaBool {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.0 as i8).encode(buf)
    }
}

impl Decodable for KafkaBool {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        Ok(KafkaBool(i8::decode(buf)? != 0))
    }
}

impl From<bool> for KafkaBool {
    fn from(b: bool) -> Self {
        Self(b)
    }
}

// --- KafkaBytes ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct KafkaBytes(pub Bytes);

impl Encodable for KafkaBytes {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.0.len() as i32).encode(buf)?;
        buf.put_slice(&self.0);
        Ok(())
    }
}

impl Decodable for KafkaBytes {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let len = i32::decode(buf)?;
        if len < 0 {
            return Ok(KafkaBytes(Bytes::new()));
        }
        let len = len as usize;
        let bytes = buf.copy_to_bytes(len);
        Ok(KafkaBytes(bytes))
    }
}

// --- NullableString ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct NullableString(pub Option<String>);

impl Encodable for NullableString {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.0 {
            Some(s) => {
                (s.len() as i16).encode(buf)?;
                buf.put_slice(s.as_bytes());
            }
            None => (-1i16).encode(buf)?,
        }
        Ok(())
    }
}

impl Decodable for NullableString {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let len = i16::decode(buf)?;
        if len < 0 {
            return Ok(NullableString(None));
        }
        let len = len as usize;
        let mut bytes = vec![0; len];
        buf.copy_to_slice(&mut bytes);
        let s = str::from_utf8(&bytes)?.to_string();
        Ok(NullableString(Some(s)))
    }
}

impl From<Option<String>> for NullableString {
    fn from(s: Option<String>) -> Self {
        Self(s)
    }
}

// --- NullableBytes ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct NullableBytes(pub Option<Bytes>);

impl Encodable for NullableBytes {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.0 {
            Some(bytes) => {
                (bytes.len() as i32).encode(buf)?;
                buf.put_slice(bytes);
            }
            None => {
                (-1i32).encode(buf)?;
            }
        }
        Ok(())
    }
}

impl Decodable for NullableBytes {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let len = i32::decode(buf)?;
        if len < 0 {
            return Ok(Self(None));
        }
        let len = len as usize;
        if buf.remaining() < len {
            return Err(anyhow::anyhow!("Not enough bytes to decode NullableBytes"));
        }
        Ok(Self(Some(buf.copy_to_bytes(len))))
    }
}

// --- Implementations for primitive integer types ---

macro_rules! impl_codec_for_int {
    ($($t:ty),*) => {
        $(
            impl Encodable for $t {
                fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
                    buf.put(self.to_be_bytes().as_ref());
                    Ok(())
                }
            }

            impl Decodable for $t {
                fn decode(buf: &mut impl Buf) -> Result<Self> {
                    if buf.remaining() < std::mem::size_of::<Self>() {
                        return Err(anyhow::anyhow!("not enough bytes for integer"));
                    }
                    let val = <$t>::from_be_bytes(buf.chunk()[..std::mem::size_of::<Self>()].try_into()?);
                    buf.advance(std::mem::size_of::<Self>());
                    Ok(val)
                }
            }
        )*
    };
}

impl_codec_for_int!(i8, i16, i32, i64, u8, u16, u32, u64);

// --- Varint / Varlong Utils ---

pub fn encode_varint(n: i32, buf: &mut impl BufMut) {
    let mut n = n as u32; // treat as unsigned bits
    loop {
        let mut b = (n as u8) & 0x7f;
        n >>= 7;
        if n == 0 {
            buf.put_u8(b);
            break;
        } else {
            b |= 0x80;
            buf.put_u8(b);
        }
    }
}

pub fn decode_varint(buf: &mut impl Buf) -> Result<i32> {
    let mut n: u32 = 0;
    let mut shift = 0;
    loop {
        if !buf.has_remaining() {
            return Err(anyhow::anyhow!("Unexpected EOF reading varint"));
        }
        let b = buf.get_u8();
        n |= ((b & 0x7f) as u32) << shift;
        if (b & 0x80) == 0 {
            return Ok(n as i32);
        }
        shift += 7;
        if shift > 32 {
            return Err(anyhow::anyhow!("Varint too long"));
        }
    }
}

// --- CompactString ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct CompactString(pub String);

impl Encodable for CompactString {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let len = self.0.len() as i32 + 1;
        encode_varint(len, buf);
        buf.put_slice(self.0.as_bytes());
        Ok(())
    }
}

impl Decodable for CompactString {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let len = decode_varint(buf)?;
        if len == 0 {
            return Ok(CompactString("".to_string()));
        }
        let len = (len - 1) as usize;
        let mut bytes = vec![0; len];
        buf.copy_to_slice(&mut bytes);
        let s = str::from_utf8(&bytes)?.to_string();
        Ok(CompactString(s))
    }
}

// --- CompactArray ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct CompactArray<T>(pub Vec<T>);

impl<T: Encodable> Encodable for CompactArray<T> {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let len = self.0.len() as i32 + 1;
        encode_varint(len, buf);
        for item in &self.0 {
            item.encode(buf)?;
        }
        Ok(())
    }
}

impl<T: Decodable> Decodable for CompactArray<T> {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let len = decode_varint(buf)?;
        if len == 0 {
            return Ok(CompactArray(vec![]));
        }
        let len = (len - 1) as usize;
        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(T::decode(buf)?);
        }
        Ok(CompactArray(items))
    }
}

// --- TaggedFields ---

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct TaggedFields;

impl Encodable for TaggedFields {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_varint(0, buf);
        Ok(())
    }
}

impl Decodable for TaggedFields {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let num_tags = decode_varint(buf)?;
        for _ in 0..num_tags {
            let _tag = decode_varint(buf)?;
            let len = decode_varint(buf)?;
            if buf.remaining() < len as usize {
                return Err(anyhow::anyhow!("Not enough bytes for tagged field"));
            }
            buf.advance(len as usize);
        }
        Ok(TaggedFields)
    }
}
