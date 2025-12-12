use anyhow::{Context, Result};
use bytes::Bytes;
use std::io::{Read, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i16)]
pub enum CompressionType {
    #[default]
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl CompressionType {
    pub fn from_id(id: i16) -> Option<Self> {
        match id {
            0 => Some(Self::None),
            1 => Some(Self::Gzip),
            2 => Some(Self::Snappy),
            3 => Some(Self::Lz4),
            4 => Some(Self::Zstd),
            _ => None,
        }
    }

    pub fn compress(&self, data: &[u8]) -> Result<Bytes> {
        match self {
            Self::None => Ok(Bytes::copy_from_slice(data)),
            Self::Gzip => {
                // Not implemented yet, placeholder
                Err(anyhow::anyhow!("Gzip compression not implemented"))
            }
            Self::Snappy => {
                let mut encoder = snap::write::FrameEncoder::new(Vec::new());
                encoder
                    .write_all(data)
                    .context("Snappy compression failed")?;
                let compressed = encoder
                    .into_inner()
                    .context("Snappy compression finish failed")?;
                Ok(Bytes::from(compressed))
            }
            Self::Lz4 => {
                let compressed = lz4_flex::block::compress_prepend_size(data);
                Ok(Bytes::from(compressed))
            }
            Self::Zstd => {
                // Not implemented yet
                Err(anyhow::anyhow!("Zstd compression not implemented"))
            }
        }
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Bytes> {
        match self {
            Self::None => Ok(Bytes::copy_from_slice(data)),
            Self::Gzip => Err(anyhow::anyhow!("Gzip decompression not implemented")),
            Self::Snappy => {
                let mut decoder = snap::read::FrameDecoder::new(data);
                let mut buffer = Vec::new();
                decoder
                    .read_to_end(&mut buffer)
                    .context("Snappy decompression failed")?;
                Ok(Bytes::from(buffer))
            }
            Self::Lz4 => {
                let decompressed = lz4_flex::block::decompress_size_prepended(data)
                    .map_err(|e| anyhow::anyhow!("Lz4 decompression failed: {}", e))?;
                Ok(Bytes::from(decompressed))
            }
            Self::Zstd => Err(anyhow::anyhow!("Zstd decompression not implemented")),
        }
    }
}
