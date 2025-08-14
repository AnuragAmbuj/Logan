use anyhow::Result;
use num_derive::{FromPrimitive, ToPrimitive};

use crate::codec::{Decodable, Encodable};
use std::hash::Hash;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, FromPrimitive, ToPrimitive, Default)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    #[default]
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
}

impl Encodable for ApiKey {
    fn encode(&self, buf: &mut impl bytes::BufMut) -> Result<()> {
        (*self as i16).encode(buf)
    }
}

impl Decodable for ApiKey {
    fn decode(buf: &mut impl bytes::Buf) -> Result<Self> {
        let key = i16::decode(buf)?;
        num_traits::FromPrimitive::from_i16(key).ok_or_else(|| {
            anyhow::anyhow!("Unknown API key: {}", key)
        })
    }
}
