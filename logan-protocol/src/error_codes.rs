//! Kafka protocol error codes

use anyhow::Result;
use bytes::{BufMut};
use num_derive::{FromPrimitive, ToPrimitive};

use crate::codec::{Decodable, Encodable};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, FromPrimitive, ToPrimitive)]
#[repr(i16)]
pub enum ErrorCode {
    UnknownServerError = -1,
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageTooLarge = 10,
    StaleControllerEpoch = 11,
    OffsetMetadataTooLarge = 12,
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidReplicaAssignment = 39,
    InvalidConfig = 40,
    NotController = 41,
    InvalidRequest = 42,
    UnsupportedForMessageFormat = 43,
    PolicyViolation = 44,
}

impl Default for ErrorCode {
    fn default() -> Self {
        ErrorCode::None
    }
}

impl Encodable for ErrorCode {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        (*self as i16).encode(buf)
    }
}

impl Decodable for ErrorCode {
    fn decode(buf: &mut impl bytes::Buf) -> Result<Self> {
        let code = i16::decode(buf)?;
        num_traits::FromPrimitive::from_i16(code)
            .ok_or_else(|| anyhow::anyhow!("Unknown error code: {}", code))
    }
}
