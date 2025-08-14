//! Kafka protocol API keys

use std::fmt;
use std::convert::TryFrom;

use bytes::Buf;

use crate::error::ServerError;
use crate::protocol::{Decodable, Encodable};

/// Kafka API key
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApiKey {
    /// Produce: 0
    Produce = 0,
    /// Fetch: 1
    Fetch = 1,
    /// ListOffsets: 2
    ListOffsets = 2,
    /// Metadata: 3
    Metadata = 3,
    /// LeaderAndIsr: 4
    LeaderAndIsr = 4,
    /// StopReplica: 5
    StopReplica = 5,
    /// UpdateMetadata: 6
    UpdateMetadata = 6,
    /// ControlledShutdown: 7
    ControlledShutdown = 7,
    /// OffsetCommit: 8
    OffsetCommit = 8,
    /// OffsetFetch: 9
    OffsetFetch = 9,
    /// FindCoordinator: 10
    FindCoordinator = 10,
    /// JoinGroup: 11
    JoinGroup = 11,
    /// Heartbeat: 12
    Heartbeat = 12,
    /// LeaveGroup: 13
    LeaveGroup = 13,
    /// SyncGroup: 14
    SyncGroup = 14,
    /// DescribeGroups: 15
    DescribeGroups = 15,
    /// ListGroups: 16
    ListGroups = 16,
    /// SaslHandshake: 17
    SaslHandshake = 17,
    /// ApiVersions: 18
    ApiVersions = 18,
    /// CreateTopics: 19
    CreateTopics = 19,
    /// DeleteTopics: 20
    DeleteTopics = 20,
    /// DeleteRecords: 21
    DeleteRecords = 21,
    /// InitProducerId: 22
    InitProducerId = 22,
    /// OffsetForLeaderEpoch: 23
    OffsetForLeaderEpoch = 23,
    /// AddPartitionsToTxn: 24
    AddPartitionsToTxn = 24,
    /// AddOffsetsToTxn: 25
    AddOffsetsToTxn = 25,
    /// EndTxn: 26
    EndTxn = 26,
    /// WriteTxnMarkers: 27
    WriteTxnMarkers = 27,
    /// TxnOffsetCommit: 28
    TxnOffsetCommit = 28,
    /// DescribeAcls: 29
    DescribeAcls = 29,
    /// CreateAcls: 30
    CreateAcls = 30,
    /// DeleteAcls: 31
    DeleteAcls = 31,
    /// DescribeConfigs: 32
    DescribeConfigs = 32,
    /// AlterConfigs: 33
    AlterConfigs = 33,
    /// AlterReplicaLogDirs: 34
    AlterReplicaLogDirs = 34,
    /// DescribeLogDirs: 35
    DescribeLogDirs = 35,
    /// SaslAuthenticate: 36
    SaslAuthenticate = 36,
    /// CreatePartitions: 37
    CreatePartitions = 37,
    /// CreateDelegationToken: 38
    CreateDelegationToken = 38,
    /// RenewDelegationToken: 39
    RenewDelegationToken = 39,
    /// ExpireDelegationToken: 40
    ExpireDelegationToken = 40,
    /// DescribeDelegationToken: 41
    DescribeDelegationToken = 41,
    /// DeleteGroups: 42
    DeleteGroups = 42,
    /// ElectLeaders: 43
    ElectLeaders = 43,
    /// IncrementalAlterConfigs: 44
    IncrementalAlterConfigs = 44,
    /// AlterPartitionReassignments: 45
    AlterPartitionReassignments = 45,
    /// ListPartitionReassignments: 46
    ListPartitionReassignments = 46,
    /// OffsetDelete: 47
    OffsetDelete = 47,
    /// DescribeClientQuotas: 48
    DescribeClientQuotas = 48,
    /// AlterClientQuotas: 49
    AlterClientQuotas = 49,
    /// DescribeUserScramCredentials: 50
    DescribeUserScramCredentials = 50,
    /// AlterUserScramCredentials: 51
    AlterUserScramCredentials = 51,
    /// Vote: 52
    Vote = 52,
    /// BeginQuorumEpoch: 53
    BeginQuorumEpoch = 53,
    /// EndQuorumEpoch: 54
    EndQuorumEpoch = 54,
    /// DescribeQuorum: 55
    DescribeQuorum = 55,
    /// AlterPartition: 56
    AlterPartition = 56,
    /// UpdateFeatures: 57
    UpdateFeatures = 57,
    /// Envelope: 58
    Envelope = 58,
    /// FetchSnapshot: 59
    FetchSnapshot = 59,
    /// DescribeCluster: 60
    DescribeCluster = 60,
    /// DescribeProducers: 61
    DescribeProducers = 61,
    /// DescribeTransactions: 62
    DescribeTransactions = 62,
    /// ListTransactions: 63
    ListTransactions = 63,
    /// AllocateProducerIds: 64
    AllocateProducerIds = 64,
}

impl Default for ApiKey {
    fn default() -> Self {
        ApiKey::Produce
    }
}

impl TryFrom<i16> for ApiKey {
    type Error = ServerError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ApiKey::Produce),
            1 => Ok(ApiKey::Fetch),
            2 => Ok(ApiKey::ListOffsets),
            3 => Ok(ApiKey::Metadata),
            4 => Ok(ApiKey::LeaderAndIsr),
            5 => Ok(ApiKey::StopReplica),
            6 => Ok(ApiKey::UpdateMetadata),
            7 => Ok(ApiKey::ControlledShutdown),
            8 => Ok(ApiKey::OffsetCommit),
            9 => Ok(ApiKey::OffsetFetch),
            10 => Ok(ApiKey::FindCoordinator),
            11 => Ok(ApiKey::JoinGroup),
            12 => Ok(ApiKey::Heartbeat),
            13 => Ok(ApiKey::LeaveGroup),
            14 => Ok(ApiKey::SyncGroup),
            15 => Ok(ApiKey::DescribeGroups),
            16 => Ok(ApiKey::ListGroups),
            17 => Ok(ApiKey::SaslHandshake),
            18 => Ok(ApiKey::ApiVersions),
            19 => Ok(ApiKey::CreateTopics),
            20 => Ok(ApiKey::DeleteTopics),
            21 => Ok(ApiKey::DeleteRecords),
            22 => Ok(ApiKey::InitProducerId),
            23 => Ok(ApiKey::OffsetForLeaderEpoch),
            24 => Ok(ApiKey::AddPartitionsToTxn),
            25 => Ok(ApiKey::AddOffsetsToTxn),
            26 => Ok(ApiKey::EndTxn),
            27 => Ok(ApiKey::WriteTxnMarkers),
            28 => Ok(ApiKey::TxnOffsetCommit),
            29 => Ok(ApiKey::DescribeAcls),
            30 => Ok(ApiKey::CreateAcls),
            31 => Ok(ApiKey::DeleteAcls),
            32 => Ok(ApiKey::DescribeConfigs),
            33 => Ok(ApiKey::AlterConfigs),
            34 => Ok(ApiKey::AlterReplicaLogDirs),
            35 => Ok(ApiKey::DescribeLogDirs),
            36 => Ok(ApiKey::SaslAuthenticate),
            37 => Ok(ApiKey::CreatePartitions),
            38 => Ok(ApiKey::CreateDelegationToken),
            39 => Ok(ApiKey::RenewDelegationToken),
            40 => Ok(ApiKey::ExpireDelegationToken),
            41 => Ok(ApiKey::DescribeDelegationToken),
            42 => Ok(ApiKey::DeleteGroups),
            43 => Ok(ApiKey::ElectLeaders),
            44 => Ok(ApiKey::IncrementalAlterConfigs),
            45 => Ok(ApiKey::AlterPartitionReassignments),
            46 => Ok(ApiKey::ListPartitionReassignments),
            47 => Ok(ApiKey::OffsetDelete),
            48 => Ok(ApiKey::DescribeClientQuotas),
            49 => Ok(ApiKey::AlterClientQuotas),
            50 => Ok(ApiKey::DescribeUserScramCredentials),
            51 => Ok(ApiKey::AlterUserScramCredentials),
            52 => Ok(ApiKey::Vote),
            53 => Ok(ApiKey::BeginQuorumEpoch),
            54 => Ok(ApiKey::EndQuorumEpoch),
            55 => Ok(ApiKey::DescribeQuorum),
            56 => Ok(ApiKey::AlterPartition),
            57 => Ok(ApiKey::UpdateFeatures),
            58 => Ok(ApiKey::Envelope),
            59 => Ok(ApiKey::FetchSnapshot),
            60 => Ok(ApiKey::DescribeCluster),
            61 => Ok(ApiKey::DescribeProducers),
            62 => Ok(ApiKey::DescribeTransactions),
            63 => Ok(ApiKey::ListTransactions),
            64 => Ok(ApiKey::AllocateProducerIds),
            _ => Err(ServerError::DecodingError(format!(
                "Unknown API key: {}",
                value
            ))),
        }
    }
}

impl Encodable for ApiKey {
    fn encode(&self, buf: &mut impl bytes::BufMut) -> Result<(), ServerError> {
        (*self as i16).encode(buf)
    }
}

impl Decodable for ApiKey {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let key = i16::decode(buf)?;
        ApiKey::try_from(key)
    }
}

impl fmt::Display for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Produce => "Produce",
            Self::Fetch => "Fetch",
            Self::ListOffsets => "ListOffsets",
            Self::Metadata => "Metadata",
            Self::LeaderAndIsr => "LeaderAndIsr",
            Self::StopReplica => "StopReplica",
            Self::UpdateMetadata => "UpdateMetadata",
            Self::ControlledShutdown => "ControlledShutdown",
            Self::OffsetCommit => "OffsetCommit",
            Self::OffsetFetch => "OffsetFetch",
            Self::FindCoordinator => "FindCoordinator",
            Self::JoinGroup => "JoinGroup",
            Self::Heartbeat => "Heartbeat",
            Self::LeaveGroup => "LeaveGroup",
            Self::SyncGroup => "SyncGroup",
            Self::DescribeGroups => "DescribeGroups",
            Self::ListGroups => "ListGroups",
            Self::SaslHandshake => "SaslHandshake",
            Self::ApiVersions => "ApiVersions",
            Self::CreateTopics => "CreateTopics",
            Self::DeleteTopics => "DeleteTopics",
            Self::DeleteRecords => "DeleteRecords",
            Self::InitProducerId => "InitProducerId",
            Self::OffsetForLeaderEpoch => "OffsetForLeaderEpoch",
            Self::AddPartitionsToTxn => "AddPartitionsToTxn",
            Self::AddOffsetsToTxn => "AddOffsetsToTxn",
            Self::EndTxn => "EndTxn",
            Self::WriteTxnMarkers => "WriteTxnMarkers",
            Self::TxnOffsetCommit => "TxnOffsetCommit",
            Self::DescribeAcls => "DescribeAcls",
            Self::CreateAcls => "CreateAcls",
            Self::DeleteAcls => "DeleteAcls",
            Self::DescribeConfigs => "DescribeConfigs",
            Self::AlterConfigs => "AlterConfigs",
            Self::AlterReplicaLogDirs => "AlterReplicaLogDirs",
            Self::DescribeLogDirs => "DescribeLogDirs",
            Self::SaslAuthenticate => "SaslAuthenticate",
            Self::CreatePartitions => "CreatePartitions",
            Self::CreateDelegationToken => "CreateDelegationToken",
            Self::RenewDelegationToken => "RenewDelegationToken",
            Self::ExpireDelegationToken => "ExpireDelegationToken",
            Self::DescribeDelegationToken => "DescribeDelegationToken",
            Self::DeleteGroups => "DeleteGroups",
            Self::ElectLeaders => "ElectLeaders",
            Self::IncrementalAlterConfigs => "IncrementalAlterConfigs",
            Self::AlterPartitionReassignments => "AlterPartitionReassignments",
            Self::ListPartitionReassignments => "ListPartitionReassignments",
            Self::OffsetDelete => "OffsetDelete",
            Self::DescribeClientQuotas => "DescribeClientQuotas",
            Self::AlterClientQuotas => "AlterClientQuotas",
            Self::DescribeUserScramCredentials => "DescribeUserScramCredentials",
            Self::AlterUserScramCredentials => "AlterUserScramCredentials",
            Self::Vote => "Vote",
            Self::BeginQuorumEpoch => "BeginQuorumEpoch",
            Self::EndQuorumEpoch => "EndQuorumEpoch",
            Self::DescribeQuorum => "DescribeQuorum",
            Self::AlterPartition => "AlterPartition",
            Self::UpdateFeatures => "UpdateFeatures",
            Self::Envelope => "Envelope",
            Self::FetchSnapshot => "FetchSnapshot",
            Self::DescribeCluster => "DescribeCluster",
            Self::DescribeProducers => "DescribeProducers",
            Self::DescribeTransactions => "DescribeTransactions",
            Self::ListTransactions => "ListTransactions",
            Self::AllocateProducerIds => "AllocateProducerIds",
        };
        write!(f, "{}", name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_key_conversion() {
        // Test a few known API keys
        assert_eq!(ApiKey::Produce.id(), 0);
        assert_eq!(ApiKey::from_id(0), ApiKey::Produce);
        assert_eq!(ApiKey::Fetch.id(), 1);
        assert_eq!(ApiKey::from_id(1), ApiKey::Fetch);
        assert_eq!(ApiKey::Metadata.id(), 3);
        assert_eq!(ApiKey::from_id(3), ApiKey::Metadata);

        // Test an unknown API key
        assert!(ApiKey::from_id(999).is_err());
    }

    #[test]
    fn test_api_key_display() {
        assert_eq!(format!("{}", ApiKey::Produce), "Produce");
        assert_eq!(format!("{}", ApiKey::Fetch), "Fetch");
        assert_eq!(format!("{}", ApiKey::Metadata), "Metadata");
    }
}
