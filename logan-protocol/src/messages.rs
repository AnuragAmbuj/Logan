//! Kafka protocol message types

use anyhow::Result;
use bytes::{Buf, BufMut};

use crate::error_codes::ErrorCode;
use crate::primitives::{
    CompactArray, CompactString, KafkaArray, KafkaBool, KafkaBytes, KafkaString, NullableBytes,
    NullableString, TaggedFields,
};
use crate::{ApiKey, Decodable, Encodable};

/// A Kafka protocol request header
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct RequestHeader {
    /// The API key of this request
    pub api_key: ApiKey,
    /// The API version of this request
    pub api_version: i16,
    /// The correlation ID of this request
    pub correlation_id: i32,
    /// The client ID string
    pub client_id: NullableString,
}

impl Encodable for RequestHeader {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.api_key as i16).encode(buf)?;
        self.api_version.encode(buf)?;
        self.correlation_id.encode(buf)?;
        self.client_id.encode(buf)?;
        Ok(())
    }
}

impl Decodable for RequestHeader {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let api_key = ApiKey::decode(buf)?;
        let api_version = i16::decode(buf)?;
        let correlation_id = i32::decode(buf)?;
        // The client_id is a nullable string. The version of the request
        // determines if it's present. For now we will assume a version that includes it.
        let client_id = NullableString::decode(buf)?;

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }
}

/// A Kafka protocol response header
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ResponseHeader {
    /// The correlation ID of this response
    pub correlation_id: i32,
}

impl Encodable for ResponseHeader {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.correlation_id.encode(buf)
    }
}

impl Decodable for ResponseHeader {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let correlation_id = i32::decode(buf)?;
        Ok(Self { correlation_id })
    }
}

/// A Kafka protocol response header (Flexible / v1)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ResponseHeaderFlexible {
    pub correlation_id: i32,
    pub _tagged_fields: TaggedFields,
}

impl Encodable for ResponseHeaderFlexible {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.correlation_id.encode(buf)?;
        self._tagged_fields.encode(buf)
    }
}

impl Decodable for ResponseHeaderFlexible {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let correlation_id = i32::decode(buf)?;
        let _tagged_fields = TaggedFields::decode(buf)?;
        Ok(Self {
            correlation_id,
            _tagged_fields,
        })
    }
}

// Metadata Structs and Impls removed/updated below
#[derive(Debug, Clone)]
pub struct MetadataRequest {
    pub topics: KafkaArray<KafkaString>,
}

impl Decodable for MetadataRequest {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let topics = KafkaArray::<KafkaString>::decode(buf)?;
        // V0: No allow_auto_topic_creation
        Ok(Self { topics })
    }
}

impl Encodable for MetadataRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topics.encode(buf)
    }
}

#[derive(Debug, Default)]
pub struct MetadataResponse {
    pub brokers: KafkaArray<BrokerV0>,
    pub topics: KafkaArray<TopicMetadata>,
}

impl Decodable for MetadataResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        // V0: No throttle_time_ms
        let brokers = KafkaArray::<BrokerV0>::decode(buf)?;
        // V0: No cluster_id, controller_id
        let topics = KafkaArray::<TopicMetadata>::decode(buf)?;
        Ok(Self { brokers, topics })
    }
}

impl Encodable for MetadataResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.brokers.encode(buf)?;
        self.topics.encode(buf)
    }
}

/// Broker information (V0)
#[derive(Debug, Default, Clone)]
pub struct BrokerV0 {
    pub node_id: i32,
    pub host: KafkaString,
    pub port: i32,
}

impl Decodable for BrokerV0 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let node_id = i32::decode(buf)?;
        let host = KafkaString::decode(buf)?;
        let port = i32::decode(buf)?;
        Ok(Self {
            node_id,
            host,
            port,
        })
    }
}

impl Encodable for BrokerV0 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.node_id.encode(buf)?;
        self.host.encode(buf)?;
        self.port.encode(buf)
    }
}

/// Broker information
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct Broker {
    /// The broker ID.
    pub node_id: i32,
    /// The broker hostname.
    pub host: KafkaString,
    /// The broker port.
    pub port: i32,
    /// The rack of the broker, or None if it has not been assigned to a rack.
    pub rack: NullableString,
}

impl Decodable for Broker {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let node_id = i32::decode(buf)?;
        let host = KafkaString::decode(buf)?;
        let port = i32::decode(buf)?;
        let rack = NullableString::decode(buf)?;
        Ok(Self {
            node_id,
            host,
            port,
            rack,
        })
    }
}

impl Encodable for Broker {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.node_id.encode(buf)?;
        self.host.encode(buf)?;
        self.port.encode(buf)?;
        self.rack.encode(buf)
    }
}

/// Topic metadata
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct TopicMetadata {
    /// The topic error, or None if there was no error.
    pub error_code: i16,
    /// The topic name.
    pub name: KafkaString,
    /// True if the topic is internal.
    pub is_internal: KafkaBool,
    /// The partitions of this topic.
    pub partitions: KafkaArray<PartitionMetadata>,
}

impl Decodable for TopicMetadata {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = i16::decode(buf)?;
        let name = KafkaString::decode(buf)?;
        let is_internal = KafkaBool::decode(buf)?;
        let partitions = KafkaArray::<PartitionMetadata>::decode(buf)?;
        Ok(Self {
            error_code,
            name,
            is_internal,
            partitions,
        })
    }
}

impl Encodable for TopicMetadata {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.error_code.encode(buf)?;
        self.name.encode(buf)?;
        self.is_internal.encode(buf)?;
        self.partitions.encode(buf)
    }
}

/// Partition metadata
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct PartitionMetadata {
    /// The partition error, or None if there was no error.
    pub error_code: i16,
    /// The partition index.
    pub partition_index: i32,
    /// The ID of the leader broker.
    pub leader_id: i32,
    /// The leader epoch.
    pub leader_epoch: i32,
    /// The set of all nodes that host this partition.
    pub replica_nodes: KafkaArray<i32>,
    /// The set of nodes that are in sync with the leader.
    pub isr_nodes: KafkaArray<i32>,
    /// The set of offline replicas.
    pub offline_replicas: KafkaArray<i32>,
}

impl Decodable for PartitionMetadata {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = i16::decode(buf)?;
        let partition_index = i32::decode(buf)?;
        let leader_id = i32::decode(buf)?;
        let leader_epoch = i32::decode(buf)?;
        let replica_nodes = KafkaArray::<i32>::decode(buf)?;
        let isr_nodes = KafkaArray::<i32>::decode(buf)?;
        let offline_replicas = KafkaArray::<i32>::decode(buf)?;
        Ok(Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            offline_replicas,
        })
    }
}

impl Encodable for PartitionMetadata {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.error_code.encode(buf)?;
        self.partition_index.encode(buf)?;
        self.leader_id.encode(buf)?;
        self.leader_epoch.encode(buf)?;
        self.replica_nodes.encode(buf)?;
        self.isr_nodes.encode(buf)?;
        self.offline_replicas.encode(buf)
    }
}

/// ApiVersions Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct ApiVersionsRequest;

impl Decodable for ApiVersionsRequest {
    fn decode(_buf: &mut impl Buf) -> Result<Self> {
        Ok(Self)
    }
}

impl Encodable for ApiVersionsRequest {
    fn encode(&self, _buf: &mut impl BufMut) -> Result<()> {
        Ok(())
    }
}

/// ApiVersions Response (v0+)
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct ApiVersionsResponse {
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The API versions supported by the broker.
    pub api_versions: KafkaArray<ApiVersionResponse>,
}

impl Decodable for ApiVersionsResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = i16::decode(buf)?;
        let api_versions = KafkaArray::<ApiVersionResponse>::decode(buf)?;
        Ok(Self {
            error_code,
            api_versions,
        })
    }
}

impl Encodable for ApiVersionsResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.error_code.encode(buf)?;
        self.api_versions.encode(buf)
    }
}

/// Part of the ApiVersionsResponse, representing a single API key and its supported versions.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct ApiVersionResponse {
    /// The API key.
    pub api_key: ApiKey,
    /// The minimum supported version, inclusive.
    pub min_version: i16,
    /// The maximum supported version, inclusive.
    pub max_version: i16,
}

impl Decodable for ApiVersionResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let api_key = ApiKey::decode(buf)?;
        let min_version = i16::decode(buf)?;
        let max_version = i16::decode(buf)?;
        Ok(Self {
            api_key,
            min_version,
            max_version,
        })
    }
}

impl Encodable for ApiVersionResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.api_key.encode(buf)?;
        self.min_version.encode(buf)?;
        self.max_version.encode(buf)
    }
}

/// ApiVersion for V3+ (Flexible headers)
#[derive(Debug, Default)]
pub struct ApiVersionV3 {
    pub api_key: ApiKey,
    pub min_version: i16,
    pub max_version: i16,
    pub _tagged_fields: TaggedFields,
}

impl Encodable for ApiVersionV3 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.api_key.encode(buf)?;
        self.min_version.encode(buf)?;
        self.max_version.encode(buf)?;
        self._tagged_fields.encode(buf)
    }
}

impl Decodable for ApiVersionV3 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let api_key = ApiKey::decode(buf)?;
        let min_version = i16::decode(buf)?;
        let max_version = i16::decode(buf)?;
        let _tagged_fields = TaggedFields::decode(buf)?;
        Ok(Self {
            api_key,
            min_version,
            max_version,
            _tagged_fields,
        })
    }
}

/// ApiVersions Response (v3+)
#[derive(Debug, Default)]
pub struct ApiVersionsResponseV3 {
    pub error_code: i16,
    pub api_keys: CompactArray<ApiVersionV3>,
    pub throttle_time_ms: i32,
    pub _tagged_fields: TaggedFields,
}

impl Encodable for ApiVersionsResponseV3 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.error_code.encode(buf)?;
        self.api_keys.encode(buf)?;
        self.throttle_time_ms.encode(buf)?;
        self._tagged_fields.encode(buf)
    }
}

impl Decodable for ApiVersionsResponseV3 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = i16::decode(buf)?;
        let api_keys = CompactArray::<ApiVersionV3>::decode(buf)?;
        let throttle_time_ms = i32::decode(buf)?;
        let _tagged_fields = TaggedFields::decode(buf)?;
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            _tagged_fields,
        })
    }
}

/// CreateTopics Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct CreateTopicsRequest {
    /// The topics to create.
    pub topics: KafkaArray<CreatableTopic>,
    /// The timeout in milliseconds for the request.
    pub timeout_ms: i32,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CreatableTopic {
    pub name: KafkaString,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub assignments: KafkaArray<ReplicaAssignment>,
    pub configs: KafkaArray<ConfigEntry>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ReplicaAssignment {
    pub partition_index: i32,
    pub broker_ids: KafkaArray<i32>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConfigEntry {
    pub config_name: KafkaString,
    pub config_value: NullableString,
}

/// CreateTopics Response (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct CreateTopicsResponse {
    /// The results for each topic.
    pub topic_errors: KafkaArray<TopicError>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct TopicError {
    /// The topic name.
    pub topic: KafkaString,
    /// The error code, or 0 if there was no error.
    pub error_code: ErrorCode,
}

impl Encodable for CreateTopicsRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topics.encode(buf)?;
        self.timeout_ms.encode(buf)
    }
}

impl Decodable for CreateTopicsRequest {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let topics = KafkaArray::<CreatableTopic>::decode(buf)?;
        let timeout_ms = i32::decode(buf)?;
        Ok(Self { topics, timeout_ms })
    }
}

impl Encodable for CreatableTopic {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.name.encode(buf)?;
        self.num_partitions.encode(buf)?;
        self.replication_factor.encode(buf)?;
        self.assignments.encode(buf)?;
        self.configs.encode(buf)
    }
}

impl Decodable for CreatableTopic {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let name = KafkaString::decode(buf)?;
        let num_partitions = i32::decode(buf)?;
        let replication_factor = i16::decode(buf)?;
        let assignments = KafkaArray::<ReplicaAssignment>::decode(buf)?;
        let configs = KafkaArray::<ConfigEntry>::decode(buf)?;
        Ok(Self {
            name,
            num_partitions,
            replication_factor,
            assignments,
            configs,
        })
    }
}

impl Encodable for ReplicaAssignment {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.partition_index.encode(buf)?;
        self.broker_ids.encode(buf)
    }
}

impl Decodable for ReplicaAssignment {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let partition_index = i32::decode(buf)?;
        let broker_ids = KafkaArray::<i32>::decode(buf)?;
        Ok(Self {
            partition_index,
            broker_ids,
        })
    }
}

impl Encodable for ConfigEntry {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.config_name.encode(buf)?;
        self.config_value.encode(buf)
    }
}

impl Decodable for ConfigEntry {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let config_name = KafkaString::decode(buf)?;
        let config_value = NullableString::decode(buf)?;
        Ok(Self {
            config_name,
            config_value,
        })
    }
}

impl Encodable for CreateTopicsResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topic_errors.encode(buf)
    }
}

impl Decodable for CreateTopicsResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let topic_errors = KafkaArray::<TopicError>::decode(buf)?;
        Ok(Self { topic_errors })
    }
}

impl Encodable for TopicError {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topic.encode(buf)?;
        self.error_code.encode(buf)
    }
}

impl Decodable for TopicError {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let topic = KafkaString::decode(buf)?;
        let error_code = ErrorCode::decode(buf)?;
        Ok(Self { topic, error_code })
    }
}

/// DeleteTopics Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct DeleteTopicsRequest {
    /// The topics to delete.
    pub topics: KafkaArray<KafkaString>,
    /// The timeout in milliseconds for the request.
    pub timeout_ms: i32,
}

/// DeleteTopics Response (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct DeleteTopicsResponse {
    /// The results for each topic.
    pub topic_errors: KafkaArray<TopicError>,
}

impl Encodable for DeleteTopicsRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topics.encode(buf)?;
        self.timeout_ms.encode(buf)
    }
}

impl Decodable for DeleteTopicsRequest {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let topics = KafkaArray::<KafkaString>::decode(buf)?;
        let timeout_ms = i32::decode(buf)?;
        Ok(Self { topics, timeout_ms })
    }
}

impl Encodable for DeleteTopicsResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topic_errors.encode(buf)
    }
}

impl Decodable for DeleteTopicsResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let topic_errors = KafkaArray::<TopicError>::decode(buf)?;
        Ok(Self { topic_errors })
    }
}

/// Produce Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct ProduceRequest {
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: KafkaArray<TopicProduceData>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct TopicProduceData {
    pub name: KafkaString,
    pub partition_data: KafkaArray<PartitionProduceData>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct PartitionProduceData {
    pub index: i32,
    pub record_set: KafkaBytes,
}

/// Produce Response (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct ProduceResponse {
    pub responses: KafkaArray<TopicProduceResponse>,
    pub throttle_time_ms: i32,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct TopicProduceResponse {
    pub name: KafkaString,
    pub partition_responses: KafkaArray<PartitionProduceResponse>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct PartitionProduceResponse {
    pub index: i32,
    pub error_code: ErrorCode,
    pub base_offset: i64,
}

impl Encodable for ProduceRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.acks.encode(buf)?;
        self.timeout_ms.encode(buf)?;
        self.topic_data.encode(buf)?;
        Ok(())
    }
}

impl Decodable for ProduceRequest {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let acks = i16::decode(buf)?;
        let timeout_ms = i32::decode(buf)?;
        let topic_data = KafkaArray::<TopicProduceData>::decode(buf)?;
        Ok(Self {
            acks,
            timeout_ms,
            topic_data,
        })
    }
}

impl Encodable for TopicProduceData {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.name.encode(buf)?;
        self.partition_data.encode(buf)?;
        Ok(())
    }
}

impl Decodable for TopicProduceData {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let name = KafkaString::decode(buf)?;
        let partition_data = KafkaArray::<PartitionProduceData>::decode(buf)?;
        Ok(Self {
            name,
            partition_data,
        })
    }
}

impl Encodable for PartitionProduceData {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.index.encode(buf)?;
        self.record_set.encode(buf)?;
        Ok(())
    }
}

impl Decodable for PartitionProduceData {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let index = i32::decode(buf)?;
        let record_set = KafkaBytes::decode(buf)?;
        Ok(Self { index, record_set })
    }
}

impl Encodable for ProduceResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.responses.encode(buf)?;
        self.throttle_time_ms.encode(buf)?;
        Ok(())
    }
}

impl Decodable for ProduceResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let responses = KafkaArray::<TopicProduceResponse>::decode(buf)?;
        let throttle_time_ms = i32::decode(buf)?;
        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }
}

impl Encodable for TopicProduceResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.name.encode(buf)?;
        self.partition_responses.encode(buf)?;
        Ok(())
    }
}

impl Decodable for TopicProduceResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let name = KafkaString::decode(buf)?;
        let partition_responses = KafkaArray::<PartitionProduceResponse>::decode(buf)?;
        Ok(Self {
            name,
            partition_responses,
        })
    }
}

impl Encodable for PartitionProduceResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.index.encode(buf)?;
        self.error_code.encode(buf)?;
        self.base_offset.encode(buf)?;
        Ok(())
    }
}

impl Decodable for PartitionProduceResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let index = i32::decode(buf)?;
        let error_code = ErrorCode::decode(buf)?;
        let base_offset = i64::decode(buf)?;
        Ok(Self {
            index,
            error_code,
            base_offset,
        })
    }
}

/// ListOffsets Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct ListOffsetsRequest {
    pub replica_id: i32,
    pub isolation_level: i8,
    pub topics: KafkaArray<ListOffsetsTopic>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ListOffsetsTopic {
    pub name: KafkaString,
    pub partitions: KafkaArray<ListOffsetsPartition>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ListOffsetsPartition {
    pub partition_index: i32,
    pub timestamp: i64,
}

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct ListOffsetsResponse {
    pub throttle_time_ms: i32,
    pub topics: KafkaArray<ListOffsetsTopicResponse>,
}

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct ListOffsetsTopicResponse {
    pub name: KafkaString,
    pub partitions: KafkaArray<ListOffsetsPartitionResponse>,
}

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct ListOffsetsPartitionResponse {
    pub partition_index: i32,
    pub error_code: ErrorCode,
    pub timestamp: i64,
    pub offset: i64,
    pub leader_epoch: i32,
}

impl Encodable for ListOffsetsRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.replica_id.encode(buf)?;
        self.isolation_level.encode(buf)?;
        self.topics.encode(buf)
    }
}

impl Decodable for ListOffsetsRequest {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let replica_id = i32::decode(buf)?;
        let isolation_level = i8::decode(buf)?;
        let topics = KafkaArray::<ListOffsetsTopic>::decode(buf)?;
        Ok(Self {
            replica_id,
            isolation_level,
            topics,
        })
    }
}

impl Encodable for ListOffsetsTopic {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.name.encode(buf)?;
        self.partitions.encode(buf)
    }
}

impl Decodable for ListOffsetsTopic {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let name = KafkaString::decode(buf)?;
        let partitions = KafkaArray::<ListOffsetsPartition>::decode(buf)?;
        Ok(Self { name, partitions })
    }
}

impl Encodable for ListOffsetsPartition {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.partition_index.encode(buf)?;
        self.timestamp.encode(buf)
    }
}

impl Decodable for ListOffsetsPartition {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let partition_index = i32::decode(buf)?;
        let timestamp = i64::decode(buf)?;
        Ok(Self {
            partition_index,
            timestamp,
        })
    }
}

impl Encodable for ListOffsetsResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.throttle_time_ms.encode(buf)?;
        self.topics.encode(buf)
    }
}

impl Decodable for ListOffsetsResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topics = KafkaArray::<ListOffsetsTopicResponse>::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

impl Encodable for ListOffsetsTopicResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.name.encode(buf)?;
        self.partitions.encode(buf)
    }
}

impl Decodable for ListOffsetsTopicResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let name = KafkaString::decode(buf)?;
        let partitions = KafkaArray::<ListOffsetsPartitionResponse>::decode(buf)?;
        Ok(Self { name, partitions })
    }
}

impl Encodable for ListOffsetsPartitionResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.partition_index.encode(buf)?;
        self.error_code.encode(buf)?;
        self.timestamp.encode(buf)?;
        self.offset.encode(buf)?;
        self.leader_epoch.encode(buf)
    }
}

impl Decodable for ListOffsetsPartitionResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let partition_index = i32::decode(buf)?;
        let error_code = ErrorCode::decode(buf)?;
        let timestamp = i64::decode(buf)?;
        let offset = i64::decode(buf)?;
        let leader_epoch = i32::decode(buf)?;
        Ok(Self {
            partition_index,
            error_code,
            timestamp,
            offset,
            leader_epoch,
        })
    }
}

/// Fetch Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct FetchRequest {
    pub topics: KafkaArray<FetchTopic>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FetchTopic {
    pub topic: KafkaString,
    pub partitions: KafkaArray<FetchPartition>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FetchPartition {
    pub partition_index: i32,
    pub fetch_offset: i64,
    pub max_bytes: i32,
}

/// Fetch Response (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct FetchResponse {
    pub responses: KafkaArray<FetchableTopicResponse>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FetchableTopicResponse {
    pub topic: KafkaString,
    pub partitions: KafkaArray<PartitionData>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct PartitionData {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub records: NullableBytes,
}

impl Encodable for FetchRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topics.encode(buf)
    }
}

impl Decodable for FetchRequest {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let topics = KafkaArray::<FetchTopic>::decode(buf)?;
        Ok(Self { topics })
    }
}

impl Encodable for FetchTopic {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topic.encode(buf)?;
        self.partitions.encode(buf)
    }
}

impl Decodable for FetchTopic {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let topic = KafkaString::decode(buf)?;
        let partitions = KafkaArray::<FetchPartition>::decode(buf)?;
        Ok(Self { topic, partitions })
    }
}

impl Encodable for FetchPartition {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.partition_index.encode(buf)?;
        self.fetch_offset.encode(buf)?;
        self.max_bytes.encode(buf)
    }
}

impl Decodable for FetchPartition {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let partition_index = i32::decode(buf)?;
        let fetch_offset = i64::decode(buf)?;
        let max_bytes = i32::decode(buf)?;
        Ok(Self {
            partition_index,
            fetch_offset,
            max_bytes,
        })
    }
}

impl Encodable for FetchResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.responses.encode(buf)
    }
}

impl Decodable for FetchResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let responses = KafkaArray::<FetchableTopicResponse>::decode(buf)?;
        Ok(Self { responses })
    }
}

impl Encodable for FetchableTopicResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topic.encode(buf)?;
        self.partitions.encode(buf)
    }
}

impl Decodable for FetchableTopicResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let topic = KafkaString::decode(buf)?;
        let partitions = KafkaArray::<PartitionData>::decode(buf)?;
        Ok(Self { topic, partitions })
    }
}

impl Encodable for PartitionData {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.partition_index.encode(buf)?;
        self.error_code.encode(buf)?;
        self.high_watermark.encode(buf)?;
        self.records.encode(buf)
    }
}

impl Decodable for PartitionData {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let partition_index = i32::decode(buf)?;
        let error_code = i16::decode(buf)?;
        let high_watermark = i64::decode(buf)?;
        let records = NullableBytes::decode(buf)?;
        Ok(Self {
            partition_index,
            error_code,
            high_watermark,
            records,
        })
    }
}

/// OffsetCommit Request (v0-v2 common fields)
#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetCommitRequest {
    pub group_id: KafkaString,
    pub generation_id: i32,
    pub member_id: KafkaString,
    // V2+: retention_time. We'll include it for V2 compatibility, defaults to -1 (generic)
    pub retention_time: i64,
    pub topics: KafkaArray<OffsetCommitTopic>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetCommitTopic {
    pub name: KafkaString,
    pub partitions: KafkaArray<OffsetCommitPartition>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetCommitPartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub metadata: NullableString,
}

/// OffsetCommit Response
#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetCommitResponse {
    pub topics: KafkaArray<OffsetCommitTopicResponse>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetCommitTopicResponse {
    pub name: KafkaString,
    pub partitions: KafkaArray<OffsetCommitPartitionResponse>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetCommitPartitionResponse {
    pub partition_index: i32,
    pub error_code: ErrorCode,
}

impl Encodable for OffsetCommitRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.group_id.encode(buf)?;
        self.generation_id.encode(buf)?;
        self.member_id.encode(buf)?;
        self.retention_time.encode(buf)?;
        self.topics.encode(buf)
    }
}

impl Decodable for OffsetCommitRequest {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let group_id = KafkaString::decode(buf)?;
        let generation_id = i32::decode(buf)?;
        let member_id = KafkaString::decode(buf)?;
        let retention_time = i64::decode(buf)?;
        let topics = KafkaArray::<OffsetCommitTopic>::decode(buf)?;
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            retention_time,
            topics,
        })
    }
}

impl Encodable for OffsetCommitTopic {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.name.encode(buf)?;
        self.partitions.encode(buf)
    }
}

impl Decodable for OffsetCommitTopic {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let name = KafkaString::decode(buf)?;
        let partitions = KafkaArray::<OffsetCommitPartition>::decode(buf)?;
        Ok(Self { name, partitions })
    }
}

impl Encodable for OffsetCommitPartition {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.partition_index.encode(buf)?;
        self.committed_offset.encode(buf)?;
        self.metadata.encode(buf)
    }
}

impl Decodable for OffsetCommitPartition {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let partition_index = i32::decode(buf)?;
        let committed_offset = i64::decode(buf)?;
        let metadata = NullableString::decode(buf)?;
        Ok(Self {
            partition_index,
            committed_offset,
            metadata,
        })
    }
}

impl Encodable for OffsetCommitResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topics.encode(buf)
    }
}

impl Decodable for OffsetCommitResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let topics = KafkaArray::<OffsetCommitTopicResponse>::decode(buf)?;
        Ok(Self { topics })
    }
}

impl Encodable for OffsetCommitTopicResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.name.encode(buf)?;
        self.partitions.encode(buf)
    }
}

impl Decodable for OffsetCommitTopicResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let name = KafkaString::decode(buf)?;
        let partitions = KafkaArray::<OffsetCommitPartitionResponse>::decode(buf)?;
        Ok(Self { name, partitions })
    }
}

impl Encodable for OffsetCommitPartitionResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.partition_index.encode(buf)?;
        self.error_code.encode(buf)
    }
}

impl Decodable for OffsetCommitPartitionResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let partition_index = i32::decode(buf)?;
        let error_code = ErrorCode::decode(buf)?;
        Ok(Self {
            partition_index,
            error_code,
        })
    }
}

/// OffsetFetch Request (v1+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetFetchRequest {
    pub group_id: KafkaString,
    pub topics: KafkaArray<OffsetFetchTopic>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetFetchTopic {
    pub name: KafkaString,
    pub partition_indexes: KafkaArray<i32>,
}

/// OffsetFetch Response
#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetFetchResponse {
    pub topics: KafkaArray<OffsetFetchTopicResponse>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetFetchTopicResponse {
    pub name: KafkaString,
    pub partitions: KafkaArray<OffsetFetchPartitionResponse>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetFetchPartitionResponse {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub metadata: NullableString,
    pub error_code: ErrorCode,
}

impl Encodable for OffsetFetchRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.group_id.encode(buf)?;
        self.topics.encode(buf)
    }
}

impl Decodable for OffsetFetchRequest {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let group_id = KafkaString::decode(buf)?;
        let topics = KafkaArray::<OffsetFetchTopic>::decode(buf)?;
        Ok(Self { group_id, topics })
    }
}

impl Encodable for OffsetFetchTopic {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.name.encode(buf)?;
        self.partition_indexes.encode(buf)
    }
}

impl Decodable for OffsetFetchTopic {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let name = KafkaString::decode(buf)?;
        let partition_indexes = KafkaArray::<i32>::decode(buf)?;
        Ok(Self {
            name,
            partition_indexes,
        })
    }
}

impl Encodable for OffsetFetchResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topics.encode(buf)
    }
}

impl Decodable for OffsetFetchResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let topics = KafkaArray::<OffsetFetchTopicResponse>::decode(buf)?;
        Ok(Self { topics })
    }
}

impl Encodable for OffsetFetchTopicResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.name.encode(buf)?;
        self.partitions.encode(buf)
    }
}

impl Decodable for OffsetFetchTopicResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let name = KafkaString::decode(buf)?;
        let partitions = KafkaArray::<OffsetFetchPartitionResponse>::decode(buf)?;
        Ok(Self { name, partitions })
    }
}

impl Encodable for OffsetFetchPartitionResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.partition_index.encode(buf)?;
        self.committed_offset.encode(buf)?;
        self.metadata.encode(buf)?;
        self.error_code.encode(buf)
    }
}

impl Decodable for OffsetFetchPartitionResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let partition_index = i32::decode(buf)?;
        let committed_offset = i64::decode(buf)?;
        let metadata = NullableString::decode(buf)?;
        let error_code = ErrorCode::decode(buf)?;
        Ok(Self {
            partition_index,
            committed_offset,
            metadata,
            error_code,
        })
    }
}
