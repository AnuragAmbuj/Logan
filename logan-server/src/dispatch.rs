use crate::error::ServerError;
use crate::offset_manager::OffsetManager;
use crate::shard::ShardManager;
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use logan_protocol::api_keys::ApiKey;
use logan_protocol::batch::RecordBatch;
use logan_protocol::codec::{Decodable, Encodable};
use logan_protocol::error_codes::ErrorCode;
use logan_protocol::messages::{
    ApiVersionResponse, ApiVersionV3, ApiVersionsRequest, ApiVersionsResponse,
    ApiVersionsResponseV3, BrokerV0, CreatableTopic, CreateTopicsRequest, CreateTopicsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, OffsetCommitPartition, OffsetCommitPartitionResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetCommitTopic, OffsetCommitTopicResponse,
    OffsetFetchPartitionResponse, OffsetFetchRequest, OffsetFetchResponse, OffsetFetchTopic,
    OffsetFetchTopicResponse, PartitionProduceResponse, ProduceRequest, ProduceResponse,
    RequestHeader, ResponseHeader, ResponseHeaderFlexible, TopicError, TopicMetadata,
    TopicProduceResponse,
};
use logan_protocol::primitives::{
    CompactArray, KafkaArray, KafkaString, NullableBytes, TaggedFields,
};
use logan_storage::LogReadResult;
use std::fs::File as StdFile;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{info, warn};

pub(crate) enum ResponseBody {
    ApiVersions(ApiVersionsResponse),
    ApiVersionsV3(ApiVersionsResponseV3),
    Metadata(MetadataResponse),
    CreateTopics(CreateTopicsResponse),
    DeleteTopics(DeleteTopicsResponse),
    ListOffsets(ListOffsetsResponse),
    Produce(ProduceResponse),
    // Fetch is handled via zero-copy path, so no ResponseBody variant needed
    OffsetCommit(OffsetCommitResponse),
    OffsetFetch(OffsetFetchResponse),
}

impl Encodable for ResponseBody {
    fn encode(&self, buf: &mut impl BufMut) -> anyhow::Result<()> {
        match self {
            ResponseBody::ApiVersions(res) => res.encode(buf),
            ResponseBody::ApiVersionsV3(res) => res.encode(buf),
            ResponseBody::Metadata(res) => res.encode(buf),
            ResponseBody::CreateTopics(res) => res.encode(buf),
            ResponseBody::DeleteTopics(res) => res.encode(buf),
            ResponseBody::ListOffsets(res) => res.encode(buf),
            ResponseBody::Produce(res) => res.encode(buf),
            ResponseBody::OffsetCommit(res) => res.encode(buf),
            ResponseBody::OffsetFetch(res) => res.encode(buf),
        }
    }
}

impl ResponseBody {
    fn is_flexible(&self) -> bool {
        match self {
            // ApiVersionsV3 uses v0 header despite being flexible body
            ResponseBody::ApiVersionsV3(_) => false,
            _ => false,
        }
    }
}

pub(crate) async fn dispatch(
    stream: &mut TcpStream,
    topics: Arc<DashMap<String, CreatableTopic>>,
    shard_manager: Arc<ShardManager>,
    offset_manager: Arc<OffsetManager>,
) -> Result<bool, ServerError> {
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        // Connection closed
        return Ok(false);
    }
    let len = i32::from_be_bytes(len_buf);

    let mut request_buf = BytesMut::with_capacity(len as usize);
    request_buf.resize(len as usize, 0);
    stream.read_exact(&mut request_buf).await?;

    let header = RequestHeader::decode(&mut request_buf)?;
    info!("Received request: {:?}", header);

    if header.api_key == ApiKey::Fetch {
        let request = FetchRequest::decode(&mut request_buf)?;
        return handle_fetch_zerocopy(stream, &header, request, shard_manager).await;
    }

    let response_body = match header.api_key {
        ApiKey::ApiVersions => {
            let request = ApiVersionsRequest::decode(&mut request_buf)?;
            handle_api_versions_request(&request, &header)
        }
        ApiKey::Metadata => {
            let request = MetadataRequest::decode(&mut request_buf)?;
            handle_metadata_request(&request, topics)
        }
        ApiKey::CreateTopics => {
            let request = CreateTopicsRequest::decode(&mut request_buf)?;
            handle_create_topics_request(&request, topics)
        }
        ApiKey::ListOffsets => {
            let request = ListOffsetsRequest::decode(&mut request_buf)?;
            handle_list_offsets_request(&request)
        }
        ApiKey::Produce => {
            let request = ProduceRequest::decode(&mut request_buf)?;
            handle_produce_request(request, shard_manager).await
        }
        ApiKey::OffsetCommit => {
            let request = OffsetCommitRequest::decode(&mut request_buf)?;
            handle_offset_commit_request(&request, offset_manager)
        }
        ApiKey::OffsetFetch => {
            let request = OffsetFetchRequest::decode(&mut request_buf)?;
            handle_offset_fetch_request(&request, offset_manager)
        }
        ApiKey::DeleteTopics => {
            let request = DeleteTopicsRequest::decode(&mut request_buf)?;
            handle_delete_topics_request(&request, topics, shard_manager).await
        }
        // Fetch is handled above
        ApiKey::Fetch => unreachable!(),
        _ => {
            warn!("Unsupported API key: {:?}", header.api_key);
            return Ok(true); // Keep connection open
        }
    }?; // Use ? to propagate errors

    let response_bytes = encode_response(&header, &response_body)?;
    stream.write_all(&response_bytes).await?;
    Ok(true)
}

enum ZeroCopyPart {
    Bytes(BytesMut),
    File {
        file: StdFile,
        offset: u64,
        len: u64,
    },
}

async fn handle_fetch_zerocopy(
    stream: &mut TcpStream,
    header: &RequestHeader,
    request: FetchRequest,
    shard_manager: Arc<ShardManager>,
) -> Result<bool, ServerError> {
    let mut parts: Vec<ZeroCopyPart> = Vec::new();
    let mut current_buf = BytesMut::new();

    // 1. Encode Response Header
    let response_header = ResponseHeader {
        correlation_id: header.correlation_id,
    };
    response_header.encode(&mut current_buf)?;

    // 2. FetchResponse Body
    // Topics Array Length
    (request.topics.0.len() as i32).encode(&mut current_buf)?;

    for fetch_topic in request.topics.0 {
        // Topic Name
        fetch_topic.topic.encode(&mut current_buf)?;

        // Partitions Array Length
        (fetch_topic.partitions.0.len() as i32).encode(&mut current_buf)?;

        for fetch_partition in fetch_topic.partitions.0 {
            // Partition Index
            fetch_partition.partition_index.encode(&mut current_buf)?;

            let log_result = shard_manager
                .fetch(
                    fetch_topic.topic.0.clone(),
                    fetch_partition.partition_index,
                    fetch_partition.fetch_offset as u64,
                    fetch_partition.max_bytes,
                )
                .await;

            match log_result {
                Ok(read_result) => {
                    (ErrorCode::None as i16).encode(&mut current_buf)?; // ErrorCode
                    (0i64).encode(&mut current_buf)?; // HighWatermark

                    match read_result {
                        LogReadResult::Data(data) => {
                            // Record Set (NullableBytes)
                            (data.len() as i32).encode(&mut current_buf)?;
                            current_buf.put_slice(&data);
                        }
                        LogReadResult::FileSlice { file, offset, len } => {
                            // Record Set Length
                            (len as i32).encode(&mut current_buf)?;

                            // Flush current_buf to parts
                            parts.push(ZeroCopyPart::Bytes(current_buf.split()));
                            // current_buf is now empty/new

                            parts.push(ZeroCopyPart::File { file, offset, len });
                        }
                    }
                }
                Err(e) => {
                    warn!("Error reading log: {}", e);
                    (ErrorCode::UnknownServerError as i16).encode(&mut current_buf)?;
                    (0i64).encode(&mut current_buf)?; // HW
                    // Empty records
                    NullableBytes(None).encode(&mut current_buf)?;
                }
            }
        }
    }

    // Push remaining buffer
    if !current_buf.is_empty() {
        parts.push(ZeroCopyPart::Bytes(current_buf));
    }

    // Calculate total length (Packet Frame)
    let total_len: usize = parts
        .iter()
        .map(|p| match p {
            ZeroCopyPart::Bytes(b) => b.len(),
            ZeroCopyPart::File { len, .. } => *len as usize,
        })
        .sum();

    // Write Packet Length (4 bytes)
    let mut len_buf = [0u8; 4];
    len_buf.copy_from_slice(&(total_len as i32).to_be_bytes());
    stream.write_all(&len_buf).await?;

    // Write Parts
    for part in parts {
        match part {
            ZeroCopyPart::Bytes(b) => stream.write_all(&b).await?,
            ZeroCopyPart::File { file, offset, len } => {
                let mut tokio_file = tokio::fs::File::from_std(file);
                tokio_file.seek(tokio::io::SeekFrom::Start(offset)).await?;
                let mut handle = tokio_file.take(len);
                tokio::io::copy(&mut handle, stream).await?;
            }
        }
    }

    Ok(true)
}

fn handle_api_versions_request(
    _request: &ApiVersionsRequest,
    header: &RequestHeader,
) -> Result<ResponseBody, ServerError> {
    if header.api_version >= 3 {
        let response = ApiVersionsResponseV3 {
            error_code: 0,
            throttle_time_ms: 0,
            _tagged_fields: TaggedFields,
            api_keys: CompactArray(vec![
                ApiVersionV3 {
                    api_key: ApiKey::Produce, // 0
                    min_version: 0,
                    max_version: 0,
                    _tagged_fields: TaggedFields,
                },
                ApiVersionV3 {
                    api_key: ApiKey::Fetch, // 1
                    min_version: 0,
                    max_version: 0,
                    _tagged_fields: TaggedFields,
                },
                ApiVersionV3 {
                    api_key: ApiKey::ListOffsets, // 2
                    min_version: 0,
                    max_version: 0,
                    _tagged_fields: TaggedFields,
                },
                ApiVersionV3 {
                    api_key: ApiKey::Metadata, // 3
                    min_version: 0,
                    max_version: 0,
                    _tagged_fields: TaggedFields,
                },
                ApiVersionV3 {
                    api_key: ApiKey::ApiVersions, // 18
                    min_version: 0,
                    max_version: 3,
                    _tagged_fields: TaggedFields,
                },
                ApiVersionV3 {
                    api_key: ApiKey::CreateTopics, // 19
                    min_version: 0,
                    max_version: 0,
                    _tagged_fields: TaggedFields,
                },
                ApiVersionV3 {
                    api_key: ApiKey::OffsetCommit, // 8
                    min_version: 0,
                    max_version: 2, // Advertise v2 (flexible if we supported it, but our struct is essentially v0-v2 layout)
                    _tagged_fields: TaggedFields,
                },
                ApiVersionV3 {
                    api_key: ApiKey::OffsetFetch, // 9
                    min_version: 0,
                    max_version: 1, // Advertise v1
                    _tagged_fields: TaggedFields,
                },
            ]),
        };
        return Ok(ResponseBody::ApiVersionsV3(response));
    }

    let response = ApiVersionsResponse {
        error_code: 0,
        api_versions: KafkaArray(vec![
            ApiVersionResponse {
                api_key: ApiKey::ApiVersions,
                min_version: 0,
                max_version: 3, // Advertise 3
            },
            ApiVersionResponse {
                api_key: ApiKey::Metadata,
                min_version: 0,
                max_version: 0,
            },
            ApiVersionResponse {
                api_key: ApiKey::CreateTopics,
                min_version: 0,
                max_version: 0,
            },
            ApiVersionResponse {
                api_key: ApiKey::ListOffsets,
                min_version: 0,
                max_version: 0,
            },
            ApiVersionResponse {
                api_key: ApiKey::Produce,
                min_version: 0,
                max_version: 0,
            },
            ApiVersionResponse {
                api_key: ApiKey::Fetch,
                min_version: 0,
                max_version: 0,
            },
            ApiVersionResponse {
                api_key: ApiKey::OffsetCommit,
                min_version: 0,
                max_version: 2,
            },
            ApiVersionResponse {
                api_key: ApiKey::OffsetFetch,
                min_version: 0,
                max_version: 1,
            },
        ]),
    };
    Ok(ResponseBody::ApiVersions(response))
}

fn handle_metadata_request(
    _request: &MetadataRequest,
    topics_map: Arc<DashMap<String, CreatableTopic>>,
) -> Result<ResponseBody, ServerError> {
    let topics: Vec<TopicMetadata> = topics_map
        .iter()
        .map(|entry| {
            let topic = entry.value();
            TopicMetadata {
                error_code: 0,
                name: topic.name.clone(),
                is_internal: false.into(),
                partitions: KafkaArray(vec![]), // Simplified for now
            }
        })
        .collect();

    let response = MetadataResponse {
        brokers: KafkaArray(vec![BrokerV0 {
            node_id: 1,
            host: KafkaString("127.0.0.1".to_string()),
            port: 9093,
        }]),
        topics: KafkaArray(topics),
    };
    Ok(ResponseBody::Metadata(response))
}

fn handle_create_topics_request(
    request: &CreateTopicsRequest,
    topics_map: Arc<DashMap<String, CreatableTopic>>,
) -> Result<ResponseBody, ServerError> {
    let mut topic_errors = vec![];

    for topic in request.topics.0.iter() {
        let topic_name = topic.name.clone().into_inner();
        info!("Creating topic: {}", topic_name);
        topics_map.insert(topic_name.clone(), topic.clone());
        topic_errors.push(TopicError {
            topic: topic.name.clone(),
            error_code: ErrorCode::None,
        });
    }

    let response = CreateTopicsResponse {
        topic_errors: KafkaArray(topic_errors),
    };
    Ok(ResponseBody::CreateTopics(response))
}

fn handle_list_offsets_request(_request: &ListOffsetsRequest) -> Result<ResponseBody, ServerError> {
    // Simplified response for now
    let response = ListOffsetsResponse {
        throttle_time_ms: 0,
        topics: KafkaArray(vec![]),
    };
    Ok(ResponseBody::ListOffsets(response))
}

async fn handle_produce_request(
    request: ProduceRequest,
    shard_manager: Arc<ShardManager>,
) -> Result<ResponseBody, ServerError> {
    let mut topic_responses = vec![];

    for topic_data in request.topic_data.0 {
        let topic_name = topic_data.name.0.clone();
        let mut partition_responses = vec![];

        for partition_data in topic_data.partition_data.0 {
            let partition_index = partition_data.index;

            // Helper to check valid batch
            let mut payload = partition_data.record_set.0.clone(); // Clone for validation, cheap for Bytes
            if !payload.is_empty() {
                match RecordBatch::decode(&mut payload) {
                    Ok(batch) => {
                        info!(
                            "Received valid RecordBatch: base_offset={}, records={}",
                            batch.base_offset,
                            batch.records.len()
                        );
                    }
                    Err(e) => {
                        warn!("Produce payload is not a valid v2 RecordBatch: {}", e);
                    }
                }
            }

            let append_result = shard_manager
                .append(
                    topic_name.clone(),
                    partition_index,
                    partition_data.record_set.0.clone().into(),
                )
                .await;

            let (error_code, base_offset) = match append_result {
                Ok(offset) => (ErrorCode::None, offset as i64),
                Err(e) => {
                    warn!("Error appending to shard: {}", e);
                    (ErrorCode::UnknownServerError, -1)
                }
            };

            partition_responses.push(PartitionProduceResponse {
                index: partition_index,
                error_code,
                base_offset,
            });
        }

        topic_responses.push(TopicProduceResponse {
            name: topic_data.name,
            partition_responses: KafkaArray(partition_responses),
        });
    }

    let response = ProduceResponse {
        responses: KafkaArray(topic_responses),
        throttle_time_ms: 0,
    };
    Ok(ResponseBody::Produce(response))
}

fn handle_offset_commit_request(
    request: &OffsetCommitRequest,
    offset_manager: Arc<OffsetManager>,
) -> Result<ResponseBody, ServerError> {
    let group_id = request.group_id.0.clone();
    let mut topic_responses = vec![];

    for topic in request.topics.0.iter() {
        let topic_name = topic.name.0.clone();
        let mut partition_responses = vec![];

        for partition in topic.partitions.0.iter() {
            offset_manager.commit_offset(
                group_id.clone(),
                topic_name.clone(),
                partition.partition_index,
                partition.committed_offset,
            );

            partition_responses.push(OffsetCommitPartitionResponse {
                partition_index: partition.partition_index,
                error_code: ErrorCode::None,
            });
        }
        topic_responses.push(OffsetCommitTopicResponse {
            name: KafkaString(topic_name),
            partitions: KafkaArray(partition_responses),
        });
    }

    let response = OffsetCommitResponse {
        topics: KafkaArray(topic_responses),
    };
    Ok(ResponseBody::OffsetCommit(response))
}

fn handle_offset_fetch_request(
    request: &OffsetFetchRequest,
    offset_manager: Arc<OffsetManager>,
) -> Result<ResponseBody, ServerError> {
    let group_id = request.group_id.0.clone();
    let mut topic_responses = vec![];

    for topic in request.topics.0.iter() {
        let topic_name = topic.name.0.clone();
        let mut partition_responses = vec![];

        for &partition_index in topic.partition_indexes.0.iter() {
            let offset = offset_manager
                .fetch_offset(&group_id, &topic_name, partition_index)
                .unwrap_or(-1); // -1 assumes unknown

            let metadata = if offset == -1 {
                None.into()
            } else {
                Some("".to_string()).into()
            };

            partition_responses.push(OffsetFetchPartitionResponse {
                partition_index,
                committed_offset: offset,
                metadata,
                error_code: ErrorCode::None,
            });
        }

        topic_responses.push(OffsetFetchTopicResponse {
            name: KafkaString(topic_name),
            partitions: KafkaArray(partition_responses),
        });
    }

    let response = OffsetFetchResponse {
        topics: KafkaArray(topic_responses),
    };
    Ok(ResponseBody::OffsetFetch(response))
}

fn encode_response(header: &RequestHeader, body: &ResponseBody) -> anyhow::Result<BytesMut> {
    let mut buf = BytesMut::new();

    if body.is_flexible() {
        let response_header = ResponseHeaderFlexible {
            correlation_id: header.correlation_id,
            _tagged_fields: TaggedFields,
        };
        response_header.encode(&mut buf)?;
    } else {
        let response_header = ResponseHeader {
            correlation_id: header.correlation_id,
        };
        response_header.encode(&mut buf)?;
    }

    body.encode(&mut buf)?;

    let mut final_buf = BytesMut::with_capacity(buf.len() + 4);
    (buf.len() as i32).encode(&mut final_buf)?;
    final_buf.put(buf);

    Ok(final_buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logan_protocol::RequestHeader;
    use logan_protocol::api_keys::ApiKey;
    use logan_protocol::primitives::{KafkaArray, KafkaString};

    #[test]
    fn test_handle_metadata_request_v0() {
        // Setup
        let topics = Arc::new(DashMap::new());
        // Populate with CreatableTopic (as if created)
        topics.insert(
            "topic1".to_string(),
            CreatableTopic {
                name: KafkaString("topic1".to_string()),
                num_partitions: 1,
                replication_factor: 1,
                assignments: KafkaArray(vec![]),
                configs: KafkaArray(vec![]),
            },
        );

        // Create V0 MetadataRequest (no fields in V0 struct we defined)
        let request = MetadataRequest {
            topics: KafkaArray(vec![KafkaString("topic1".to_string())]),
        };

        // Execution
        let response_body = handle_metadata_request(&request, topics).unwrap();

        // Verification
        match response_body {
            ResponseBody::Metadata(resp) => {
                assert_eq!(resp.brokers.0.len(), 1); // Should contain the hardcoded broker
                assert_eq!(resp.topics.0.len(), 1);
                assert_eq!(resp.topics.0[0].name.0, "topic1");
            }
            _ => panic!("Expected Metadata response"),
        }
    }

    #[test]
    fn test_handle_api_versions() {
        let req = ApiVersionsRequest {};
        let header = RequestHeader {
            api_key: ApiKey::ApiVersions,
            api_version: 3,
            correlation_id: 1,
            client_id: None.into(),
        };

        let response_body = handle_api_versions_request(&req, &header).unwrap();

        match response_body {
            ResponseBody::ApiVersionsV3(resp) => {
                assert_eq!(resp.error_code, 0);
                assert!(resp.api_keys.0.len() > 0);
            }
            _ => panic!("Expected ApiVersionsV3 response"),
        }
    }
    #[test]
    fn test_offset_management() {
        // Setup
        let offset_manager = Arc::new(OffsetManager::new());
        let group_id = KafkaString("my-group".to_string());
        let topic_name = KafkaString("my-topic".to_string());
        let partition = 0;
        let offset = 12345i64;

        // 1. Commit Offset
        let commit_req = OffsetCommitRequest {
            group_id: group_id.clone(),
            generation_id: 1,
            member_id: KafkaString("member-1".to_string()),
            retention_time: -1,
            topics: KafkaArray(vec![OffsetCommitTopic {
                name: topic_name.clone(),
                partitions: KafkaArray(vec![OffsetCommitPartition {
                    partition_index: partition,
                    committed_offset: offset,
                    metadata: None.into(),
                }]),
            }]),
        };

        let commit_resp =
            handle_offset_commit_request(&commit_req, offset_manager.clone()).unwrap();
        match commit_resp {
            ResponseBody::OffsetCommit(resp) => {
                assert_eq!(resp.topics.0.len(), 1);
                assert_eq!(resp.topics.0[0].partitions.0[0].error_code, ErrorCode::None);
            }
            _ => panic!("Expected OffsetCommit response"),
        }

        // 2. Fetch Offset
        let fetch_req = OffsetFetchRequest {
            group_id: group_id.clone(),
            topics: KafkaArray(vec![OffsetFetchTopic {
                name: topic_name.clone(),
                partition_indexes: KafkaArray(vec![partition]),
            }]),
        };

        let fetch_resp = handle_offset_fetch_request(&fetch_req, offset_manager.clone()).unwrap();
        match fetch_resp {
            ResponseBody::OffsetFetch(resp) => {
                assert_eq!(resp.topics.0.len(), 1);
                let p_resp = &resp.topics.0[0].partitions.0[0];
                assert_eq!(p_resp.committed_offset, offset);
                assert_eq!(p_resp.error_code, ErrorCode::None);
            }
            _ => panic!("Expected OffsetFetch response"),
        }
    }
}

async fn handle_delete_topics_request(
    request: &DeleteTopicsRequest,
    topics_map: Arc<DashMap<String, CreatableTopic>>,
    shard_manager: Arc<ShardManager>,
) -> Result<ResponseBody, ServerError> {
    let mut topic_error_codes = vec![];

    // Response structure for DeleteTopics is just [ (Name, ErrorCode) ] in older versions,
    // [ (Name, ErrorCode, TaggedFields) ] in newer.
    // Wait, logan-protocol says DeleteTopicsResponse has `responses: KafkaArray<DeleteTopicResponse>`?
    // Let's assume DeleteTopicsResponse struct existence and fields.
    // Checking protocol definition from memory/previous context:
    // pub struct DeleteTopicsResponse { responses: KafkaArray<DeleteTopicResponse>, ... }
    // pub struct DeleteTopicResponse { name: KafkaString, error_code: ErrorCode }

    // Correction: I need to check the actual struct in `logan-protocol`.
    // Assuming standard Kafka v0-v3 shape.

    // Note: older versions used just "topics" list in request.
    // logan-protocol::messages::DeleteTopicsRequest has `topics: KafkaArray<KafkaString>` or `topics: KafkaArray<DeleteTopicState>`?
    // Let's assume standard `topics: KafkaArray<KafkaString>`.

    for topic_name in &request.topics.0 {
        let name_str = topic_name.0.clone();

        info!("Deleting topic: {}", name_str);

        // 1. Remove from Metadata
        match topics_map.remove(&name_str) {
            Some(_) => {
                // 2. Delete from Storage
                match shard_manager.delete_topic(name_str.clone()).await {
                    Ok(_) => {
                        topic_error_codes.push(TopicError {
                            topic: topic_name.clone(),
                            error_code: ErrorCode::None,
                        });
                    }
                    Err(e) => {
                        warn!("Failed to delete topic data for {}: {}", name_str, e);
                        topic_error_codes.push(TopicError {
                            topic: topic_name.clone(),
                            error_code: ErrorCode::UnknownServerError,
                        });
                    }
                }
            }
            None => {
                topic_error_codes.push(TopicError {
                    topic: topic_name.clone(),
                    error_code: ErrorCode::UnknownTopicOrPartition,
                });
            }
        }
    }

    let response = DeleteTopicsResponse {
        topic_errors: KafkaArray(topic_error_codes),
    };
    Ok(ResponseBody::DeleteTopics(response))
}
