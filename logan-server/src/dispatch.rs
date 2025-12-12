use crate::error::ServerError;
use crate::shard::ShardManager;
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use logan_protocol::api_keys::ApiKey;
use logan_protocol::batch::RecordBatch;
use logan_protocol::codec::{Decodable, Encodable};
use logan_protocol::error_codes::ErrorCode;
use logan_protocol::messages::*;
use logan_protocol::primitives::{KafkaArray, NullableBytes};
use logan_storage::LogReadResult;
use std::fs::File as StdFile;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{info, warn};

pub(crate) enum ResponseBody {
    ApiVersions(ApiVersionsResponse),
    Metadata(MetadataResponse),
    CreateTopics(CreateTopicsResponse),
    ListOffsets(ListOffsetsResponse),
    Produce(ProduceResponse),
    Fetch(FetchResponse),
}

impl Encodable for ResponseBody {
    fn encode(&self, buf: &mut impl BufMut) -> anyhow::Result<()> {
        match self {
            ResponseBody::ApiVersions(res) => res.encode(buf),
            ResponseBody::Metadata(res) => res.encode(buf),
            ResponseBody::CreateTopics(res) => res.encode(buf),
            ResponseBody::ListOffsets(res) => res.encode(buf),
            ResponseBody::Produce(res) => res.encode(buf),
            ResponseBody::Fetch(res) => res.encode(buf),
        }
    }
}

pub(crate) async fn dispatch(
    stream: &mut TcpStream,
    topics: Arc<DashMap<String, CreatableTopic>>,
    shard_manager: Arc<ShardManager>,
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
            handle_api_versions_request(&request)
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

fn handle_api_versions_request(_request: &ApiVersionsRequest) -> Result<ResponseBody, ServerError> {
    let response = ApiVersionsResponse {
        error_code: 0,
        api_versions: KafkaArray(vec![
            ApiVersionResponse {
                api_key: ApiKey::ApiVersions,
                min_version: 0,
                max_version: 0,
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
        throttle_time_ms: 0,
        brokers: KafkaArray(vec![]), // Simplified for now
        cluster_id: Some("logan-cluster".to_string()).into(),
        controller_id: 1, // Simplified for now
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
    };
    Ok(ResponseBody::Produce(response))
}

fn encode_response(header: &RequestHeader, body: &ResponseBody) -> anyhow::Result<BytesMut> {
    let mut buf = BytesMut::new();

    let response_header = ResponseHeader {
        correlation_id: header.correlation_id,
    };
    response_header.encode(&mut buf)?;
    body.encode(&mut buf)?;

    let mut final_buf = BytesMut::with_capacity(buf.len() + 4);
    (buf.len() as i32).encode(&mut final_buf)?;
    final_buf.put(buf);

    Ok(final_buf)
}
