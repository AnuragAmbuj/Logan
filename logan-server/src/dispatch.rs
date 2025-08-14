use crate::error::ServerError;
use crate::server::TopicMessageStore;
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use logan_protocol::api_keys::ApiKey;
use logan_protocol::codec::{Decodable, Encodable};
use logan_protocol::error_codes::ErrorCode;
use logan_protocol::messages::*;
use logan_protocol::primitives::{KafkaArray, KafkaString, NullableBytes};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    messages: Arc<TopicMessageStore>,
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
            handle_produce_request(request, messages).await
        }
        ApiKey::Fetch => {
            let request = FetchRequest::decode(&mut request_buf)?;
            handle_fetch_request(request, messages).await
        }
        _ => {
            warn!("Unsupported API key: {:?}", header.api_key);
            return Ok(true); // Keep connection open
        }
    }?; // Use ? to propagate errors

    let response_bytes = encode_response(&header, &response_body)?;
    stream.write_all(&response_bytes).await?;
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

    let response = CreateTopicsResponse { topic_errors: KafkaArray(topic_errors) };
    Ok(ResponseBody::CreateTopics(response))
}

fn handle_list_offsets_request(
    _request: &ListOffsetsRequest,
) -> Result<ResponseBody, ServerError> {
    // Simplified response for now
    let response = ListOffsetsResponse {
        throttle_time_ms: 0,
        topics: KafkaArray(vec![]),
    };
    Ok(ResponseBody::ListOffsets(response))
}

async fn handle_produce_request(
    request: ProduceRequest,
    messages: Arc<TopicMessageStore>,
) -> Result<ResponseBody, ServerError> {
    let mut topic_responses = vec![];

    for topic_data in request.topic_data.0 {
        let topic_name = topic_data.name.0.clone();
        let mut partition_responses = vec![];

        let topic_storage = messages.entry(topic_name.clone()).or_default();

        for partition_data in topic_data.partition_data.0 {
            let partition_index = partition_data.index;
            let partition_storage = topic_storage.entry(partition_index).or_default();
            let mut records = partition_storage.lock().await;

            let base_offset = records.len() as i64;
            records.extend_from_slice(&partition_data.record_set.0);

            partition_responses.push(PartitionProduceResponse {
                index: partition_index,
                error_code: ErrorCode::None,
                base_offset,
            });
        }

        topic_responses.push(TopicProduceResponse {
            name: topic_data.name,
            partition_responses: KafkaArray(partition_responses),
        });
    }

    let response = ProduceResponse { responses: KafkaArray(topic_responses) };
    Ok(ResponseBody::Produce(response))
}

async fn handle_fetch_request(
    request: FetchRequest,
    messages: Arc<TopicMessageStore>,
) -> Result<ResponseBody, ServerError> {
    let mut topic_responses = vec![];

    for fetch_topic in request.topics.0 {
        let mut partition_responses = vec![];
        let topic_name = fetch_topic.topic.0.clone();

        for fetch_partition in fetch_topic.partitions.0 {
            let partition_index = fetch_partition.partition_index;
            let mut records = None;
            let error_code = if let Some(partitions) = messages.get(&topic_name) {
                if let Some(messages) = partitions.get(&partition_index) {
                    let locked_messages = messages.lock().await;
                    let mut records_buf = BytesMut::with_capacity(locked_messages.len());
                    records_buf.extend_from_slice(&locked_messages);
                    records = Some(NullableBytes(Some(records_buf.freeze())));
                    ErrorCode::None
                } else {
                    ErrorCode::UnknownTopicOrPartition
                }
            } else {
                ErrorCode::UnknownTopicOrPartition
            };

            partition_responses.push(PartitionData {
                partition_index,
                error_code: error_code as i16,
                high_watermark: 0, // Not implemented
                records: records.unwrap_or_default(),
            });
        }

        topic_responses.push(FetchableTopicResponse {
            topic: KafkaString(topic_name),
            partitions: KafkaArray(partition_responses),
        });
    }

    let response = FetchResponse { responses: KafkaArray(topic_responses) };
    Ok(ResponseBody::Fetch(response))
}

fn encode_response(
    header: &RequestHeader,
    body: &ResponseBody,
) -> anyhow::Result<BytesMut> {
    let mut buf = BytesMut::new();

    let response_header = ResponseHeader { correlation_id: header.correlation_id };
    response_header.encode(&mut buf)?;
    body.encode(&mut buf)?;

    let mut final_buf = BytesMut::with_capacity(buf.len() + 4);
    (buf.len() as i32).encode(&mut final_buf)?;
    final_buf.put(buf);

    Ok(final_buf)
}
