use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use futures::AsyncReadExt;
use futures::AsyncWriteExt;
use glommio::{net::TcpListener, LocalExecutorBuilder, Placement};
use logan_protocol::{
    api_keys::ApiKey,
    codec::Decodable,
    codec::Encodable,
    error_codes::ErrorCode,
    messages::*,
    primitives::{KafkaArray, KafkaBool, KafkaBytes, KafkaString, NullableString},
    RequestHeader, ResponseHeader,
};
use std::rc::Rc;
use num_cpus;

fn main() -> Result<()> {
    // Spawn one executor per core (simplification: just 1 core for now to test vs 1 thread tokio, or use parallelism)
    // To match benchmarking setup, we want to use all cores.
    // Glommio's typical pattern: spawn threads, each runs a LocalExecutor.

    // Spawn one executor per core
    let handles: Vec<_> = (0..num_cpus::get())
        .map(|cpu| {
            LocalExecutorBuilder::new(Placement::Fixed(cpu))
                .name(&format!("executor-{}", cpu))
                .spawn(move || async move {
                    let listener = TcpListener::bind("0.0.0.0:9093").expect("Failed to bind");
                    println!("Started listener on core {}", cpu);
                    
                    loop {
                        match listener.accept().await {
                            Ok(mut stream) => {
                                glommio::spawn_local(async move {
                                    // Basic handling loop
                                    let mut buf = BytesMut::with_capacity(1024);
                                    let mut size_buf = [0u8; 4];
                                    
                                    loop {
                                        // Read length
                                        if let Err(_) = stream.read_exact(&mut size_buf).await {
                                            break; 
                                        }
                                        let size = i32::from_be_bytes(size_buf);
                                        // Sanity check size
                                        if size <= 0 || size > 10 * 1024 * 1024 {
                                            break;
                                        }

                                        buf.resize(size as usize, 0);
                                        if let Err(_) = stream.read_exact(&mut buf).await {
                                            break;
                                        }

                                        let mut cursor = Bytes::copy_from_slice(&buf); // Expensive copy? Glommio IO might avoid this if we used DmaFile
                                        
                                        // Parse Header
                                        if let Ok(header) = RequestHeader::decode(&mut cursor) {
                                            // Handle Produce Request
                                            if header.api_key == ApiKey::Produce {
                                                 if let Ok(req) = ProduceRequest::decode(&mut cursor) {
                                                     // Simulate write (no-op or memory append)
                                                     
                                                    // Construct Response
                                                    let resp = ProduceResponse {
                                                        responses: KafkaArray(req.topic_data.0.into_iter().map(|td| {
                                                            TopicProduceResponse {
                                                                name: td.name,
                                                                partition_responses: KafkaArray(td.partition_data.0.into_iter().map(|pd| {
                                                                    PartitionProduceResponse {
                                                                        index: pd.index,
                                                                        error_code: ErrorCode::None,
                                                                        base_offset: 1234, // Mock
                                                                    }
                                                                }).collect())
                                                            }
                                                        }).collect()),
                                                        throttle_time_ms: 0,
                                                    };
                                                     
                                                     let mut resp_buf = BytesMut::with_capacity(1024);
                                                     // Reserve space for size
                                                     resp_buf.put_u32(0); 
                                                     
                                                     let resp_header = ResponseHeader { correlation_id: header.correlation_id };
                                                     if resp_header.encode(&mut resp_buf).is_ok() && resp.encode(&mut resp_buf).is_ok() {
                                                         let len = resp_buf.len() - 4;
                                                         let len_bytes = (len as i32).to_be_bytes();
                                                         resp_buf[0..4].copy_from_slice(&len_bytes);
                                                         
                                                         let _ = stream.write_all(&resp_buf).await;
                                                     }
                                                 }
                                            } else if header.api_key == ApiKey::ApiVersions {
                                                // Handle ApiVersions for client handshake
                                                let resp = ApiVersionsResponse {
                                                    error_code: 0,
                                                    api_versions: KafkaArray(vec![]),
                                                };
                                                // ... (simplification: ignoring handshake for benchmark efficiency if client supports it, but client sends it)
                                                 let mut resp_buf = BytesMut::with_capacity(1024);
                                                 resp_buf.put_u32(0);
                                                 let resp_header = ResponseHeader { correlation_id: header.correlation_id };
                                                 resp_header.encode(&mut resp_buf).unwrap();
                                                 resp.encode(&mut resp_buf).unwrap();
                                                 let len = (resp_buf.len() - 4) as i32;
                                                 resp_buf[0..4].copy_from_slice(&len.to_be_bytes());
                                                 let _ = stream.write_all(&resp_buf).await;
                                            }
                                        }
                                        
                                        // Reset buf ??
                                    }
                                }).detach();
                            }
                            Err(e) => {
                                eprintln!("Accept error: {}", e);
                            }
                        }
                    }
                }).expect("Failed to spawn executor")
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    
    Ok(())
}
