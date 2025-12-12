use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use data_types::{DataBlobGuid, TraceId};
use futures_util::stream::{FuturesUnordered, StreamExt};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use std::future::Future;
use std::pin::Pin;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

use self::usage::Usage;
use crate::results::WorkerResult;

mod usage;
mod user_input;

pub type Handle = JoinHandle<anyhow::Result<WorkerResult>>;

const TEST_BUCKET_ROOT_BLOB_NAME: &str = "12345678-1234567890abcdef-0001-1234";
const INODE_SIZE: usize = 187;
const BLOB_SIZE: usize = 8192;

fn read_keys(filename: &str, num_tasks: usize, keys_limit: usize) -> Vec<VecDeque<String>> {
    let file = File::open(filename).unwrap_or_else(|_| panic!("open {filename} failed"));
    let mut res = vec![VecDeque::new(); num_tasks];
    let mut i = 0;
    let mut total = 0;
    for line in BufReader::new(file).lines() {
        if let Ok(line) = line {
            res[i].push_back(line);
            i = (i + 1) % num_tasks;
            total += 1;
        }
        if total >= keys_limit {
            break;
        }
    }
    res
}

pub async fn start_tasks_for_nss(
    time_for: Duration,
    keys_limit: usize,
    connections: usize,
    uri_string: String,
    io_depth: usize,
    input: String,
    workload: String,
) -> anyhow::Result<FuturesUnordered<Handle>> {
    let deadline = Instant::now() + time_for;

    let handles = FuturesUnordered::new();

    println!("Fetching keys from {input} for {connections} connections, io_depth={io_depth}");
    let mut gen_keys = read_keys(&input, connections, keys_limit)
        .into_iter()
        .collect::<Vec<_>>();

    for _i in 0..connections {
        let keys = gen_keys.pop().unwrap();
        let connector = RewrkConnector::new(deadline, uri_string.clone());
        let rpc_client = connector.connect_nss().await.unwrap();
        let workload = workload.clone();
        let handle = match workload.as_str() {
            "read" => tokio::spawn(benchmark_nss_read(deadline, rpc_client, keys, io_depth)),
            _ => tokio::spawn(benchmark_nss_write(deadline, rpc_client, keys, io_depth)),
        };

        handles.push(handle);
    }

    Ok(handles)
}

pub async fn start_tasks_for_bss(
    time_for: Duration,
    keys_limit: usize,
    connections: usize,
    uri_string: String,
    io_depth: usize,
    input: String,
    workload: String,
) -> anyhow::Result<FuturesUnordered<Handle>> {
    let deadline = Instant::now() + time_for;

    let handles = FuturesUnordered::new();

    println!("Fetching uuids from {input} for {connections} connections, io_depth={io_depth}");
    let mut gen_uuids = read_keys(&input, connections, keys_limit)
        .into_iter()
        .collect::<Vec<_>>();

    for _i in 0..connections {
        let uuids = gen_uuids.pop().unwrap();
        let connector = RewrkConnector::new(deadline, uri_string.clone());
        let rpc_client = connector.connect_bss().await.unwrap();
        let workload = workload.clone();
        let handle = match workload.as_str() {
            "read" => tokio::spawn(benchmark_bss_read(deadline, rpc_client, uuids, io_depth)),
            "write" => tokio::spawn(benchmark_bss_write(deadline, rpc_client, uuids, io_depth)),
            _ => unimplemented!(),
        };

        handles.push(handle);
    }

    Ok(handles)
}

// Futures must not be awaited without timeout.
async fn benchmark_nss_read(
    deadline: Instant,
    rpc_client: Arc<RpcClientNss>,
    mut keys: VecDeque<String>,
    io_depth: usize,
) -> anyhow::Result<WorkerResult> {
    let benchmark_start = Instant::now();
    let mut request_times = Vec::new();
    let mut error_map = HashMap::new();

    let mut in_flight_requests = FuturesUnordered::<
        Pin<Box<dyn Future<Output = (Instant, anyhow::Result<()>)> + Send + 'static>>,
    >::new();

    // Fill the in-flight requests up to io_depth
    for _ in 0..io_depth {
        if let Some(key) = keys.pop_front() {
            let rpc_client = rpc_client.clone();
            let future = async move {
                let request_start = Instant::now();
                let trace_id = TraceId::new();
                let result = rpc_client
                    .get_inode(TEST_BUCKET_ROOT_BLOB_NAME, &key, None, &trace_id, 0)
                    .await
                    .map(|_| ())
                    .map_err(|e| anyhow::anyhow!(e));
                (request_start, result)
            };
            in_flight_requests.push(Box::pin(future));
        } else {
            break; // No more keys to process
        }
    }

    // Benchmark loop.
    while let Ok(Some((request_start, result))) =
        timeout_at(deadline, in_flight_requests.next()).await
    {
        if let Err(e) = result {
            let error = e.to_string();

            // Insert/add error string to error log.
            match error_map.get_mut(&error) {
                Some(count) => *count += 1,
                None => {
                    error_map.insert(error, 1);
                }
            }
        } else {
            request_times.push(request_start.elapsed());
        }

        // If there are more keys, add a new request to maintain io_depth
        if let Some(key) = keys.pop_front() {
            let rpc_client = rpc_client.clone();
            let future = async move {
                let request_start = Instant::now();
                let trace_id = TraceId::new();
                let result = rpc_client
                    .get_inode(TEST_BUCKET_ROOT_BLOB_NAME, &key, None, &trace_id, 0)
                    .await
                    .map(|_| ())
                    .map_err(|e| anyhow::anyhow!(e));
                (request_start, result)
            };
            in_flight_requests.push(Box::pin(future));
        } else if in_flight_requests.is_empty() {
            // If no more keys and no more in-flight requests, break
            break;
        }
    }

    Ok(WorkerResult {
        total_times: vec![benchmark_start.elapsed()],
        request_times,
        error_map,
    })
}

// Futures must not be awaited without timeout.
async fn benchmark_bss_write(
    deadline: Instant,
    rpc_client: Arc<RpcClientBss>,
    mut uuids: VecDeque<String>,
    io_depth: usize,
) -> anyhow::Result<WorkerResult> {
    let benchmark_start = Instant::now();
    let mut request_times = Vec::new();
    let mut error_map = HashMap::new();

    let mut in_flight_requests = FuturesUnordered::<
        Pin<Box<dyn Future<Output = (Instant, anyhow::Result<()>)> + Send + 'static>>,
    >::new();

    // Fill the in-flight requests up to io_depth
    for _ in 0..io_depth {
        if let Some(uuid) = uuids.pop_front() {
            let rpc_client = rpc_client.clone();
            let content = Bytes::from(vec![0; BLOB_SIZE - 256]);
            let body_checksum = xxhash_rust::xxh3::xxh3_64(&content);
            let blob_guid = DataBlobGuid {
                blob_id: Uuid::parse_str(&uuid).unwrap(),
                volume_id: 1,
            };
            in_flight_requests.push(Box::pin(async move {
                let request_start = Instant::now();
                let trace_id = TraceId::new();
                let result = rpc_client
                    .put_data_blob(blob_guid, 0, content, body_checksum, None, &trace_id, 0)
                    .await
                    .map(|_| ()) // Map Ok(usize) to Ok(())
                    .map_err(|e| anyhow::anyhow!(e)); // Convert RpcErrorBss to anyhow::Error
                (request_start, result)
            }));
        } else {
            break; // No more UUIDs to process
        }
    }

    // Benchmark loop.
    while let Ok(Some((request_start, result))) =
        timeout_at(deadline, in_flight_requests.next()).await
    {
        if let Err(e) = result {
            let error = e.to_string();

            // Insert/add error string to error log.
            match error_map.get_mut(&error) {
                Some(count) => *count += 1,
                None => {
                    error_map.insert(error, 1);
                }
            }
        } else {
            request_times.push(request_start.elapsed());
        }

        // If there are more UUIDs, add a new request to maintain io_depth
        if let Some(uuid) = uuids.pop_front() {
            let rpc_client = rpc_client.clone();
            let content = Bytes::from(vec![0; BLOB_SIZE - 256]);
            let body_checksum = xxhash_rust::xxh3::xxh3_64(&content);
            let blob_guid = DataBlobGuid {
                blob_id: Uuid::parse_str(&uuid).unwrap(),
                volume_id: 1,
            };
            in_flight_requests.push(Box::pin(async move {
                let request_start = Instant::now();
                let trace_id = TraceId::new();
                let result = rpc_client
                    .put_data_blob(blob_guid, 0, content, body_checksum, None, &trace_id, 0)
                    .await
                    .map(|_| ()) // Map Ok(usize) to Ok(())
                    .map_err(|e| anyhow::anyhow!(e)); // Convert RpcErrorBss to anyhow::Error
                (request_start, result)
            }));
        } else if in_flight_requests.is_empty() {
            // If no more UUIDs and no more in-flight requests, break
            break;
        }
    }

    Ok(WorkerResult {
        total_times: vec![benchmark_start.elapsed()],
        request_times,
        error_map,
    })
}

// Futures must not be awaited without timeout.
async fn benchmark_bss_read(
    deadline: Instant,
    rpc_client: Arc<RpcClientBss>,
    mut uuids: VecDeque<String>,
    io_depth: usize,
) -> anyhow::Result<WorkerResult> {
    let benchmark_start = Instant::now();
    let mut request_times = Vec::new();
    let mut error_map = HashMap::new();

    let mut in_flight_requests = FuturesUnordered::<
        Pin<Box<dyn Future<Output = (Instant, anyhow::Result<Bytes>)> + Send + 'static>>,
    >::new();

    // Fill the in-flight requests up to io_depth
    for _ in 0..io_depth {
        if let Some(uuid) = uuids.pop_front() {
            let rpc_client = rpc_client.clone();
            in_flight_requests.push(Box::pin(async move {
                let request_start = Instant::now();
                let trace_id = TraceId::new();
                let blob_guid = DataBlobGuid {
                    blob_id: Uuid::parse_str(&uuid).unwrap(),
                    volume_id: 1,
                };
                let mut content = Bytes::new();
                let result = rpc_client
                    .get_data_blob(
                        blob_guid,
                        0,
                        &mut content,
                        BLOB_SIZE - 256,
                        None,
                        &trace_id,
                        0,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e)); // Convert RpcErrorBss to anyhow::Error
                (request_start, result.map(|_| content))
            }));
        } else {
            break; // No more UUIDs to process
        }
    }

    // Benchmark loop.
    while let Ok(Some((request_start, result))) =
        timeout_at(deadline, in_flight_requests.next()).await
    {
        if let Err(e) = result {
            let error = e.to_string();

            // Insert/add error string to error log.
            match error_map.get_mut(&error) {
                Some(count) => *count += 1,
                None => {
                    error_map.insert(error, 1);
                }
            }
        } else {
            request_times.push(request_start.elapsed());
        }

        // If there are more UUIDs, add a new request to maintain io_depth
        if let Some(uuid) = uuids.pop_front() {
            let rpc_client = rpc_client.clone();
            in_flight_requests.push(Box::pin(async move {
                let request_start = Instant::now();
                let trace_id = TraceId::new();
                let blob_guid = DataBlobGuid {
                    blob_id: Uuid::parse_str(&uuid).unwrap(),
                    volume_id: 1,
                };
                let mut content = Bytes::new();
                let result = rpc_client
                    .get_data_blob(
                        blob_guid,
                        0,
                        &mut content,
                        BLOB_SIZE - 256,
                        None,
                        &trace_id,
                        0,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e)); // Convert RpcErrorBss to anyhow::Error
                (request_start, result.map(|_| content))
            }));
        } else if in_flight_requests.is_empty() {
            // If no more UUIDs and no more in-flight requests, break
            break;
        }
    }

    Ok(WorkerResult {
        total_times: vec![benchmark_start.elapsed()],
        request_times,
        error_map,
    })
}
async fn benchmark_nss_write(
    deadline: Instant,
    rpc_client: Arc<RpcClientNss>,
    mut keys: VecDeque<String>,
    io_depth: usize,
) -> anyhow::Result<WorkerResult> {
    let benchmark_start = Instant::now();
    let mut request_times = Vec::new();
    let mut error_map = HashMap::new();

    let mut in_flight_requests = FuturesUnordered::<
        Pin<Box<dyn Future<Output = (Instant, anyhow::Result<()>)> + Send + 'static>>,
    >::new();

    // Fill the in-flight requests up to io_depth
    for _ in 0..io_depth {
        if let Some(key) = keys.pop_front() {
            let rpc_client = rpc_client.clone();
            let value = Bytes::from(vec![b'i'; INODE_SIZE]);
            in_flight_requests.push(Box::pin(async move {
                let request_start = Instant::now();
                let trace_id = TraceId::new();
                let result = rpc_client
                    .put_inode(TEST_BUCKET_ROOT_BLOB_NAME, &key, value, None, &trace_id, 0)
                    .await
                    .map(|_| ()) // Map Ok(PutInodeResponse) to Ok(())
                    .map_err(|e| anyhow::anyhow!(e)); // Convert RpcErrorNss to anyhow::Error
                (request_start, result)
            }));
        } else {
            break; // No more keys to process
        }
    }

    // Benchmark loop.
    while let Ok(Some((request_start, result))) =
        timeout_at(deadline, in_flight_requests.next()).await
    {
        if let Err(e) = result {
            let error = e.to_string();

            // Insert/add error string to error log.
            match error_map.get_mut(&error) {
                Some(count) => *count += 1,
                None => {
                    error_map.insert(error, 1);
                }
            }
        } else {
            request_times.push(request_start.elapsed());
        }

        // If there are more keys, add a new request to maintain io_depth
        if let Some(key) = keys.pop_front() {
            let rpc_client = rpc_client.clone();
            let value = Bytes::from(vec![b'i'; INODE_SIZE]);
            in_flight_requests.push(Box::pin(async move {
                let request_start = Instant::now();
                let trace_id = TraceId::new();
                let result = rpc_client
                    .put_inode(TEST_BUCKET_ROOT_BLOB_NAME, &key, value, None, &trace_id, 0)
                    .await
                    .map(|_| ()) // Map Ok(PutInodeResponse) to Ok(())
                    .map_err(|e| anyhow::anyhow!(e)); // Convert RpcErrorNss to anyhow::Error
                (request_start, result)
            }));
        } else if in_flight_requests.is_empty() {
            // If no more keys and no more in-flight requests, break
            break;
        }
    }

    Ok(WorkerResult {
        total_times: vec![benchmark_start.elapsed()],
        request_times,
        error_map,
    })
}

struct RewrkConnector {
    #[allow(unused)]
    deadline: Instant,
    host: String,
    #[allow(unused)]
    usage: Usage,
}

impl RewrkConnector {
    fn new(deadline: Instant, host: String) -> Self {
        let usage = Usage::new();

        Self {
            deadline,
            host,
            usage,
        }
    }

    async fn connect_nss(&self) -> anyhow::Result<Arc<RpcClientNss>> {
        Ok(
            RpcClientNss::new_from_address(self.host.clone(), std::time::Duration::from_secs(5))
                .into(),
        )
    }

    async fn connect_bss(&self) -> anyhow::Result<Arc<RpcClientBss>> {
        Ok(
            RpcClientBss::new_from_address(self.host.clone(), std::time::Duration::from_secs(5))
                .into(),
        )
    }

    #[allow(dead_code)]
    fn get_received_bytes(&self) -> usize {
        self.usage.get_received_bytes()
    }
}
