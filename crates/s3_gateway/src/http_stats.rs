use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpEndpointType {
    GetObj,
    PutObj,
    DeleteObj,
    Other,
}

impl HttpEndpointType {
    pub fn as_str(&self) -> &'static str {
        match self {
            HttpEndpointType::GetObj => "get_obj",
            HttpEndpointType::PutObj => "put_obj",
            HttpEndpointType::DeleteObj => "delete_obj",
            HttpEndpointType::Other => "other",
        }
    }

    pub fn all() -> [HttpEndpointType; 4] {
        [
            HttpEndpointType::GetObj,
            HttpEndpointType::PutObj,
            HttpEndpointType::DeleteObj,
            HttpEndpointType::Other,
        ]
    }

    pub fn from_endpoint_name(name: &str) -> Self {
        match name {
            "GetObject" => HttpEndpointType::GetObj,
            "PutObject"
            | "CompleteMultipartUpload"
            | "CreateMultipartUpload"
            | "UploadPart"
            | "CopyObject" => HttpEndpointType::PutObj,
            "DeleteObject" | "AbortMultipartUpload" => HttpEndpointType::DeleteObj,
            _ => HttpEndpointType::Other,
        }
    }
}

pub struct HttpStats {
    get_obj: AtomicU64,
    put_obj: AtomicU64,
    delete_obj: AtomicU64,
    other: AtomicU64,
}

impl Default for HttpStats {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpStats {
    pub fn new() -> Self {
        Self {
            get_obj: AtomicU64::new(0),
            put_obj: AtomicU64::new(0),
            delete_obj: AtomicU64::new(0),
            other: AtomicU64::new(0),
        }
    }

    pub fn increment(&self, endpoint: HttpEndpointType) {
        match endpoint {
            HttpEndpointType::GetObj => self.get_obj.fetch_add(1, Ordering::Relaxed),
            HttpEndpointType::PutObj => self.put_obj.fetch_add(1, Ordering::Relaxed),
            HttpEndpointType::DeleteObj => self.delete_obj.fetch_add(1, Ordering::Relaxed),
            HttpEndpointType::Other => self.other.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn decrement(&self, endpoint: HttpEndpointType) {
        match endpoint {
            HttpEndpointType::GetObj => self.get_obj.fetch_sub(1, Ordering::Relaxed),
            HttpEndpointType::PutObj => self.put_obj.fetch_sub(1, Ordering::Relaxed),
            HttpEndpointType::DeleteObj => self.delete_obj.fetch_sub(1, Ordering::Relaxed),
            HttpEndpointType::Other => self.other.fetch_sub(1, Ordering::Relaxed),
        };
    }

    pub fn get_count(&self, endpoint: HttpEndpointType) -> u64 {
        match endpoint {
            HttpEndpointType::GetObj => self.get_obj.load(Ordering::Relaxed),
            HttpEndpointType::PutObj => self.put_obj.load(Ordering::Relaxed),
            HttpEndpointType::DeleteObj => self.delete_obj.load(Ordering::Relaxed),
            HttpEndpointType::Other => self.other.load(Ordering::Relaxed),
        }
    }
}

static GLOBAL_HTTP_STATS: OnceLock<HttpStats> = OnceLock::new();

pub fn get_global_http_stats() -> &'static HttpStats {
    GLOBAL_HTTP_STATS.get_or_init(HttpStats::new)
}

pub struct HttpStatsGuard {
    endpoint: HttpEndpointType,
}

impl HttpStatsGuard {
    pub fn new(endpoint_name: &str) -> Self {
        let endpoint = HttpEndpointType::from_endpoint_name(endpoint_name);
        let stats = get_global_http_stats();
        stats.increment(endpoint);
        Self { endpoint }
    }
}

impl Drop for HttpStatsGuard {
    fn drop(&mut self) {
        let stats = get_global_http_stats();
        stats.decrement(self.endpoint);
    }
}
