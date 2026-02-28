mod api_key;
mod blob_guid;
mod bucket;
pub mod hash;
pub mod object_layout;
pub mod observer_types;
mod permission;
mod trace_id;
mod volume;

// Re-export the main types for convenience
pub use api_key::ApiKey;
pub use blob_guid::{DataBlobGuid, MetaBlobGuid};
pub use bucket::Bucket;
pub use permission::BucketKeyPerm;
pub use trace_id::TraceId;
pub use volume::{BssNode, DataVgInfo, Volume, VolumeMode};

pub use observer_types::{
    HealthCheckResponse, MachineState, ObserverPersistentState, ObserverState, ServiceStatus,
    ServiceType,
};

#[derive(Clone)]
pub struct Versioned<T: Sized> {
    pub version: i64,
    pub data: T,
}

impl<T: Sized> Versioned<T> {
    pub fn new(version: i64, data: T) -> Self {
        Self { version, data }
    }
}

impl<T: Sized> From<(i64, T)> for Versioned<T> {
    fn from(value: (i64, T)) -> Self {
        Self {
            version: value.0,
            data: value.1,
        }
    }
}
