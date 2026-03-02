use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceId(pub u64);

impl TraceId {
    pub fn new() -> Self {
        Self::new_with_worker_id(0xff)
    }

    pub fn new_with_worker_id(worker_id: u8) -> Self {
        let random_value: u64 = rand::random();
        let trace_id = (random_value & 0xFFFFFFFFFFFFFF00) | (worker_id as u64);
        Self(trace_id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn worker_id(&self) -> u8 {
        (self.0 & 0xFF) as u8
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl From<u64> for TraceId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<TraceId> for u64 {
    fn from(trace_id: TraceId) -> Self {
        trace_id.0
    }
}
