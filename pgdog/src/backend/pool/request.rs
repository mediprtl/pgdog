use tokio::time::Instant;

use crate::net::messages::BackendKeyData;

/// Connection request.
#[derive(Clone, Debug, Copy)]
pub struct Request {
    pub id: BackendKeyData,
    pub created_at: Instant,
    pub read: bool,
    /// Stable per-client index used by the `ClientAffinity` load balancing
    /// strategy to pin the client to one replica. Ignored by other strategies.
    pub replica_affinity: Option<usize>,
}

impl Request {
    pub fn new(id: BackendKeyData, read: bool) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read,
            replica_affinity: None,
        }
    }

    pub fn unrouted(id: BackendKeyData) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read: false,
            replica_affinity: None,
        }
    }

    /// Pin replica selection to a stable per-client index (used by the
    /// `ClientAffinity` strategy).
    pub fn with_replica_affinity(mut self, index: usize) -> Self {
        self.replica_affinity = Some(index);
        self
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::unrouted(BackendKeyData::new())
    }
}
