//! Sticky settings for clients that override
//! default routing behavior determined by the query parser.

use std::hash::{Hash, Hasher};

use fnv::FnvHasher;
use pgdog_config::Role;
use rand::{rng, Rng};

use crate::net::{parameter::ParameterValue, Parameters};

#[derive(Debug, Clone, Copy)]
pub struct Sticky {
    /// Which shard to use for omnisharded queries, making them
    /// stick to only one database.
    pub omni_index: usize,

    /// Stable per-client index used by the `ClientAffinity` load balancing
    /// strategy to pin this connection to a single replica for its lifetime.
    pub replica_index: usize,

    /// Desired database role. This comes from `target_session_attrs`
    /// provided by the client.
    pub role: Option<Role>,
}

impl Default for Sticky {
    fn default() -> Self {
        Self::new()
    }
}

impl Sticky {
    /// Create new sticky config.
    pub fn new() -> Self {
        Self::from_params(&Parameters::default())
    }

    #[cfg(test)]
    pub fn new_test() -> Self {
        Self {
            omni_index: 1,
            replica_index: 1,
            role: None,
        }
    }

    /// Create Sticky from params.
    pub fn from_params(params: &Parameters) -> Self {
        let role = params.get("pgdog.role").and_then(|value| match value {
            ParameterValue::String(value) => match value.as_str() {
                "primary" => Some(Role::Primary),
                "replica" => Some(Role::Replica),
                _ => None,
            },
            _ => None,
        });

        // Clients sharing a `pgdog.replica_affinity_key` (e.g. all connections
        // from one pod) hash to the same index and therefore pin to the same
        // replica under `ClientAffinity`. Without the key each connection gets
        // its own random index — per-connection affinity.
        let replica_index = params
            .get("pgdog.replica_affinity_key")
            .and_then(|value| value.as_str())
            .map(replica_index_from_key)
            .unwrap_or_else(|| rng().random_range(1..usize::MAX));

        Self {
            omni_index: rng().random_range(1..usize::MAX),
            replica_index,
            role,
        }
    }
}

/// Map a client-supplied affinity key to a stable replica index.
///
/// FNV has a fixed seed, so the mapping is identical across PgDog processes —
/// a client's connections pin to the same replica even when they land on
/// different PgDog instances behind a load balancer.
fn replica_index_from_key(key: &str) -> usize {
    let mut hasher = FnvHasher::default();
    key.hash(&mut hasher);
    hasher.finish() as usize
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sticky() {
        let params = Parameters::default();
        assert!(Sticky::from_params(&params).role.is_none());

        for (attr, role) in [
            ("primary", Some(Role::Primary)),
            ("replica", Some(Role::Replica)),
            ("random", None),
        ] {
            let mut params = Parameters::default();
            params.insert("pgdog.role", attr);
            let sticky = Sticky::from_params(&params);
            assert_eq!(sticky.role, role);
        }
    }

    fn sticky_with_key(key: &str) -> Sticky {
        let mut params = Parameters::default();
        params.insert("pgdog.replica_affinity_key", key);
        Sticky::from_params(&params)
    }

    #[test]
    fn test_replica_affinity_key_is_stable() {
        // Same key always maps to the same replica index — across connections
        // and (since FNV is fixed-seed) across PgDog processes.
        assert_eq!(
            sticky_with_key("pod-a").replica_index,
            sticky_with_key("pod-a").replica_index
        );
        // Distinct keys map to distinct indices.
        assert_ne!(
            sticky_with_key("pod-a").replica_index,
            sticky_with_key("pod-b").replica_index
        );
    }
}
