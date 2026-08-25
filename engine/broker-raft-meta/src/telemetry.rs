//! Read-only controller telemetry snapshot.

use crate::ClusterRuntime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerTelemetrySnapshot {
    pub term: u64,
    pub quorum_ready: bool,
    pub configured_nodes: usize,
    pub required_quorum: usize,
}

pub fn controller_telemetry_snapshot(runtime: &ClusterRuntime) -> ControllerTelemetrySnapshot {
    let config = runtime.config();
    ControllerTelemetrySnapshot {
        term: runtime.controller_term(),
        quorum_ready: runtime.has_controller_lease(),
        configured_nodes: config.node_count(),
        required_quorum: config.quorum_size(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterConfig, NodeConfig};
    use uuid::Uuid;

    #[test]
    fn snapshot_tracks_controller_term_and_readiness() {
        let node_id = Uuid::new_v4();
        let runtime = ClusterRuntime::from_config_only(ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![NodeConfig {
                id: node_id,
                addr: "http://127.0.0.1:8080".into(),
            }],
            node_id,
            generation: 1,
            hash_version: 1,
        });
        let initial = controller_telemetry_snapshot(&runtime);
        assert!(initial.quorum_ready);
        assert_eq!(initial.required_quorum, 1);
        let term = runtime.campaign_controller().unwrap();
        let after = controller_telemetry_snapshot(&runtime);
        assert_eq!(after.term, term);
        assert!(after.quorum_ready);
    }
}
