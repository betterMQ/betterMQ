//! Durable controller term/vote compatibility boundary.
//!
//! This is deliberately not presented as Raft: it has no replicated command
//! log. It does provide durable one-vote-per-term state and requires proof of
//! a membership quorum before a controller leader can be confirmed.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum EpochError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("stale term: have {have}, saw {saw}")]
    StaleTerm { have: u64, saw: u64 },
    #[error("term {term} was already voted for {voted_for}, not {candidate}")]
    VoteConflict {
        term: u64,
        voted_for: Uuid,
        candidate: Uuid,
    },
    #[error("term {term} already has leader {leader}, not {candidate}")]
    LeaderConflict {
        term: u64,
        leader: Uuid,
        candidate: Uuid,
    },
    #[error("controller term space exhausted")]
    TermExhausted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControllerVoteRequest {
    pub term: u64,
    pub candidate_id: Uuid,
    pub cluster_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControllerVoteResponse {
    pub term: u64,
    pub voter_id: Uuid,
    pub candidate_id: Uuid,
    pub granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControllerLeaderProof {
    pub term: u64,
    pub leader_id: Uuid,
    pub cluster_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ControllerEpoch {
    pub current_term: u64,
    pub voted_for: Option<Uuid>,
    pub leader_id: Option<Uuid>,
}

impl ControllerEpoch {
    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("controller-epoch.json")
    }

    pub fn load(data_dir: &Path) -> Result<Self, EpochError> {
        let path = Self::path(data_dir);
        if !path.exists() {
            return Ok(Self::default());
        }
        let bytes = std::fs::read(path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn store(&self, data_dir: &Path) -> Result<(), EpochError> {
        std::fs::create_dir_all(data_dir)?;
        let path = Self::path(data_dir);
        let bytes = serde_json::to_vec_pretty(self)?;
        broker_storage::atomic_write_file(&path, &bytes)?;
        Ok(())
    }

    /// Start a campaign and durably cast this node's self vote. This does not
    /// make the candidate leader; callers must gather and verify quorum votes.
    pub fn begin_campaign(&mut self, data_dir: &Path, node_id: Uuid) -> Result<u64, EpochError> {
        self.current_term = self
            .current_term
            .checked_add(1)
            .filter(|term| *term != u64::MAX)
            .ok_or(EpochError::TermExhausted)?;
        self.voted_for = Some(node_id);
        self.leader_id = None;
        self.store(data_dir)?;
        Ok(self.current_term)
    }

    /// Compatibility helper for a one-node controller only.
    pub fn become_leader(&mut self, data_dir: &Path, node_id: Uuid) -> Result<u64, EpochError> {
        let term = self.begin_campaign(data_dir, node_id)?;
        self.confirm_leader(data_dir, term, node_id)?;
        Ok(term)
    }

    /// Durably grant at most one candidate a vote in a term.
    pub fn grant_vote(
        &mut self,
        data_dir: &Path,
        term: u64,
        candidate: Uuid,
    ) -> Result<bool, EpochError> {
        if term < self.current_term {
            return Err(EpochError::StaleTerm {
                have: self.current_term,
                saw: term,
            });
        }
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
            self.leader_id = None;
        }
        if let Some(leader) = self.leader_id {
            if leader != candidate {
                return Err(EpochError::LeaderConflict {
                    term,
                    leader,
                    candidate,
                });
            }
        }
        match self.voted_for {
            Some(voted_for) if voted_for != candidate => {
                return Err(EpochError::VoteConflict {
                    term,
                    voted_for,
                    candidate,
                });
            }
            Some(_) => {}
            None => self.voted_for = Some(candidate),
        }
        self.store(data_dir)?;
        Ok(true)
    }

    /// Persist a leader only after the caller has validated a quorum proof.
    pub fn confirm_leader(
        &mut self,
        data_dir: &Path,
        term: u64,
        candidate: Uuid,
    ) -> Result<u64, EpochError> {
        if term < self.current_term {
            return Err(EpochError::StaleTerm {
                have: self.current_term,
                saw: term,
            });
        }
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = Some(candidate);
        }
        if let Some(voted_for) = self.voted_for {
            if voted_for != candidate {
                return Err(EpochError::VoteConflict {
                    term,
                    voted_for,
                    candidate,
                });
            }
        }
        if let Some(leader) = self.leader_id {
            if leader != candidate {
                return Err(EpochError::LeaderConflict {
                    term,
                    leader,
                    candidate,
                });
            }
        }
        self.voted_for = Some(candidate);
        self.leader_id = Some(candidate);
        self.store(data_dir)?;
        Ok(term)
    }

    pub fn observe_term(
        &mut self,
        data_dir: &Path,
        term: u64,
        leader: Uuid,
    ) -> Result<u64, EpochError> {
        if term < self.current_term {
            return Err(EpochError::StaleTerm {
                have: self.current_term,
                saw: term,
            });
        }
        if term == self.current_term {
            if let Some(voted_for) = self.voted_for {
                if voted_for != leader {
                    return Err(EpochError::VoteConflict {
                        term,
                        voted_for,
                        candidate: leader,
                    });
                }
            }
            if let Some(existing) = self.leader_id {
                if existing != leader {
                    return Err(EpochError::LeaderConflict {
                        term,
                        leader: existing,
                        candidate: leader,
                    });
                }
            }
        }
        self.current_term = term;
        self.voted_for = Some(leader);
        self.leader_id = Some(leader);
        self.store(data_dir)?;
        Ok(self.current_term)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn leader_term_is_monotonic_and_durable() {
        let dir = tempdir().unwrap();
        let id = Uuid::new_v4();
        let mut epoch = ControllerEpoch::default();
        let t1 = epoch.become_leader(dir.path(), id).unwrap();
        assert_eq!(t1, 1);
        let loaded = ControllerEpoch::load(dir.path()).unwrap();
        assert_eq!(loaded.current_term, 1);
        assert_eq!(loaded.leader_id, Some(id));
        let t2 = epoch.become_leader(dir.path(), id).unwrap();
        assert_eq!(t2, 2);
    }

    #[test]
    fn stale_term_is_rejected() {
        let dir = tempdir().unwrap();
        let mut epoch = ControllerEpoch {
            current_term: 5,
            voted_for: None,
            leader_id: None,
        };
        let err = epoch
            .observe_term(dir.path(), 3, Uuid::new_v4())
            .unwrap_err();
        assert!(matches!(err, EpochError::StaleTerm { have: 5, saw: 3 }));
    }

    #[test]
    fn grants_only_one_durable_vote_per_term() {
        let dir = tempdir().unwrap();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let mut epoch = ControllerEpoch::default();
        assert!(epoch.grant_vote(dir.path(), 4, a).unwrap());
        let err = epoch.grant_vote(dir.path(), 4, b).unwrap_err();
        assert!(matches!(err, EpochError::VoteConflict { term: 4, .. }));
        let loaded = ControllerEpoch::load(dir.path()).unwrap();
        assert_eq!(loaded.current_term, 4);
        assert_eq!(loaded.voted_for, Some(a));
        assert_eq!(loaded.leader_id, None);
    }
}
