use super::heartbeat::Heartbeat;
use super::state::{BrokerState, Term};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct LeaderElection {
    pub broker_id: String,
    pub state: Arc<Mutex<BrokerState>>,
    pub term: Arc<Mutex<Term>>,
    pub peers: Arc<Mutex<HashMap<String, String>>>,
    pub last_heartbeat: Arc<Mutex<Instant>>,
    pub election_timeout: Duration,
}

impl LeaderElection {
    /// Creates a new leader election instance.
    ///
    /// # Arguments
    ///
    /// * `broker_id` - The ID of the broker.
    /// * `peers` - A hashmap of peer broker IDs and their addresses.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_kafka_like::broker::leader::election::LeaderElection;
    /// use std::collections::HashMap;
    ///
    /// let peers = HashMap::new();
    /// let election = LeaderElection::new("broker1", peers);
    /// assert_eq!(election.broker_id, "broker1");
    /// ```
    pub fn new(broker_id: &str, peers: HashMap<String, String>) -> Self {
        LeaderElection {
            broker_id: broker_id.to_string(),
            state: Arc::new(Mutex::new(BrokerState::Follower)),
            term: Arc::new(Mutex::new(Term {
                current: AtomicU64::new(0),
                voted_for: None,
            })),
            peers: Arc::new(Mutex::new(peers)),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            election_timeout: Duration::from_secs(5),
        }
    }

    /// Starts the leader election process.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_kafka_like::broker::leader::election::LeaderElection;
    /// use std::collections::HashMap;
    ///
    /// let peers = HashMap::new();
    /// let mut election = LeaderElection::new("broker1", peers);
    /// let elected = election.start_election();
    /// assert!(elected);
    /// ```
    pub fn start_election(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if *state != BrokerState::Follower {
            return false;
        }

        *state = BrokerState::Candidate;
        let mut term = self.term.lock().unwrap();
        term.current.fetch_add(1, Ordering::SeqCst);
        term.voted_for = Some(self.broker_id.clone());

        let votes = self.request_votes();
        let peers = self.peers.lock().unwrap();
        let majority = (peers.len() + 1) / 2 + 1;

        if votes >= majority {
            *state = BrokerState::Leader;
            Heartbeat::start(self.clone());
            true
        } else {
            *state = BrokerState::Follower;
            false
        }
    }

    fn request_votes(&self) -> usize {
        let votes = 1;
        let peers = self.peers.lock().unwrap();
        votes + peers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leader_election() {
        let mut peers = HashMap::new();
        peers.insert("peer1".to_string(), "localhost:8081".to_string());
        peers.insert("peer2".to_string(), "localhost:8082".to_string());

        let election = LeaderElection::new("broker1", peers);
        assert_eq!(*election.state.lock().unwrap(), BrokerState::Follower);

        let elected = election.start_election();
        assert!(elected);
        assert_eq!(*election.state.lock().unwrap(), BrokerState::Leader);
    }
}
