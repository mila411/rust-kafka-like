use super::election::LeaderElection;
use super::state::BrokerState;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct Heartbeat {
    pub last_beat: Arc<Mutex<Instant>>,
    pub timeout: Duration,
}

impl Heartbeat {
    pub fn new(timeout: Duration) -> Self {
        Heartbeat {
            last_beat: Arc::new(Mutex::new(Instant::now())),
            timeout,
        }
    }

    pub fn start(election: LeaderElection) {
        let heartbeat = Arc::new(Self::new(Duration::from_secs(1)));
        let election = Arc::new(election);

        // Heartbeat Transmission Thread
        let send_election = election.clone();
        let send_heartbeat = heartbeat.clone();
        std::thread::spawn(move || {
            while *send_election.state.lock().unwrap() == BrokerState::Leader {
                Self::send_heartbeat(&send_election);
                *send_heartbeat.last_beat.lock().unwrap() = Instant::now();
                std::thread::sleep(Duration::from_millis(500));
            }
        });

        // Heartbeat monitoring thread
        let monitor_election = election.clone();
        let monitor_heartbeat = heartbeat.clone();
        std::thread::spawn(move || {
            while *monitor_election.state.lock().unwrap() != BrokerState::Leader {
                if Self::check_timeout(&monitor_heartbeat) {
                    println!("Heartbeat timeout, starting election");
                    monitor_election.start_election();
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        });
    }

    fn send_heartbeat(election: &LeaderElection) {
        let peers = election.peers.lock().unwrap();
        for (peer_id, _) in peers.iter() {
            println!("Sending heartbeat to peer: {}", peer_id);
            // TODO Implementing actual network communication here
        }
    }

    fn check_timeout(&self) -> bool {
        let last = *self.last_beat.lock().unwrap();
        last.elapsed() > self.timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_timeout() {
        let heartbeat = Heartbeat::new(Duration::from_millis(100));
        std::thread::sleep(Duration::from_millis(150));
        assert!(heartbeat.check_timeout());
    }

    #[test]
    fn test_heartbeat_within_timeout() {
        let heartbeat = Heartbeat::new(Duration::from_millis(100));
        assert!(!heartbeat.check_timeout());
    }
}
