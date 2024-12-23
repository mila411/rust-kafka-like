use std::sync::atomic::AtomicU64;

#[derive(Debug, Clone, PartialEq)]
pub enum BrokerState {
    Follower,
    Candidate,
    Leader,
}

pub struct Term {
    pub current: AtomicU64,
    pub voted_for: Option<String>,
}
