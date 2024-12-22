use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc::{channel, Receiver, Sender},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

#[derive(Debug)]
pub enum BrokerError {
    IoError(io::Error),
    LockError(String),
    TopicError(String),
    PartitionError(String),
    PublishError(String),
    SubscribeError(String),
    ReplicationError(String),
    LeaderError(String),
    AckError(String),
}

#[derive(Debug, Clone)]
pub struct MessageAck {
    pub message_id: u64,
    pub topic: String,
    pub status: AckStatus,
}

#[derive(Debug, Clone)]
pub enum AckStatus {
    Success,
    Failed(String),
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BrokerError::IoError(e) => write!(f, "IO error: {}", e),
            BrokerError::LockError(e) => write!(f, "Locked error: {}", e),
            BrokerError::TopicError(e) => write!(f, "Topic error: {}", e),
            BrokerError::PartitionError(e) => write!(f, "Partition error: {}", e),
            BrokerError::PublishError(e) => write!(f, "Publish error: {}", e),
            BrokerError::SubscribeError(e) => write!(f, "Subscribe error: {}", e),
            BrokerError::ReplicationError(e) => write!(f, "Replication error: {}", e),
            BrokerError::LeaderError(e) => write!(f, "Leader error: {}", e),
            BrokerError::AckError(e) => write!(f, "Ack error: {}", e),
        }
    }
}

impl Error for BrokerError {}

impl From<io::Error> for BrokerError {
    fn from(error: io::Error) -> Self {
        BrokerError::IoError(error)
    }
}

type Subscriber = Box<dyn Fn(String, Sender<MessageAck>) + Send + 'static>;

struct Partition {
    log_file: Arc<Mutex<File>>,
    replicas: HashSet<String>,
}

struct Topic {
    name: String,
    partitions: Vec<Partition>,
    subscribers: Vec<Subscriber>,
}

pub struct Broker {
    id: String,
    state: BrokerState,
    topics: HashMap<String, Topic>,
    num_partitions: usize,
    peers: HashSet<String>,
    term: AtomicU64,
    is_active: Arc<Mutex<bool>>,
    min_replicas: usize,
}

#[derive(Debug, Clone, PartialEq)]
enum BrokerState {
    Leader,
    Follower,
    Candidate,
}

impl Broker {
    pub fn new(
        id: String,
        log_dir: &str,
        default_partitions: usize,
        peers: HashSet<String>,
        min_replicas: usize,
    ) -> Result<Self, BrokerError> {
        use std::path::Path;

        if !Path::new(log_dir).exists() {
            std::fs::create_dir(log_dir)?;
        }

        Ok(Broker {
            id,
            state: BrokerState::Leader,
            topics: HashMap::new(),
            num_partitions: default_partitions,
            peers,
            term: AtomicU64::new(0),
            is_active: Arc::new(Mutex::new(true)),
            min_replicas,
        })
    }

    pub fn create_topic(
        &mut self,
        name: &str,
        num_partitions: Option<usize>,
    ) -> Result<(), BrokerError> {
        if self.topics.contains_key(name) {
            return Err(BrokerError::TopicError(format!(
                "The topic already exists.: {}",
                name
            )));
        }

        let partitions_count = num_partitions.unwrap_or(self.num_partitions);
        let mut partitions = Vec::with_capacity(partitions_count);

        for i in 0..partitions_count {
            let file_path = format!("logs/{}_partition_{}.log", name, i);
            let log_file = OpenOptions::new()
                .append(true)
                .create(true)
                .read(true)
                .open(&file_path)?;

            partitions.push(Partition {
                log_file: Arc::new(Mutex::new(log_file)),
                replicas: HashSet::new(),
            });
        }

        self.topics.insert(
            name.to_string(),
            Topic {
                name: name.to_string(),
                partitions,
                subscribers: Vec::new(),
            },
        );

        Ok(())
    }

    pub fn subscribe(&mut self, topic_name: &str, callback: Subscriber) -> Result<(), BrokerError> {
        let topic = self
            .topics
            .get_mut(topic_name)
            .ok_or_else(|| BrokerError::TopicError(format!("No topics found: {}", topic_name)))?;

        topic.subscribers.push(callback);
        Ok(())
    }

    pub fn publish_with_ack(
        &self,
        topic_name: &str,
        message: String,
        key: Option<&str>,
    ) -> Result<MessageAck, BrokerError> {
        let message_id = self.term.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = channel();

        self.publish_internal(topic_name, message, key, message_id, tx)?;

        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(ack) => Ok(ack),
            Err(_) => Err(BrokerError::AckError("Response timeout".to_string())),
        }
    }

    fn publish_internal(
        &self,
        topic_name: &str,
        message: String,
        key: Option<&str>,
        message_id: u64,
        ack_sender: Sender<MessageAck>,
    ) -> Result<(), BrokerError> {
        if self.state != BrokerState::Leader {
            return Err(BrokerError::LeaderError(
                "Only the leader can publish messages.".to_string(),
            ));
        }

        let topic = self
            .topics
            .get(topic_name)
            .ok_or_else(|| BrokerError::TopicError(format!("No topics found: {}", topic_name)))?;

        for subscriber in &topic.subscribers {
            let ack_sender = ack_sender.clone();
            subscriber(message.clone(), ack_sender);
        }

        let partition = self.get_partition(topic, key)?;
        let mut log_file = partition
            .log_file
            .lock()
            .map_err(|e| BrokerError::LockError(e.to_string()))?;
        writeln!(log_file, "{}", message)?;

        Ok(())
    }

    fn get_partition<'a>(
        &self,
        topic: &'a Topic,
        key: Option<&str>,
    ) -> Result<&'a Partition, BrokerError> {
        if topic.partitions.is_empty() {
            return Err(BrokerError::PartitionError(
                "There is no partition.".to_string(),
            ));
        }

        let partition_index = if let Some(k) = key {
            let hash = self.hash_key(k);
            hash % topic.partitions.len()
        } else {
            0
        };

        topic
            .partitions
            .get(partition_index)
            .ok_or_else(|| BrokerError::PartitionError("Invalid partition index.".to_string()))
    }

    fn hash_key(&self, key: &str) -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }

   pub fn start_health_check(&self) {
        let peers = self.peers.clone();
        let is_active = Arc::clone(&self.is_active);

        thread::spawn(move || loop {
            for peer in &peers {
                if let Err(_) = Self::check_peer_health(peer) {
                    println!("Detecting peer problems: {}", peer);
                    if let Ok(mut active) = is_active.lock() {
                        *active = false;
                    }
                }
            }
            thread::sleep(Duration::from_secs(5));
        });
    }

    fn check_peer_health(_peer: &str) -> Result<(), BrokerError> {
        // TODO: Implemented the actual health check process
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut peers = HashSet::new();
    peers.insert("broker2".to_string());
    peers.insert("broker3".to_string());

    println!("Initializing broker...");
    let broker = Arc::new(Mutex::new(Broker::new(
        "broker1".to_string(),
        "logs",
        3,
        peers,
        2,
    )?));

    if let Ok(mut broker_ref) = broker.lock() {
        println!("Creating a topic...");
        broker_ref.create_topic("test_topic", Some(3))?;

        println!("Registering a subscriber...");
        broker_ref.subscribe(
            "test_topic",
            Box::new(|message, ack_sender| {
                println!("Received message: {}", message);
                ack_sender
                    .send(MessageAck {
                        message_id: 0, // TODO: Set an appropriate ID
                        topic: "test_topic".to_string(),
                        status: AckStatus::Success,
                    })
                    .unwrap();
            }),
        )?;

        broker_ref.start_health_check();
    }

    let broker_producer = Arc::clone(&broker);
    let producer_handle = thread::spawn(move || {
        println!("I started as a producer....");
        thread::sleep(Duration::from_secs(2));

        if let Ok(broker) = broker_producer.lock() {
            println!("Sending message...");
            match broker.publish_with_ack("test_topic", "Test message".to_string(), None) {
                Ok(ack) => println!("Received message confirmation response: {:?}", ack),
                Err(e) => eprintln!("Message publication error: {}", e),
            }
        }
    });

    thread::sleep(Duration::from_secs(3));
    producer_handle.join().unwrap();

    println!("Quit the program");
    Ok(())
}