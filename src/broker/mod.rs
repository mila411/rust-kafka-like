pub mod cluster;
pub mod consumer;
pub mod error;
pub mod leader;
pub mod log_compression;
pub mod message_queue;
pub mod node_management;
pub mod scaling;
pub mod storage;
pub mod topic;

use crate::broker::consumer::group::ConsumerGroup;
use crate::broker::error::BrokerError;
use crate::broker::leader::{BrokerState, LeaderElection};
use crate::broker::log_compression::LogCompressor;
use crate::broker::message_queue::MessageQueue;
use crate::broker::node_management::{check_node_health, recover_node};
use crate::broker::storage::Storage;
use crate::broker::topic::Topic;
use crate::message::ack::MessageAck;
use crate::subscriber::types::Subscriber;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub type ConsumerGroups = HashMap<String, ConsumerGroup>;

impl ConsumerGroup {
    pub fn reset_assignments(&mut self) {
        if let Ok(mut map) = self.assignments.lock() {
            map.clear();
        }
    }
}

pub struct Broker {
    pub id: String,
    pub topics: HashMap<String, Topic>,
    pub num_partitions: usize,
    pub replication_factor: usize,
    pub term: AtomicU64,
    pub leader_election: LeaderElection,
    pub storage: Arc<Mutex<Storage>>,
    pub consumer_groups: Arc<Mutex<HashMap<String, ConsumerGroup>>>,
    pub nodes: Arc<Mutex<HashMap<String, Node>>>,
    partitions: Arc<Mutex<HashMap<usize, Partition>>>,
    pub replicas: Arc<Mutex<HashMap<String, Vec<String>>>>,
    pub leader: Arc<Mutex<Option<String>>>,
    pub message_queue: Arc<MessageQueue>,
    log_path: String,
}

impl Broker {
    /// Creates a new broker instance.
    ///
    /// # Arguments
    ///
    /// * `id` - A string slice that holds the ID of the broker.
    /// * `num_partitions` - The number of partitions for the broker.
    /// * `replication_factor` - The replication factor for the broker.
    /// * `storage_path` - The path to the storage file.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::Broker;
    ///
    /// let broker = Broker::new("broker1", 3, 2, "logs");
    /// assert_eq!(broker.id, "broker1");
    /// assert_eq!(broker.num_partitions, 3);
    /// assert_eq!(broker.replication_factor, 2);
    /// ```
    pub fn new(
        id: &str,
        num_partitions: usize,
        replication_factor: usize,
        storage_path: &str,
    ) -> Self {
        let min_instances = 1;
        let max_instances = 10;
        let check_interval = Duration::from_secs(30);
        let peers = HashMap::new(); //TODO it reads from the settings
        let leader_election = LeaderElection::new(id, peers);
        let storage = Storage::new(storage_path).expect("Failed to initialize storage");
        let log_path = format!("logs/{}.log", id);

        let broker = Broker {
            id: id.to_string(),
            topics: HashMap::new(),
            num_partitions,
            replication_factor,
            term: AtomicU64::new(0),
            leader_election,
            storage: Arc::new(Mutex::new(storage)),
            consumer_groups: Arc::new(Mutex::new(HashMap::new())),
            nodes: Arc::new(Mutex::new(HashMap::new())),
            partitions: Arc::new(Mutex::new(HashMap::new())),
            replicas: Arc::new(Mutex::new(HashMap::new())),
            leader: Arc::new(Mutex::new(None)),
            message_queue: Arc::new(MessageQueue::new(
                min_instances,
                max_instances,
                check_interval,
            )),
            log_path: log_path.to_string(),
        };
        broker.monitor_nodes();
        broker
    }

    pub fn perform_operation_with_retry<F, T, E>(
        &self,
        operation: F,
        max_retries: u32,
        delay: Duration,
    ) -> Result<T, E>
    where
        F: Fn() -> Result<T, E>,
    {
        let mut attempts = 0;
        loop {
            match operation() {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if attempts >= max_retries {
                        return Err(e);
                    }
                    attempts += 1;
                    println!("Operation failed. Retry {} times...", attempts);
                    std::thread::sleep(delay);
                }
            }
        }
    }

    pub fn is_healthy(&self) -> bool {
        // Storage Health Check
        if let Ok(storage) = self.storage.lock() {
            if !storage.is_available() {
                return false;
            }
        } else {
            return false;
        }

        // Node Health Check
        if let Ok(nodes) = self.nodes.lock() {
            for node in nodes.values() {
                if let Ok(data) = node.data.lock() {
                    if data.is_empty() {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        } else {
            return false;
        }

        // Checking the status of the message queue
        if self.message_queue.is_empty() {
            return false;
        }

        // Check the status of the leader election
        if *self.leader_election.state.lock().unwrap() != BrokerState::Leader {
            return false;
        }

        true
    }

    pub fn send_message(&self, message: String) {
        self.message_queue.send(message);
    }

    pub fn receive_message(&self) -> Option<String> {
        Some(self.message_queue.receive())
    }

    pub fn replicate_data(&self, partition_id: usize, data: &[u8]) {
        let replicas = self.replicas.lock().unwrap();
        if let Some(nodes) = replicas.get(&partition_id.to_string()) {
            for node_id in nodes {
                if let Some(node) = self.nodes.lock().unwrap().get(node_id) {
                    let mut node_data = node.data.lock().unwrap();
                    node_data.clear();
                    node_data.extend_from_slice(data);
                }
            }
        }
    }

    fn monitor_nodes(&self) {
        let storage = self.storage.clone();
        let consumer_groups = self.consumer_groups.clone();
        std::thread::spawn(move || {
            loop {
                let node_healthy = check_node_health(&storage);

                if !node_healthy {
                    recover_node(&storage, &consumer_groups);
                }

                std::thread::sleep(Duration::from_secs(5));
            }
        });
    }

    pub fn detect_failure(&self, node_id: &str) {
        let mut nodes = self.nodes.lock().unwrap();
        if nodes.remove(node_id).is_some() {
            // println!("Node {} has failed", node_id);
            drop(nodes);
            self.rebalance_partitions();
            self.start_election();
        }
    }

    pub fn add_node(&self, node_id: String, node: Node) {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.insert(node_id, node);
        drop(nodes);
        self.rebalance_partitions();
    }

    pub fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.remove(node_id);
        drop(nodes);
        self.rebalance_partitions();
    }

    pub fn rebalance_partitions(&self) {
        let nodes = self.nodes.lock().unwrap();
        let num_nodes = nodes.len();
        if num_nodes == 0 {
            return;
        }

        let mut partitions = self.partitions.lock().unwrap();
        for (partition_id, partition) in partitions.iter_mut() {
            let node_index = partition_id % num_nodes;
            let node_id = nodes.keys().nth(node_index).unwrap().clone();
            partition.node_id = node_id;
        }
    }

    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::Broker;
    /// use pilgrimage::broker::Node;
    /// use std::sync::{Arc, Mutex};
    ///
    /// let broker = Broker::new("broker1", 3, 2, "logs");
    /// let node1 = Node { data: Arc::new(Mutex::new(Vec::new())) };
    /// let node2 = Node { data: Arc::new(Mutex::new(Vec::new())) };
    ///
    /// broker.add_node("node1".to_string(), node1);
    /// broker.add_node("node2".to_string(), node2);
    ///
    /// let elected = broker.start_election();
    ///
    /// assert!(elected);
    /// ```
    pub fn start_election(&self) -> bool {
        let nodes = self.nodes.lock().unwrap();
        if let Some((new_leader, _)) = nodes.iter().next() {
            let mut leader = self.leader.lock().unwrap();
            *leader = Some(new_leader.clone());
            // println!("New leader elected: {}", new_leader);
            return true;
        }
        false
    }

    pub fn is_leader(&self) -> bool {
        *self.leader_election.state.lock().unwrap() == BrokerState::Leader
    }

    /// Creates a new topic.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the topic.
    /// * `num_partitions` - The number of partitions for the topic.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::Broker;
    ///
    /// let mut broker = Broker::new("broker1", 3, 2, "logs");
    /// broker.create_topic("test_topic", None).unwrap();
    /// assert!(broker.topics.contains_key("test_topic"));
    /// ```
    pub fn create_topic(
        &mut self,
        name: &str,
        num_partitions: Option<usize>,
    ) -> Result<(), BrokerError> {
        if self.topics.contains_key(name) {
            return Err(BrokerError::TopicError(format!(
                "Topic {} already exists",
                name
            )));
        }

        let partitions = num_partitions.unwrap_or(self.num_partitions);
        let topic = Topic::new(name, partitions, self.replication_factor);
        self.topics.insert(name.to_string(), topic);
        Ok(())
    }

    /// Subscribes a subscriber to a topic.
    ///
    /// # Arguments
    ///
    /// * `topic_name` - The name of the topic.
    /// * `subscriber` - The subscriber to add.
    /// * `group_id` - Optional consumer group ID.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::Broker;
    /// use pilgrimage::subscriber::types::Subscriber;
    ///
    /// let mut broker = Broker::new("broker1", 3, 2, "logs");
    /// broker.create_topic("test_topic", None).unwrap();
    /// let subscriber = Subscriber::new(
    ///     "consumer1",
    ///     Box::new(|msg: String| {
    ///         println!("Received message: {}", msg);
    ///     }),
    /// );
    /// broker.subscribe("test_topic", subscriber, None).unwrap();
    /// ```
    pub fn subscribe(
        &mut self,
        topic_name: &str,
        subscriber: Subscriber,
        group_id: Option<&str>,
    ) -> Result<(), BrokerError> {
        if let Some(topic) = self.topics.get_mut(topic_name) {
            if let Some(group_id) = group_id {
                let mut groups = self.consumer_groups.lock().unwrap();
                let group = groups
                    .entry(group_id.to_string())
                    .or_insert_with(|| ConsumerGroup::new(group_id));
                group.add_member(&subscriber.id.clone(), subscriber);
            } else {
                topic.add_subscriber(subscriber);
            }
            Ok(())
        } else {
            Err(BrokerError::TopicError(format!(
                "Topic {} not found",
                topic_name
            )))
        }
    }

    /// Publishes a message to a topic with acknowledgment.
    ///
    /// # Arguments
    ///
    /// * `topic_name` - The name of the topic.
    /// * `message` - The message to publish.
    /// * `key` - An optional key for partitioning.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::Broker;
    ///
    /// let mut broker = Broker::new("broker1", 3, 2, "logs");
    /// broker.create_topic("test_topic", None).unwrap();
    /// let ack = broker.publish_with_ack("test_topic", "test_message".to_string(), None).unwrap();
    /// assert_eq!(ack.topic, "test_topic");
    /// ```
    pub fn publish_with_ack(
        &mut self,
        topic_name: &str,
        message: String,
        key: Option<&str>,
    ) -> Result<MessageAck, BrokerError> {
        if let Some(topic) = self.topics.get_mut(topic_name) {
            let partition_id = topic.publish(message.clone(), key)?;
            let ack = MessageAck::new(
                self.term.fetch_add(1, Ordering::SeqCst),
                topic_name,
                partition_id,
            );

            self.storage.lock().unwrap().write_message(&message)?;

            // Message delivery to consumer groups
            let message_clone = message.clone();
            let consumer_groups = self.consumer_groups.clone();
            std::thread::spawn(move || {
                if let Ok(groups) = consumer_groups.lock() {
                    for group in groups.values() {
                        if let (Ok(assignments), Ok(members)) =
                            (group.assignments.lock(), group.members.lock())
                        {
                            for (cons_id, parts) in assignments.iter() {
                                if parts.contains(&partition_id) {
                                    if let Some(member) = members.get(cons_id) {
                                        (member.subscriber.callback)(message_clone.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            });

            Ok(ack)
        } else {
            Err(BrokerError::TopicError(format!(
                "Topic {} not found",
                topic_name
            )))
        }
    }

    pub fn rotate_logs(&self) {
        let log_path = Path::new(&self.log_path);
        let rotated = log_path.with_extension("old");
        if let Err(e) = LogCompressor::compress_file(log_path, rotated.as_path()) {
            eprintln!("Failed to compress log file: {}", e);
        }
        if let Err(e) = File::create(&self.log_path) {
            eprintln!("Failed to create new log file: {}", e);
        }
    }

    pub fn write_log(&self, message: &str) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.log_path)?;
        writeln!(file, "{}", message)?;
        Ok(())
    }

    pub fn cleanup_logs(&self) -> Result<(), BrokerError> {
        self.storage.lock().unwrap().cleanup_logs()?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Node {
    pub data: Arc<Mutex<Vec<u8>>>,
}

pub struct Partition {
    pub node_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::{self, File};
    use std::path::Path;
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    fn create_test_broker() -> Broker {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("test_storage")
            .to_str()
            .unwrap()
            .to_owned();

        Broker {
            consumer_groups: Arc::new(Mutex::new(HashMap::new())),
            id: String::from("test_broker"),
            leader: Arc::new(Mutex::new(Some(String::from("leader1")))),
            storage: Arc::new(Mutex::new(Storage::new(&storage_path).unwrap())),
            topics: HashMap::new(),
            num_partitions: 1,
            replication_factor: 1,
            term: AtomicU64::new(1),
            leader_election: LeaderElection::new("test_broker", HashMap::new()),
            nodes: Arc::new(Mutex::new(HashMap::new())),
            partitions: Arc::new(Mutex::new(HashMap::new())),
            replicas: Arc::new(Mutex::new(HashMap::new())),
            message_queue: Arc::new(MessageQueue::new(1, 10, Duration::from_secs(1))),
            log_path: "logs/broker.log".to_string(),
        }
    }

    fn create_test_broker_with_path(log_path: &str) -> Broker {
        Broker {
            id: "test_broker".to_string(),
            topics: HashMap::new(),
            num_partitions: 3,
            replication_factor: 2,
            term: AtomicU64::new(0),
            leader_election: LeaderElection::new("test_broker", HashMap::new()),
            storage: Arc::new(Mutex::new(Storage::new("test_storage").unwrap())),
            consumer_groups: Arc::new(Mutex::new(HashMap::new())),
            nodes: Arc::new(Mutex::new(HashMap::new())),
            partitions: Arc::new(Mutex::new(HashMap::new())),
            replicas: Arc::new(Mutex::new(HashMap::new())),
            leader: Arc::new(Mutex::new(None)),
            message_queue: Arc::new(MessageQueue::new(1, 10, Duration::from_secs(30))),
            log_path: log_path.to_string(),
        }
    }

    fn setup_test_logs(path: &str) {
        cleanup_test_logs(path);
    }

    fn cleanup_test_logs(path: &str) {
        if Path::new(path).exists() {
            let _ = fs::remove_file(path);
        }
        let old_path = format!("{}.old", path);
        if Path::new(&old_path).exists() {
            let _ = fs::remove_file(old_path);
        }
    }

    #[test]
    fn test_message_persistence() {
        let test_log = "test_logs_persistence";
        setup_test_logs(test_log);

        let mut broker = Broker::new("broker1", 3, 2, test_log);
        broker.create_topic("test_topic", None).unwrap();

        broker
            .publish_with_ack("test_topic", "test_message_1".to_string(), None)
            .unwrap();
        broker
            .publish_with_ack("test_topic", "test_message_2".to_string(), None)
            .unwrap();
        broker
            .publish_with_ack("test_topic", "test_message_3".to_string(), None)
            .unwrap();

        let storage = Storage::new(test_log).unwrap();
        let messages = storage.read_messages().unwrap();
        assert_eq!(
            messages.len(),
            3,
            "The number of expected messages differs from the actual number of messages."
        );
        assert_eq!(messages[0], "test_message_1");
        assert_eq!(messages[1], "test_message_2");
        assert_eq!(messages[2], "test_message_3");

        cleanup_test_logs(test_log);
    }

    #[test]
    fn test_log_rotation_and_cleanup() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("test_storage")
            .to_str()
            .unwrap()
            .to_owned();
        let storage = Storage::new(&storage_path).unwrap();

        let result = storage.rotate_logs();
        assert!(result.is_err(), "Old log file does not exist");

        let old_log_path = format!("{}.old", storage_path);
        File::create(&old_log_path).unwrap();
        let result = storage.rotate_logs();
        assert!(
            result.is_ok(),
            "Log rotation should succeed when old log file exists"
        );
    }

    #[test]
    fn test_broker_creation() {
        let test_log = "test_logs_creation";
        setup_test_logs(test_log);

        let broker = Broker::new("broker1", 3, 2, test_log);
        assert_eq!(broker.id, "broker1");
        assert_eq!(broker.num_partitions, 3);
        assert_eq!(broker.replication_factor, 2);

        cleanup_test_logs(test_log);
    }

    #[test]
    fn test_create_topic() {
        let test_log = "test_logs_topic";
        setup_test_logs(test_log);

        let mut broker = Broker::new("broker1", 3, 2, test_log);
        broker.create_topic("test_topic", None).unwrap();
        assert!(broker.topics.contains_key("test_topic"));

        cleanup_test_logs(test_log);
    }

    #[test]
    fn test_subscribe_and_publish() {
        let test_log = "test_logs_sub_pub";
        setup_test_logs(test_log);

        let mut broker = Broker::new("broker1", 3, 2, test_log);
        broker.create_topic("test_topic", None).unwrap();

        let subscriber = Subscriber::new(
            "sub1",
            Box::new(|msg: String| {
                println!("Received message: {}", msg);
            }),
        );
        broker.subscribe("test_topic", subscriber, None).unwrap();

        let ack = broker
            .publish_with_ack("test_topic", "test_message".to_string(), None)
            .unwrap();
        assert_eq!(ack.topic, "test_topic");

        cleanup_test_logs(test_log);
    }

    #[test]
    fn test_consumer_group_message_distribution() {
        println!("Test Start");
        let mut broker = Broker::new("broker1", 3, 2, "logs");
        broker.create_topic("test_topic", None).unwrap();

        let received_count = Arc::new((Mutex::new(0), Condvar::new()));
        let total_messages = 10;

        let received_messages = Arc::new(Mutex::new(Vec::new()));

        for i in 0..2 {
            let received_messages = Arc::clone(&received_messages);
            let received_count = Arc::clone(&received_count);

            let subscriber = Subscriber::new(
                &format!("consumer_{}", i),
                Box::new(move |msg: String| {
                    received_messages.lock().unwrap().push(msg);

                    let (lock, cvar) = &*received_count;
                    let mut count = lock.lock().unwrap();
                    *count += 1;
                    println!("Received count: {}", *count);
                    if *count == total_messages {
                        cvar.notify_all();
                    }
                }),
            );

            broker
                .subscribe("test_topic", subscriber, Some("group1"))
                .unwrap();
            thread::sleep(Duration::from_millis(100));
        }

        println!("Start sending message");
        for i in 0..total_messages {
            let message = format!("message_{}", i);
            println!("Sending: {}", message);
            broker
                .publish_with_ack("test_topic", message, None)
                .unwrap();
            thread::sleep(Duration::from_millis(10));
        }

        let (lock, cvar) = &*received_count;
        let timeout = Duration::from_secs(5);
        let result = cvar
            .wait_timeout_while(lock.lock().unwrap(), timeout, |&mut count| {
                count < total_messages
            })
            .unwrap();

        if result.1.timed_out() {
            panic!(
                "The test timed out: Number of messages received {}",
                *result.0
            );
        }

        let received = received_messages.lock().unwrap();
        println!("Received messages: {:?}", *received);
        assert_eq!(received.len(), total_messages);
    }

    #[test]
    fn test_data_replication() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let node1 = Node {
            data: Arc::new(Mutex::new(Vec::new())),
        };
        let node2 = Node {
            data: Arc::new(Mutex::new(Vec::new())),
        };

        broker.add_node("node1".to_string(), node1);
        broker.add_node("node2".to_string(), node2);

        {
            let mut replicas = broker.replicas.lock().unwrap();
            replicas.insert("1".to_string(), vec![
                "node1".to_string(),
                "node2".to_string(),
            ]);
        }

        let data = b"test data";
        broker.replicate_data(1, data);

        let nodes = broker.nodes.lock().unwrap();
        let node1_data = nodes.get("node1").unwrap().data.lock().unwrap().clone();
        let node2_data = nodes.get("node2").unwrap().data.lock().unwrap().clone();

        assert_eq!(node1_data, data);
        assert_eq!(node2_data, data);
    }

    #[test]
    fn test_failover() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let node1 = Node {
            data: Arc::new(Mutex::new(Vec::new())),
        };
        let node2 = Node {
            data: Arc::new(Mutex::new(Vec::new())),
        };

        broker.add_node("node1".to_string(), node1);
        broker.add_node("node2".to_string(), node2);

        {
            let mut leader = broker.leader.lock().unwrap();
            *leader = Some("node1".to_string());
        }

        broker.detect_failure("node1");

        let nodes = broker.nodes.lock().unwrap();
        assert!(!nodes.contains_key("node1"));
        assert!(nodes.contains_key("node2"));

        let leader = broker.leader.lock().unwrap();
        assert_eq!(leader.as_deref(), Some("node2"));
    }

    #[test]
    fn test_rebalance() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let node1 = Node {
            data: Arc::new(Mutex::new(Vec::new())),
        };
        let node2 = Node {
            data: Arc::new(Mutex::new(Vec::new())),
        };

        broker.add_node("node1".to_string(), node1);
        broker.add_node("node2".to_string(), node2);

        {
            let mut partitions = broker.partitions.lock().unwrap();
            for i in 0..3 {
                partitions.insert(i, Partition {
                    node_id: String::new(),
                });
            }
        }

        broker.rebalance_partitions();
        let partitions = broker.partitions.lock().unwrap();
        let nodes = broker.nodes.lock().unwrap();
        for (partition_id, partition) in partitions.iter() {
            let node_index = partition_id % nodes.len();
            let expected_node_id = nodes.keys().nth(node_index).unwrap();
            assert_eq!(&partition.node_id, expected_node_id);
        }
    }

    #[test]
    fn test_add_existing_node() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let node = Node {
            data: Arc::new(Mutex::new(Vec::new())),
        };

        broker.add_node("node1".to_string(), node.clone());
        broker.add_node("node1".to_string(), node);

        let nodes = broker.nodes.lock().unwrap();
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn test_remove_nonexistent_node() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        broker.remove_node("node1");

        let nodes = broker.nodes.lock().unwrap();
        assert_eq!(nodes.len(), 0);
    }

    #[test]
    fn test_replicate_to_nonexistent_node() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let data = b"test data";

        broker.replicate_data(1, data);

        let nodes = broker.nodes.lock().unwrap();
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_election_with_no_nodes() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let elected = broker.start_election();

        assert!(!elected);
    }

    #[test]
    fn test_add_invalid_node() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let node = Node {
            data: Arc::new(Mutex::new(Vec::new())),
        };

        broker.add_node("node1".to_string(), node.clone());
        broker.add_node("node1".to_string(), node);

        let nodes = broker.nodes.lock().unwrap();
        assert_eq!(nodes.len(), 1, "Duplicate nodes should not be added.");
    }

    #[test]
    fn test_elect_leader_with_no_nodes() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let elected = broker.start_election();
        assert!(
            !elected,
            "If there are no nodes, the leader election should fail."
        );
    }

    #[test]
    fn test_replicate_to_nonexistent_nodes() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let data = b"test data";

        broker.replicate_data(1, data);

        let nodes = broker.nodes.lock().unwrap();
        assert!(
            nodes.is_empty(),
            "If the node does not exist, replication should fail."
        );
    }

    #[test]
    fn test_rotate_logs() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        {
            let mut file = File::create(&log_path).unwrap();
            writeln!(file, "This is a test log entry.").unwrap();
        }

        let broker = create_test_broker_with_path(log_path.to_str().unwrap());

        broker.rotate_logs();
        let rotated_log_path = log_path.with_extension("old");

        assert!(rotated_log_path.exists());
        assert!(log_path.exists());

        let new_log_content = fs::read_to_string(&log_path).unwrap();
        assert!(new_log_content.is_empty());
    }

    #[test]
    fn test_rotate_logs_invalid_path() {
        let broker = create_test_broker_with_path("/invalid/path/test.log");
        broker.rotate_logs();
    }

    #[test]
    fn test_rotate_logs_no_permissions() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        {
            let mut file = File::create(&log_path).unwrap();
            writeln!(file, "test data").unwrap();
        }

        fs::set_permissions(
            &log_path,
            <fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o444),
        )
        .unwrap();

        let broker = create_test_broker_with_path(log_path.to_str().unwrap());
        broker.rotate_logs();
    }

    #[test]
    fn test_rotate_logs_file_not_found() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("nonexistent.log");

        let broker = create_test_broker_with_path(log_path.to_str().unwrap());
        broker.rotate_logs();
    }

    #[test]
    fn test_rotate_logs_file_in_use() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("locked.log");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&log_path)
            .unwrap();

        let broker = create_test_broker_with_path(log_path.to_str().unwrap());
        broker.rotate_logs();

        drop(file);
    }

    #[test]
    fn test_write_log() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        fs::create_dir_all(log_path.parent().unwrap()).unwrap();

        let broker = create_test_broker_with_path(log_path.to_str().unwrap());
        broker.write_log("Test message 1").unwrap();
        broker.write_log("Test message 2").unwrap();

        let content = fs::read_to_string(&log_path).unwrap();
        assert!(content.contains("Test message 1"));
        assert!(content.contains("Test message 2"));
    }

    #[test]
    fn test_write_log_invalid_path() {
        let dir = tempdir().unwrap();
        let invalid_path = dir.path().join("nonexistent").join("test.log");
        let broker = create_test_broker_with_path(invalid_path.to_str().unwrap());

        // Test error handling - check that it returns an error
        let result = broker.write_log("This should not panic");
        assert!(result.is_err());
    }

    #[test]
    fn test_write_log_no_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Create a file and set the permissions to read-only.
        {
            let mut file = File::create(&log_path).unwrap();
            writeln!(file, "Initial content").unwrap();
        }
        fs::set_permissions(&log_path, fs::Permissions::from_mode(0o444)).unwrap(); // 読み取り専用

        let broker = create_test_broker_with_path(log_path.to_str().unwrap());
        let result = broker.write_log("This should handle permission error");
        assert!(result.is_err());
    }

    #[test]
    fn test_rotate_logs_with_invalid_storage() {
        let storage_result = Storage::new("/invalid/path/to/storage");
        assert!(
            storage_result.is_err(),
            "Operations on invalid storage paths should fail."
        );
    }

    #[test]
    fn test_multithreaded_message_processing() {
        let broker = Arc::new(Broker::new("broker1", 3, 2, "logs"));

        let broker_sender = Arc::clone(&broker);
        let sender_handle = thread::spawn(move || {
            for i in 0..10 {
                let message = format!("Message {}", i);
                broker_sender.send_message(message);
            }
        });

        let broker_receiver = Arc::clone(&broker);
        let receiver_handle = thread::spawn(move || {
            for _ in 0..10 {
                if let Some(_message) = broker_receiver.receive_message() {}
            }
        });

        sender_handle.join().unwrap();
        receiver_handle.join().unwrap();
    }

    #[test]
    fn test_log_rotation_error_message() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("test_storage")
            .to_str()
            .unwrap()
            .to_owned();
        let storage = Storage::new(&storage_path).unwrap();

        let result = storage.rotate_logs();
        if let Err(e) = result {
            assert_eq!(
                e.to_string(),
                format!("Old log file {}.old does not exist", storage_path)
            );
        } else {
            panic!("Expected an error but got success");
        }
    }

    #[test]
    fn test_write_and_read_messages() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("test_storage")
            .to_str()
            .unwrap()
            .to_owned();
        let mut storage = Storage::new(&storage_path).unwrap();

        storage.write_message("test_message_1").unwrap();
        storage.write_message("test_message_2").unwrap();

        let messages = storage.read_messages().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], "test_message_1");
        assert_eq!(messages[1], "test_message_2");
    }

    #[test]
    fn test_perform_operation_with_retry_success_on_first_try() {
        let broker = create_test_broker();

        let result = broker.perform_operation_with_retry(
            || Ok::<&str, &str>("Success"),
            3,
            Duration::from_millis(10),
        );

        assert_eq!(result.unwrap(), "Success");
    }

    #[test]
    fn test_perform_operation_with_retry_success_after_retries() {
        let broker = create_test_broker();

        let counter = Arc::new(Mutex::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = broker.perform_operation_with_retry(
            || {
                let mut num = counter_clone.lock().unwrap();
                *num += 1;
                if *num < 3 {
                    Err("Temporary Error")
                } else {
                    Ok("Success after retries")
                }
            },
            5,
            Duration::from_millis(10),
        );

        assert_eq!(result.unwrap(), "Success after retries");
        assert_eq!(*counter.lock().unwrap(), 3);
    }

    #[test]
    fn test_perform_operation_with_retry_failure_after_max_retries() {
        let broker = create_test_broker();

        let counter = Arc::new(Mutex::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = broker.perform_operation_with_retry(
            || {
                let mut num = counter_clone.lock().unwrap();
                *num += 1;
                Err::<(), &str>("Persistent Error")
            },
            3,
            Duration::from_millis(10),
        );

        assert!(result.is_err());
        assert_eq!(*counter.lock().unwrap(), 4);
    }
}
