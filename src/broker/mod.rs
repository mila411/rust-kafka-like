pub mod consumer;
pub mod error;
pub mod leader;
pub mod storage;
pub mod topic;

use crate::broker::consumer::group::ConsumerGroup;
use crate::broker::error::BrokerError;
use crate::broker::leader::{BrokerState, LeaderElection};
use crate::broker::storage::Storage;
use crate::broker::topic::Topic;
use crate::message::ack::MessageAck;
use crate::subscriber::types::Subscriber;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

pub struct MessageQueue {
    queue: Mutex<VecDeque<String>>,
    condvar: Condvar,
}

impl MessageQueue {
    pub fn new() -> Self {
        MessageQueue {
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
        }
    }

    pub fn send(&self, message: String) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(message);
        self.condvar.notify_one();
    }

    pub fn receive(&self) -> String {
        let mut queue = self.queue.lock().unwrap();
        loop {
            match queue.pop_front() {
                Some(message) => return message,
                None => queue = self.condvar.wait(queue).unwrap(),
            }
        }
    }
}

fn check_node_health(storage: &Mutex<Storage>) -> bool {
    let storage_guard = storage.lock().unwrap();
    storage_guard.is_available()
}

pub type ConsumerGroups = HashMap<String, ConsumerGroup>;

impl ConsumerGroup {
    pub fn reset_assignments(&mut self) {
        if let Ok(mut map) = self.assignments.lock() {
            map.clear();
        }
    }
}

fn recover_node(storage: &Mutex<Storage>, consumer_groups: &Mutex<ConsumerGroups>) {
    let mut storage_guard = storage.lock().unwrap();
    if let Err(e) = storage_guard.reinitialize() {
        eprintln!("Storage initialization failed: {}", e);
    }

    let mut groups_guard = consumer_groups.lock().unwrap();
    for group in groups_guard.values_mut() {
        group.reset_assignments();
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
        let peers = HashMap::new(); //TODO it reads from the settings
        let leader_election = LeaderElection::new(id, peers);
        let storage = Storage::new(storage_path).expect("Failed to initialize storage");

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
            message_queue: Arc::new(MessageQueue::new()),
        };
        broker.monitor_nodes();
        broker
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
        let storage = Arc::clone(&self.storage);
        std::thread::spawn(move || {
            if let Ok(mut storage_guard) = storage.lock() {
                if !storage_guard.is_available() {
                    if let Ok(()) = storage_guard.reinitialize() {
                        storage_guard.available = true;
                    }
                }
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

    /// Rotates the logs.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::Broker;
    ///
    /// let mut broker = Broker::new("broker1", 3, 2, "logs");
    /// broker.create_topic("test_topic", None).unwrap();
    /// broker.publish_with_ack("test_topic", "test_message".to_string(), None).unwrap();
    /// broker.rotate_logs().unwrap();
    /// ```
    pub fn rotate_logs(&mut self) -> Result<(), BrokerError> {
        self.storage.lock().unwrap().rotate_logs()?;
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
    use std::fs;
    use std::path::Path;
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;
    use std::time::Duration;

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
    fn test_broker_leader_election() {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let node1 = Node {
            data: Arc::new(Mutex::new(Vec::new())),
        };
        let node2 = Node {
            data: Arc::new(Mutex::new(Vec::new())),
        };

        broker.add_node("node1".to_string(), node1);
        broker.add_node("node2".to_string(), node2);

        let elected = broker.start_election();

        assert!(elected);

        let leader = broker.leader.lock().unwrap();
        assert!(leader.is_some());
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
        let test_log = "test_logs_rotation";
        setup_test_logs(test_log);

        let mut broker = Broker::new("broker1", 3, 2, test_log);
        broker.create_topic("test_topic", None).unwrap();
        broker
            .publish_with_ack("test_topic", "test_message".to_string(), None)
            .unwrap();

        broker.rotate_logs().unwrap();
        broker.cleanup_logs().unwrap();

        let storage = Storage::new(test_log).unwrap();
        let messages = storage.read_messages().unwrap();
        assert!(messages.is_empty());

        cleanup_test_logs(test_log);
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
    fn test_rotate_logs_with_invalid_storage() {
        let temp_dir = format!("/tmp/test_logs_{}", std::process::id());
        let invalid_path = format!("{}/invalid", temp_dir);

        let result = std::panic::catch_unwind(|| {
            let mut broker = Broker::new("broker1", 3, 2, &invalid_path);
            broker.rotate_logs()
        });

        assert!(
            result.is_err(),
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
    fn test_auto_recovery() {
        let storage = Arc::new(Mutex::new(Storage::new("test_db_path").unwrap()));
        let mut broker = Broker::new("broker_id", 1, 1, "test_db_path");
        broker.storage = storage.clone();

        // Simulate a disability
        {
            let mut storage_guard = storage.lock().unwrap();
            storage_guard.available = false;
        }

        // Perform automatic recovery
        broker.monitor_nodes();

        // Check recovery
        thread::sleep(Duration::from_millis(100));
        let storage_guard = storage.lock().unwrap();
        assert!(storage_guard.is_available());
    }
}
