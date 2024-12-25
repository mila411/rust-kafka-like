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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
    /// use rust_kafka_like::broker::Broker;
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

        Broker {
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
        }
    }

    pub fn add_node(&self, node_id: String, node: Node) {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.insert(node_id, node);
        drop(nodes); // ロックを解放
        self.rebalance_partitions();
    }

    pub fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.remove(node_id);
        drop(nodes); // ロックを解放
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

    /// Starts the leader election process.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_kafka_like::broker::Broker;
    ///
    /// let broker = Broker::new("broker1", 3, 2, "logs");
    /// let elected = broker.start_election();
    /// assert!(elected);
    /// ```
    pub fn start_election(&self) -> bool {
        self.leader_election.start_election()
    }

    /// Checks if the broker is the leader.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_kafka_like::broker::Broker;
    ///
    /// let broker = Broker::new("broker1", 3, 2, "logs");
    /// broker.start_election();
    /// assert!(broker.is_leader());
    /// ```
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
    /// use rust_kafka_like::broker::Broker;
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
    /// use rust_kafka_like::broker::Broker;
    /// use rust_kafka_like::subscriber::types::Subscriber;
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
    /// use rust_kafka_like::broker::Broker;
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

            // コンシューマーグループへのメッセージ配信
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
    /// use rust_kafka_like::broker::Broker;
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

pub struct Node {
    // ノードの情報
}

pub struct Partition {
    pub node_id: String,
    // パーティションの情報
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
        let test_log = "test_logs_leader";
        setup_test_logs(test_log);

        let broker = Broker::new("broker1", 3, 2, test_log);
        assert!(!broker.is_leader());

        let elected = broker.start_election();
        assert!(elected);
        assert!(broker.is_leader());

        cleanup_test_logs(test_log);
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

        let messages = Storage::read_messages(test_log).unwrap();
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

        let messages = Storage::read_messages(test_log).unwrap();
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
        println!("テスト開始");
        let mut broker = Broker::new("broker1", 3, 2, "logs");
        broker.create_topic("test_topic", None).unwrap();

        let received_count = Arc::new((Mutex::new(0), Condvar::new()));
        let total_messages = 10;

        // メッセージ受信を追跡
        let received_messages = Arc::new(Mutex::new(Vec::new()));

        // 各コンシューマーの設定
        for i in 0..2 {
            let received_messages = Arc::clone(&received_messages);
            let received_count = Arc::clone(&received_count);

            let subscriber = Subscriber::new(
                &format!("consumer_{}", i),
                Box::new(move |msg: String| {
                    println!("Consumer {} received: {}", i, msg);
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
            thread::sleep(Duration::from_millis(100)); // リバランス用の待機
        }

        println!("メッセージ送信開始");
        for i in 0..total_messages {
            let message = format!("message_{}", i);
            println!("Sending: {}", message);
            broker
                .publish_with_ack("test_topic", message, None)
                .unwrap();
            thread::sleep(Duration::from_millis(10)); // 送信間隔を空ける
        }

        // 完了待ち
        let (lock, cvar) = &*received_count;
        let timeout = Duration::from_secs(5);
        let result = cvar
            .wait_timeout_while(lock.lock().unwrap(), timeout, |&mut count| {
                count < total_messages
            })
            .unwrap();

        if result.1.timed_out() {
            panic!("テストがタイムアウトしました: 受信数 {}", *result.0);
        }

        let received = received_messages.lock().unwrap();
        println!("Received messages: {:?}", *received);
        assert_eq!(received.len(), total_messages);
    }
}
