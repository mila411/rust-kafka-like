pub mod error;
pub mod leader;
pub mod storage;
pub mod topic;

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
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_kafka_like::broker::Broker;
    /// use rust_kafka_like::subscriber::types::Subscriber;
    ///
    /// let mut broker = Broker::new("broker1", 3, 2, "logs");
    /// broker.create_topic("test_topic", None).unwrap();
    /// let subscriber = Subscriber::new("sub1", Box::new(|msg: String| {
    ///     println!("Received message: {}", msg);
    /// }));
    /// broker.subscribe("test_topic", subscriber).unwrap();
    /// ```
    pub fn subscribe(
        &mut self,
        topic_name: &str,
        subscriber: Subscriber,
    ) -> Result<(), BrokerError> {
        if let Some(topic) = self.topics.get_mut(topic_name) {
            topic.add_subscriber(subscriber);
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

            // Write the message to storage
            self.storage.lock().unwrap().write_message(&message)?;

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

    /// Cleans up the old logs.
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
    /// broker.cleanup_logs().unwrap();
    /// ```
    pub fn cleanup_logs(&self) -> Result<(), BrokerError> {
        self.storage.lock().unwrap().cleanup_logs()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

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
        broker.subscribe("test_topic", subscriber).unwrap();

        let ack = broker
            .publish_with_ack("test_topic", "test_message".to_string(), None)
            .unwrap();
        assert_eq!(ack.topic, "test_topic");

        cleanup_test_logs(test_log);
    }
}
