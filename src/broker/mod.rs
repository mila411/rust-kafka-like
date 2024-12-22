pub mod error;
pub mod topic;

use crate::broker::error::BrokerError;
use crate::broker::topic::Topic;
use crate::message::ack::MessageAck;
use crate::subscriber::types::Subscriber;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Broker {
    #[allow(dead_code)]
    pub id: String,
    pub topics: HashMap<String, Topic>,
    pub num_partitions: usize,
    pub replication_factor: usize,
    pub term: AtomicU64,
}

impl Broker {
    pub fn new(id: &str, num_partitions: usize, replication_factor: usize) -> Self {
        Broker {
            id: id.to_string(),
            topics: HashMap::new(),
            num_partitions,
            replication_factor,
            term: AtomicU64::new(0),
        }
    }

    pub fn subscribe(&mut self, topic_name: &str, callback: Subscriber) -> Result<(), BrokerError> {
        if let Some(topic) = self.topics.get_mut(topic_name) {
            topic.add_subscriber(callback);
            Ok(())
        } else {
            Err(BrokerError::TopicError(format!(
                "Topic {} not found",
                topic_name
            )))
        }
    }

    pub fn publish_with_ack(
        &mut self,
        topic_name: &str,
        message: String,
        key: Option<&str>,
    ) -> Result<MessageAck, BrokerError> {
        if let Some(topic) = self.topics.get_mut(topic_name) {
            let partition_id = topic.publish(message, key)?;
            let ack = MessageAck::new(
                self.term.fetch_add(1, Ordering::SeqCst),
                topic_name,
                partition_id,
            );
            Ok(ack)
        } else {
            Err(BrokerError::TopicError(format!(
                "Topic {} not found",
                topic_name
            )))
        }
    }

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_creation() {
        let broker = Broker::new("broker1", 3, 2);
        assert_eq!(broker.id, "broker1");
        assert_eq!(broker.num_partitions, 3);
        assert_eq!(broker.replication_factor, 2);
    }

    #[test]
    fn test_create_topic() {
        let mut broker = Broker::new("broker1", 3, 2);
        broker.create_topic("test_topic", None).unwrap();
        assert!(broker.topics.contains_key("test_topic"));
    }

    #[test]
    fn test_subscribe_and_publish() {
        let mut broker = Broker::new("broker1", 3, 2);
        broker.create_topic("test_topic", None).unwrap();

        let subscriber = Subscriber::new(
            "test_sub",
            Box::new(|msg: String| {
                println!("Received message: {}", msg);
            }),
        );
        broker.subscribe("test_topic", subscriber).unwrap();

        let ack = broker
            .publish_with_ack("test_topic", "test_message".to_string(), None)
            .unwrap();
        assert_eq!(ack.topic, "test_topic");
    }
}
