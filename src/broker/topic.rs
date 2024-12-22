use crate::broker::error::BrokerError;
use crate::subscriber::types::Subscriber;
use std::fmt::{self, Debug};

pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
    pub subscribers: Vec<Subscriber>,
}

impl Debug for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Topic")
            .field("name", &self.name)
            .field("partitions", &self.partitions)
            .field("subscribers", &self.subscribers.len())
            .finish()
    }
}

pub struct Partition {
    pub id: usize,
    pub messages: Vec<String>,
    pub replicas: Vec<Replica>,
}

impl Debug for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Partition")
            .field("id", &self.id)
            .field("messages", &self.messages)
            .field("replicas", &self.replicas)
            .finish()
    }
}

pub struct Replica {
    pub broker_id: String,
    pub messages: Vec<String>,
}

impl Debug for Replica {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Replica")
            .field("broker_id", &self.broker_id)
            .field("messages", &self.messages)
            .finish()
    }
}

impl Topic {
    pub fn new(name: &str, num_partitions: usize, replication_factor: usize) -> Self {
        let partitions = (0..num_partitions)
            .map(|i| Partition {
                id: i,
                messages: Vec::new(),
                replicas: (0..replication_factor)
                    .map(|_| Replica {
                        broker_id: String::new(),
                        messages: Vec::new(),
                    })
                    .collect(),
            })
            .collect();

        Topic {
            name: name.to_string(),
            partitions,
            subscribers: Vec::new(),
        }
    }

    pub fn add_subscriber(&mut self, subscriber: Subscriber) {
        self.subscribers.push(subscriber);
    }

    pub fn remove_subscriber(&mut self, _subscriber_id: &str) {
        // TODO: Because the Subscriber does not have an ID, the implementation of this function needs to be reviewed.。
    }

    pub fn publish(
        &mut self,
        message: String,
        partition_key: Option<&str>,
    ) -> Result<usize, BrokerError> {
        let partition_id = match partition_key {
            Some(key) => self.get_partition_id(key),
            None => self.get_next_partition(),
        };

        if let Some(partition) = self.partitions.get_mut(partition_id) {
            partition.messages.push(message.clone());

            // Add a message to the replica
            for replica in &mut partition.replicas {
                replica.messages.push(message.clone());
            }

            // Notification to all subscribers
            for subscriber in &self.subscribers {
                (subscriber)(message.clone());
            }

            Ok(partition_id)
        } else {
            Err(BrokerError::PartitionError(format!(
                "Invalid partition id: {}",
                partition_id
            )))
        }
    }

    fn get_partition_id(&self, key: &str) -> usize {
        let hash = key.bytes().fold(0u64, |acc, b| acc.wrapping_add(b as u64));
        (hash % self.partitions.len() as u64) as usize
    }

    fn get_next_partition(&self) -> usize {
        use std::time::SystemTime;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        (now % self.partitions.len() as u128) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscriber::types::Subscriber;

    #[test]
    fn test_topic_creation() {
        let topic = Topic::new("test_topic", 3, 2);
        assert_eq!(topic.name, "test_topic");
        assert_eq!(topic.partitions.len(), 3);
        assert_eq!(topic.partitions[0].replicas.len(), 2);
    }

    #[test]
    fn test_add_subscriber() {
        let mut topic = Topic::new("test_topic", 3, 2);
        let subscriber: Subscriber = Box::new(|msg: String| {
            println!("Received message: {}", msg);
        });
        topic.add_subscriber(subscriber);
        assert_eq!(topic.subscribers.len(), 1);
    }

    #[test]
    fn test_publish_message() {
        let mut topic = Topic::new("test_topic", 3, 2);
        let subscriber: Subscriber = Box::new(|msg: String| {
            println!("Received message: {}", msg);
        });
        topic.add_subscriber(subscriber);

        let result = topic.publish("test_message".to_string(), None);
        assert!(result.is_ok());
        let partition_id = result.unwrap();
        assert!(partition_id < 3);
        assert_eq!(topic.partitions[partition_id].messages.len(), 1);
        assert_eq!(topic.partitions[partition_id].messages[0], "test_message");
        assert_eq!(topic.partitions[partition_id].replicas[0].messages.len(), 1);
        assert_eq!(
            topic.partitions[partition_id].replicas[0].messages[0],
            "test_message"
        );
    }

    #[test]
    fn test_get_partition_id() {
        let topic = Topic::new("test_topic", 3, 2);
        let partition_id = topic.get_partition_id("key");
        assert!(partition_id < 3);
    }

    #[test]
    fn test_get_next_partition() {
        let topic = Topic::new("test_topic", 3, 2);
        let partition_id = topic.get_next_partition();
        assert!(partition_id < 3);
    }
}
