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
    pub next_offset: usize,
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
                next_offset: 0, // 初期化
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
        // TODO: Because the Subscriber does not have an ID, the implementation of this function needs to be reviewed.
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
            partition.add_message(message.clone());

            // Add a message to the replica
            for replica in &mut partition.replicas {
                replica.messages.push(message.clone());
            }

            // Notification to all subscribers
            for subscriber in &self.subscribers {
                (subscriber.callback)(message.clone());
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

impl Partition {
    // Add a message.
    // Increment the offset to guarantee message order.
    pub fn add_message(&mut self, message: String) {
        self.messages.push(message);
        self.next_offset += 1;
    }

    // Returns the messages after the specified offset (start_offset).
    // This implementation guarantees the order of the messages within the partition.
    pub fn fetch_messages_in_order(&self, start_offset: usize) -> &[String] {
        if start_offset >= self.messages.len() {
            &[]
        } else {
            &self.messages[start_offset..]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscriber::types::Subscriber;
    use std::sync::{Arc, Mutex};

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
        let subscriber = Subscriber::new(
            "test_sub",
            Box::new(|msg: String| {
                println!("Received message: {}", msg);
            }),
        );
        topic.add_subscriber(subscriber);
        assert_eq!(topic.subscribers.len(), 1);
    }

    #[test]
    fn test_publish_message() {
        let mut topic = Topic::new("test_topic", 3, 2);
        let subscriber = Subscriber::new(
            "test_sub",
            Box::new(|msg: String| {
                println!("Received test message: {}", msg);
            }),
        );
        topic.add_subscriber(subscriber);

        let result = topic.publish("test_message".to_string(), None);
        assert!(result.is_ok());
        let partition_id = result.unwrap();
        assert!(partition_id < 3);
        assert_eq!(topic.partitions[partition_id].messages[0], "test_message");
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

    #[test]
    fn test_message_ordering() {
        let mut topic = Topic::new("test_topic", 1, 1);
        topic.publish("message_1".to_string(), None).unwrap();
        topic.publish("message_2".to_string(), None).unwrap();
        topic.publish("message_3".to_string(), None).unwrap();

        let partition = &topic.partitions[0];
        let messages = partition.fetch_messages_in_order(0);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0], "message_1");
        assert_eq!(messages[1], "message_2");
        assert_eq!(messages[2], "message_3");
    }

    #[test]
    fn test_fetch_messages_from_offset() {
        let mut topic = Topic::new("test_topic", 1, 1);
        topic.publish("message_1".to_string(), None).unwrap();
        topic.publish("message_2".to_string(), None).unwrap();
        topic.publish("message_3".to_string(), None).unwrap();

        let partition = &topic.partitions[0];
        let messages = partition.fetch_messages_in_order(1);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], "message_2");
        assert_eq!(messages[1], "message_3");
    }

    #[test]
    fn test_subscriber_receives_messages() {
        let mut topic = Topic::new("test_topic", 1, 1);
        let received_messages = Arc::new(Mutex::new(Vec::new()));

        let subscriber = Subscriber::new("sub1", {
            let received_messages = Arc::clone(&received_messages);
            Box::new(move |msg: String| {
                received_messages.lock().unwrap().push(msg);
            })
        });
        topic.add_subscriber(subscriber);

        topic.publish("message_1".to_string(), None).unwrap();
        topic.publish("message_2".to_string(), None).unwrap();

        let received_messages = received_messages.lock().unwrap();
        assert_eq!(received_messages.len(), 2);
        assert_eq!(received_messages[0], "message_1");
        assert_eq!(received_messages[1], "message_2");
    }
}
