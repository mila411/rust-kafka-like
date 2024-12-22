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
}

impl Debug for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Partition")
            .field("id", &self.id)
            .field("messages", &self.messages)
            .finish()
    }
}

impl Topic {
    pub fn new(name: &str, num_partitions: usize) -> Self {
        let partitions = (0..num_partitions)
            .map(|i| Partition {
                id: i,
                messages: Vec::new(),
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
        // TODO: Because the Subscriber does not have an ID, the implementation of this function needs to be reviewed
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
