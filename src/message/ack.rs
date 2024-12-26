use std::fmt;

#[derive(Debug, Clone)]
pub struct MessageAck {
    pub id: u64,
    pub topic: String,
    pub partition: usize,
}

impl MessageAck {
    /// Creates a new MessageAck instance.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the acknowledgment.
    /// * `topic` - The topic of the message.
    /// * `partition` - The partition of the message.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::message::ack::MessageAck;
    ///
    /// let ack = MessageAck::new(1, "test_topic", 0);
    /// assert_eq!(ack.id, 1);
    /// assert_eq!(ack.topic, "test_topic");
    /// assert_eq!(ack.partition, 0);
    /// ```
    pub fn new(id: u64, topic: &str, partition: usize) -> Self {
        MessageAck {
            id,
            topic: topic.to_string(),
            partition,
        }
    }
}

impl fmt::Display for MessageAck {
    /// Formats the MessageAck for display purposes.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::message::ack::MessageAck;
    ///
    /// let ack = MessageAck::new(1, "test_topic", 0);
    /// assert_eq!(format!("{}", ack), "MessageAck { id: 1, topic: test_topic, partition: 0 }");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MessageAck {{ id: {}, topic: {}, partition: {} }}",
            self.id, self.topic, self.partition
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_ack_creation() {
        let ack = MessageAck::new(1, "test-topic", 0);
        assert_eq!(ack.id, 1);
        assert_eq!(ack.topic, "test-topic");
        assert_eq!(ack.partition, 0);
    }

    #[test]
    fn test_message_ack_display() {
        let ack = MessageAck::new(1, "test-topic", 0);
        assert_eq!(
            format!("{}", ack),
            "MessageAck { id: 1, topic: test-topic, partition: 0 }"
        );
    }

    #[test]
    fn test_message_ack_clone() {
        let ack = MessageAck::new(1, "test-topic", 0);
        let cloned = ack.clone();
        assert_eq!(ack.id, cloned.id);
        assert_eq!(ack.topic, cloned.topic);
        assert_eq!(ack.partition, cloned.partition);
    }
}
