pub mod broker;
pub mod message;
pub mod subscriber;

pub use broker::Broker;
pub use broker::error::BrokerError;
pub use broker::topic::Topic;
pub use message::ack::MessageAck;
pub use subscriber::types::Subscriber;

pub mod prelude {
    pub use crate::broker::Broker;
    pub use crate::broker::error::BrokerError;
    pub use crate::broker::topic::Topic;
    pub use crate::message::ack::MessageAck;
    pub use crate::subscriber::types::Subscriber;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_creation() {
        let broker = Broker::new("broker1", 3, 2, "test_logs");
        assert_eq!(broker.id, "broker1");
        assert_eq!(broker.num_partitions, 3);
        assert_eq!(broker.replication_factor, 2);
    }

    #[test]
    fn test_create_topic() {
        let mut broker = Broker::new("broker1", 3, 2, "test_logs");
        broker.create_topic("test_topic", None).unwrap();
        assert!(broker.topics.contains_key("test_topic"));
    }

    #[test]
    fn test_subscribe_and_publish() {
        let mut broker = Broker::new("broker1", 3, 2, "test_logs");
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
    }

    #[test]
    fn test_message_ack() {
        let ack = MessageAck::new(1, "test_topic", 0);
        assert_eq!(ack.id, 1);
        assert_eq!(ack.topic, "test_topic");
        assert_eq!(ack.partition, 0);
    }

    #[test]
    fn test_broker_error() {
        let error = BrokerError::TopicError("Test topic error".to_string());
        assert_eq!(format!("{}", error), "Topic error: Test topic error");
    }
}
