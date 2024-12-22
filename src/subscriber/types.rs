pub type Subscriber = Box<dyn Fn(String) + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscriber_creation() {
        let subscriber: Subscriber = Box::new(|msg| {
            println!("Received message: {}", msg);
        });

        // Verify the subscriber can be called
        subscriber("test message".to_string());
    }

    #[test]
    fn test_subscriber_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Subscriber>();
    }
}
