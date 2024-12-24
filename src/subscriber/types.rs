pub struct Subscriber {
    pub id: String,
    pub callback: Box<dyn Fn(String) + Send + Sync>,
}

impl Subscriber {
    /// Creates a new subscriber instance.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the subscriber.
    /// * `callback` - The callback function to be called when a message is received.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_kafka_like::subscriber::types::Subscriber;
    ///
    /// let subscriber = Subscriber::new(
    ///     "test_subscriber",
    ///     Box::new(|msg| {
    ///         println!("Received message: {}", msg);
    ///     }),
    /// );
    /// assert_eq!(subscriber.id, "test_subscriber");
    /// ```
    pub fn new(id: &str, callback: Box<dyn Fn(String) + Send + Sync>) -> Self {
        Subscriber {
            id: id.to_string(),
            callback,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscriber_creation() {
        let subscriber = Subscriber::new(
            "test_subscriber",
            Box::new(|msg| {
                println!("Received message: {}", msg);
            }),
        );

        // Verify the subscriber can be called with its callback
        (subscriber.callback)("test message".to_string());
        assert_eq!(subscriber.id, "test_subscriber");
    }

    #[test]
    fn test_subscriber_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Subscriber>();
    }
}
