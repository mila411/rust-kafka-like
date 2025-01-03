use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum BrokerError {
    TopicError(String),
    PartitionError(String),
    AckError(String),
    IoError(std::io::Error),
    ScalingError(String),
    DuplicateMessage,
    TransactionError,
}

impl From<std::io::Error> for BrokerError {
    /// Converts a standard I/O error into a BrokerError.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::error::BrokerError;
    /// use std::io;
    ///
    /// let io_error = io::Error::new(io::ErrorKind::Other, "an I/O error");
    /// let broker_error: BrokerError = io_error.into();
    /// if let BrokerError::IoError(err) = broker_error {
    ///     assert_eq!(err.to_string(), "an I/O error");
    /// }
    /// ```
    fn from(error: std::io::Error) -> Self {
        BrokerError::IoError(error)
    }
}

impl fmt::Display for BrokerError {
    /// Formats the BrokerError for display purposes.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::error::BrokerError;
    ///
    /// let error = BrokerError::TopicError("topic not found".to_string());
    /// assert_eq!(format!("{}", error), "Topic error: topic not found");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BrokerError::TopicError(msg) => write!(f, "Topic error: {}", msg),
            BrokerError::PartitionError(msg) => write!(f, "Partition error: {}", msg),
            BrokerError::AckError(msg) => write!(f, "Acknowledgment error: {}", msg),
            BrokerError::IoError(err) => write!(f, "IO error: {}", err),
            BrokerError::ScalingError(msg) => write!(f, "Scaling Error: {}", msg),
            BrokerError::DuplicateMessage => write!(f, "Duplicate message ID"),
            BrokerError::TransactionError => write!(f, "Transaction failed"),
        }
    }
}

impl Error for BrokerError {
    /// Returns the source of the error, if any.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::error::BrokerError;
    /// use std::error::Error;
    /// use std::io;
    ///
    /// let io_error = io::Error::new(io::ErrorKind::Other, "an I/O error");
    /// let broker_error: BrokerError = io_error.into();
    /// assert!(broker_error.source().is_some());
    /// ```
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            BrokerError::IoError(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_topic_error() {
        let error = BrokerError::TopicError("Test topic error".to_string());
        assert_eq!(format!("{}", error), "Topic error: Test topic error");
    }

    #[test]
    fn test_partition_error() {
        let error = BrokerError::PartitionError("Test partition error".to_string());
        assert_eq!(
            format!("{}", error),
            "Partition error: Test partition error"
        );
    }

    #[test]
    fn test_ack_error() {
        let error = BrokerError::AckError("Test ack error".to_string());
        assert_eq!(format!("{}", error), "Acknowledgment error: Test ack error");
    }

    #[test]
    fn test_io_error() {
        let io_error = io::Error::new(io::ErrorKind::Other, "Test IO error");
        let error = BrokerError::IoError(io_error);
        assert_eq!(format!("{}", error), "IO error: Test IO error");
    }

    #[test]
    fn test_from_io_error() {
        let io_error = io::Error::new(io::ErrorKind::Other, "Test IO error");
        let error: BrokerError = io_error.into();
        assert_eq!(format!("{}", error), "IO error: Test IO error");
    }
}
