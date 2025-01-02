// These options control how the docs look at a crate level.
// See https://doc.rust-lang.org/rustdoc/write-documentation/the-doc-attribute.html
// for more information.
#![doc(html_logo_url = "https://raw.githubusercontent.com/mila411/pilgrimage/refs/heads/main/.github/images/logo.png")]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/mila411/pilgrimage/refs/heads/main/.github/images/logo.png")]

/// Trigger for CI testing line
pub mod auth;
pub mod broker;
pub mod crypto;
pub mod message;
pub mod schema;
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
