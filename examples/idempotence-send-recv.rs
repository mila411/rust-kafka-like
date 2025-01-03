use pilgrimage::broker::Broker;
use pilgrimage::broker::storage::Storage;
use pilgrimage::broker::transaction::Transaction;
use pilgrimage::message::ack::Message;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio;
use uuid::Uuid;

#[tokio::main]
async fn main() {
    let token_manager = TokenManager::new();
    let rbac = Rbac::new();
    let encryptor = Encryptor::new();

    let username = "user1";
    let roles = vec!["admin".to_string()];
    let token = token_manager.generate_token(username, roles).unwrap();

    if let Ok(claims) = token_manager.verify_token(&token) {
        let broker = Broker::new("broker1", 3, 2, "logs");
        let storage = Arc::new(Mutex::new(Storage::new("messages.txt").unwrap()));
        let mut transaction = Transaction::new(storage.clone());

        if rbac.has_permission(&claims.sub, &Permission::Write) {
            let message = format!("Hello from {}", username);
            match encryptor.encrypt(message.as_bytes()) {
                Ok(encrypted_data) => {
                    let msg = Message {
                        id: Uuid::new_v4(),
                        content: String::from_utf8_lossy(&encrypted_data).to_string(),
                        timestamp: SystemTime::now(),
                    };
                    if !broker.is_message_processed(&msg.id) {
                        transaction.add_message(msg.clone());
                        match broker.send_message(msg.content.clone()).await {
                            Ok(_) => {
                                transaction.commit().unwrap();
                                println!("Encrypted message sent and saved successfully");
                            }
                            Err(e) => {
                                transaction.rollback().unwrap();
                                println!("Failed to send message: {}", e);
                            }
                        }
                    } else {
                        println!("Message already processed");
                    }
                }
                Err(e) => println!("Encryption failed: {}", e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn test_exactly_once() {
        let storage = Arc::new(Mutex::new(Storage::new("messages.txt").unwrap()));
        let broker = Broker::new("broker1", 3, 2, "logs").await;
        let message_id = Uuid::new_v4();
        let message = Message {
            id: message_id,
            content: "Test message".to_string(),
            timestamp: SystemTime::now(),
        };

        // 初回メッセージ送信
        assert!(!broker.is_message_processed(&message.id));
        broker.send_message(message.content.clone()).await.unwrap();
        broker.save_message(&message).unwrap();
        assert!(broker.is_message_processed(&message.id));

        // 再度同じメッセージを送信
        broker.send_message(message.content.clone()).await.unwrap();
        assert!(broker.is_message_processed(&message.id));
    }
}

struct TokenManager;
impl TokenManager {
    fn new() -> Self {
        TokenManager
    }

    fn generate_token(&self, _username: &str, _roles: Vec<String>) -> Result<String, String> {
        // トークン生成ロジック
        Ok("dummy_token".to_string())
    }

    fn verify_token(&self, _token: &str) -> Result<Claims, String> {
        // トークン検証ロジック
        Ok(Claims {
            sub: "user1".to_string(),
            exp: 10000000000,
        })
    }
}

struct Rbac;
impl Rbac {
    fn new() -> Self {
        Rbac
    }

    fn has_permission(&self, _user: &str, _permission: &Permission) -> bool {
        // 権限チェックロジック
        true
    }
}

struct Encryptor;
impl Encryptor {
    fn new() -> Self {
        Encryptor
    }

    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        // 暗号化ロジック
        Ok(data.to_vec())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
}

enum Permission {
    Write,
}
