use pilgrimage::auth::authentication::{Authenticator, BasicAuthenticator};
use pilgrimage::auth::authorization::{Permission, RoleBasedAccessControl};
use pilgrimage::auth::token::TokenManager;
use pilgrimage::broker::Broker;

fn main() {
    let mut authenticator = BasicAuthenticator::new();
    authenticator.add_user("user1", "password1");

    let mut rbac = RoleBasedAccessControl::new();
    rbac.add_role("admin", vec![
        Permission::Read,
        Permission::Write,
        Permission::Admin,
    ]);
    rbac.assign_role("user1", "admin");

    let token_manager = TokenManager::new(b"secret");
    let username = "user1";
    let password = "password1";

    if authenticator.authenticate(username, password).unwrap() {
        println!("User {} authenticated successfully", username);

        let roles = vec!["admin".to_string()];
        let token = token_manager.generate_token(username, roles).unwrap();

        if let Ok(claims) = token_manager.verify_token(&token) {
            let broker = Broker::new("broker1", 3, 2, "logs");

            if rbac.has_permission(&claims.sub, &Permission::Write) {
                let message = format!("Hello from {}", username);
                let _ = broker.message_queue.send(message);
                println!("Message sent successfully");
            } else {
                println!("Permission denied");
            }
        }
    }
}
