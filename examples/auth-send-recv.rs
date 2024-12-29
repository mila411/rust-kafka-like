use pilgrimage::auth::authentication::{Authenticator, BasicAuthenticator};
use pilgrimage::auth::authorization::{Permission, RoleBasedAccessControl};
use pilgrimage::auth::token::TokenManager;
use pilgrimage::pubsub::broker::Broker;
use pilgrimage::pubsub::message::Message;

fn main() {
    // 認証のセットアップ
    let mut authenticator = BasicAuthenticator::new();
    authenticator.add_user("user1", "password1");

    // 承認のセットアップ
    let mut rbac = RoleBasedAccessControl::new();
    rbac.add_role("admin", vec![
        Permission::Read,
        Permission::Write,
        Permission::Admin,
    ]);
    rbac.add_role("user", vec![Permission::Read]);
    rbac.assign_role("user1", "admin");

    // トークンマネージャーのセットアップ
    let token_manager = TokenManager::new(b"secret");

    // ユーザーの認証
    let username = "user1";
    let password = "password1";
    if authenticator.authenticate(username, password).unwrap() {
        println!("User {} authenticated successfully", username);

        // トークンの生成
        let roles = vec!["admin".to_string()];
        let token = token_manager
            .generate_token(username, roles.clone())
            .unwrap();
        println!("Generated token: {}", token);

        // トークンの検証
        let claims = token_manager.verify_token(&token).unwrap();
        println!("Token verified for user: {}", claims.sub);

        // 承認の確認
        if rbac.has_permission(&claims.sub, &Permission::Write) {
            println!("User {} has write permission", claims.sub);

            // PubSubのセットアップ
            let broker = Broker::new();
            broker.subscribe("topic1");

            // メッセージのパブリッシュ
            let message = Message::new("Hello, world!");
            broker.publish("topic1", message);

            // メッセージの取得
            let messages = broker.get_messages("topic1");
            for msg in messages {
                println!("Received message: {}", msg.content);
            }
        } else {
            println!("User {} does not have write permission", claims.sub);
        }
    } else {
        println!("Authentication failed for user {}", username);
    }
}
