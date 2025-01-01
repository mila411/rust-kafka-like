use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use pilgrimage::{Broker, broker::Node};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
struct BrokerWrapper {
    inner: Arc<Mutex<Broker>>,
}

impl BrokerWrapper {
    fn new(broker: Broker) -> Self {
        Self {
            inner: Arc::new(Mutex::new(broker)),
        }
    }

    fn send_message(&self, message: String) {
        if let Ok(broker) = self.inner.lock() {
            broker.send_message(message);
        }
    }

    fn receive_message(&self) -> Option<String> {
        if let Ok(broker) = self.inner.lock() {
            broker.receive_message()
        } else {
            None
        }
    }

    fn is_healthy(&self) -> bool {
        self.inner.lock().is_ok()
    }
}

#[derive(Clone)]
pub struct AppState {
    brokers: Arc<Mutex<HashMap<String, BrokerWrapper>>>,
}

#[derive(Deserialize)]
struct StartRequest {
    id: String,
    partitions: usize,
    replication: usize,
    storage: String,
}

#[derive(Deserialize)]
struct StopRequest {
    id: String,
}

#[derive(Deserialize)]
struct SendRequest {
    id: String,
    message: String,
}

#[derive(Deserialize)]
struct ConsumeRequest {
    id: String,
}

#[derive(Deserialize)]
struct StatusRequest {
    id: String,
}

async fn start_broker(info: web::Json<StartRequest>, data: web::Data<AppState>) -> impl Responder {
    let mut brokers_lock = data.brokers.lock().unwrap();

    if brokers_lock.contains_key(&info.id) {
        return HttpResponse::BadRequest().json("Broker is already running");
    }

    let broker = Broker::new(&info.id, info.partitions, info.replication, &info.storage);

    let node = Node {
        id: "node1".to_string(),
        address: "127.0.0.1:8080".to_string(),
        is_active: true,
        data: Arc::new(Mutex::new(Vec::new())),
    };
    broker.add_node("node1".to_string(), node);

    let wrapper = BrokerWrapper::new(broker);
    brokers_lock.insert(info.id.clone(), wrapper);

    HttpResponse::Ok().json("Broker started")
}

async fn stop_broker(info: web::Json<StopRequest>, data: web::Data<AppState>) -> impl Responder {
    let mut brokers_lock = data.brokers.lock().unwrap();

    if brokers_lock.remove(&info.id).is_some() {
        HttpResponse::Ok().json("Broker stopped")
    } else {
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

async fn send_message(info: web::Json<SendRequest>, data: web::Data<AppState>) -> impl Responder {
    let brokers_lock = data.brokers.lock().unwrap();

    if let Some(broker) = brokers_lock.get(&info.id) {
        broker.send_message(info.message.clone());
        HttpResponse::Ok().json("Message sent")
    } else {
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

async fn consume_messages(
    info: web::Json<ConsumeRequest>,
    data: web::Data<AppState>,
) -> impl Responder {
    let brokers_lock = data.brokers.lock().unwrap();

    if let Some(broker) = brokers_lock.get(&info.id) {
        if let Some(message) = broker.receive_message() {
            HttpResponse::Ok().json(message)
        } else {
            HttpResponse::Ok().json("No messages available")
        }
    } else {
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

async fn broker_status(
    info: web::Json<StatusRequest>,
    data: web::Data<AppState>,
) -> impl Responder {
    let brokers_lock = data.brokers.lock().unwrap();

    if let Some(broker) = brokers_lock.get(&info.id) {
        HttpResponse::Ok().json(broker.is_healthy())
    } else {
        HttpResponse::BadRequest().json("No broker is running with the given ID")
    }
}

pub async fn run_server() -> std::io::Result<()> {
    let state = AppState {
        brokers: Arc::new(Mutex::new(HashMap::new())),
    };

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .route("/start", web::post().to(start_broker))
            .route("/stop", web::post().to(stop_broker))
            .route("/send", web::post().to(send_message))
            .route("/consume", web::post().to(consume_messages))
            .route("/status", web::post().to(broker_status))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{App, test, web};
    use serde_json::json;

    #[actix_rt::test]
    async fn test_start_broker() {
        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/start", web::post().to(start_broker)),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(&json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[actix_rt::test]
    async fn test_stop_broker() {
        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/start", web::post().to(start_broker))
                .route("/stop", web::post().to(stop_broker)),
        )
        .await;

        // Start the broker first
        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(&json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

        // Now stop the broker
        let req = test::TestRequest::post()
            .uri("/stop")
            .set_json(&json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[actix_rt::test]
    async fn test_send_message() {
        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/start", web::post().to(start_broker))
                .route("/send", web::post().to(send_message)),
        )
        .await;

        // Start the broker first
        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(&json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

        // Now send a message
        let req = test::TestRequest::post()
            .uri("/send")
            .set_json(&json!({
                "id": "broker1",
                "message": "Hello, World!"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[actix_rt::test]
    async fn test_consume_messages() {
        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/start", web::post().to(start_broker))
                .route("/send", web::post().to(send_message))
                .route("/consume", web::post().to(consume_messages)),
        )
        .await;

        // Start the broker first
        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(&json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

        // Send a message
        let req = test::TestRequest::post()
            .uri("/send")
            .set_json(&json!({
                "id": "broker1",
                "message": "Hello, World!"
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

        // Now consume the message
        let req = test::TestRequest::post()
            .uri("/consume")
            .set_json(&json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[actix_rt::test]
    async fn test_broker_status() {
        let state = AppState {
            brokers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut app = test::init_service(
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/start", web::post().to(start_broker))
                .route("/status", web::post().to(broker_status)),
        )
        .await;

        // Start the broker first
        let req = test::TestRequest::post()
            .uri("/start")
            .set_json(&json!({
                "id": "broker1",
                "partitions": 3,
                "replication": 2,
                "storage": "/tmp/broker1"
            }))
            .to_request();

        let _ = test::call_service(&mut app, req).await;

        // Now check the broker status
        let req = test::TestRequest::post()
            .uri("/status")
            .set_json(&json!({
                "id": "broker1"
            }))
            .to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }
}
