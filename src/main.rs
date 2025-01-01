use tokio;
mod web_console;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    web_console::run_server().await
}
