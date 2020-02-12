use log::info;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let mut listener = TcpListener::bind("127.0.0.1:10809").await?;

    let db = sled::open("test.db")?;

    loop {
        let (socket, _) = listener.accept().await?;

        let mut server = rnbd::Server::new(socket, db.clone());
        tokio::spawn(async move {
            let handshake_result = server.newstyle_handshake().await;
            let cl = handshake_result.unwrap_or_else(|e| {
                info!("Hanshake error {:?}", e);
                None
            });
            if cl.is_some() {
                if let Err(e) = server.serve_export().await {
                    info!("Export serving error {:?}", e);
                };
            }
        });
    }
}
