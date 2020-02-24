use log::info;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let mut listener = TcpListener::bind("127.0.0.1:10809").await?;

    let db = sled::open("test.db")?;
    let store = rnbd::export::TmpStore::default();
    // let db_store = rnbd::export::SledStore::new(db);

    loop {
        let (socket, _) = listener.accept().await?;

        let hs = rnbd::HandshakeCon::new(socket, store.clone());
        tokio::spawn(async move {
            let handshake_result = hs.newstyle_handshake().await;
            let cl = handshake_result.unwrap_or_else(|e| {
                info!("Hanshake error {:?}", e);
                None
            });
            if let Some(mut ec) = cl {
                if let Err(e) = ec.serve_export().await {
                    info!("Export serving error {:?}", e);
                };
            }
        });
    }
}
