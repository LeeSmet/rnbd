use log::info;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let listener = TcpListener::bind("127.0.0.1:10809").await?;

    //     let db1 = sled::open("test1.db")?;
    //     let db2 = sled::open("test2.db")?;
    //     let db3 = sled::open("test3.db")?;
    //     let db4 = sled::open("test4.db")?;
    //     let db5 = sled::open("test5.db")?;
    // let store = rnbd::export::TmpStore::default();
    let store = rnbd::zdb_aio::Zdb::new(9900).await;
    // let db_store = rnbd::export::SledStore::new(db);
    // let db_store = rnbd::export::SledStore::new(sled::open("test.db").unwrap());
    //    use rnbd::export::SledStore;
    //     let rse_store = rnbd::export::ErasureExportStore::new(vec![
    //         SledStore::new(db1),
    //         SledStore::new(db2),
    //         SledStore::new(db3),
    //         SledStore::new(db4),
    //         SledStore::new(db5),
    //     ]);

    log::trace!("store created");

    loop {
        let (socket, _) = listener.accept().await?;

        log::trace!("connection accepted");

        //let hs = rnbd::HandshakeCon::new(socket, rse_store.clone());
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
