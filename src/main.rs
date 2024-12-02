use common::FetchMessage;
use graph::GraphModel;
use pprof::protos::Message;
use simple_moving_average::{SumTreeSMA, SMA};
use std::ops::Sub;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, process};
use std::{fs::File, io::Write, thread};
use tokio::sync::{mpsc, Mutex};
use tracing::info;
use tracing_subscriber;

pub mod bsky;
pub mod common;
mod forward_server;
pub mod graph;
mod server;
mod ws;

//RUSTFLAGS="-Cprofile-generate=./pgo-data"     cargo build --release --target=x86_64-unknown-linux-gnu

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");
    tracing_subscriber::fmt::init();

    if !env::var("PROFILE_ENABLE").unwrap_or("".into()).is_empty() {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(1000)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .unwrap();

        ctrlc::set_handler(move || {
            info!("Shutting down");
            match guard.report().build() {
                Ok(report) => {
                    let mut file = File::create("profile.pb").unwrap();
                    let profile = report.pprof().unwrap();

                    let mut content = Vec::new();
                    profile.write_to_vec(&mut content).unwrap();
                    file.write_all(&content).unwrap();
                }
                Err(_) => {}
            };
            //TODO Exit properly
            process::exit(0x0100);
        })
        .expect("Error setting Ctrl-C handler");
    }

    let compression = env::var("COMPRESS_ENABLE").unwrap_or("".into());
    let forward_mode = env::var("FORWARD_MODE").unwrap_or("".into());

    // todo - this properly
    let user = env::var("MM_USER").unwrap_or("user".into());
    let pw = env::var("MM_PW").unwrap_or("pass".into());

    // If env says we need to forward DB requests, just do that & nothing else
    if !forward_mode.is_empty() {
        info!("Starting forward web server");
        forward_server::serve(forward_mode).await.unwrap();
        info!("Exiting forward web server");
        return Ok(());
    }

    let (send, recv) = mpsc::channel::<FetchMessage>(100);
    info!("Connecting to memgraph");
    let mut graph = GraphModel::new("bolt://localhost:7687", &user, &pw, recv)
        .await
        .unwrap();
    info!("Connected to memgraph");
    let server_conn = graph.inner();

    // Spin this off to accept incoming requests (feed serving atm, will likely just be DB reads)
    thread::spawn(move || {
        let web_runtime: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
        info!("Starting web listener thread");
        let wait = web_runtime.spawn(async move {
            let _ = server_conn;
            server::serve(send).await.unwrap();
        });
        web_runtime.block_on(wait).unwrap();
        info!("Exiting web listener thread");
    });
    //

    // Connect to the websocket
    info!("Connecting to Bluesky firehose");
    let compressed = !compression.is_empty();
    let url = format!("wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*&compress={}", compressed);
    let mut ws = ws::connect("jetstream1.us-east.bsky.network", url.clone()).await?;
    info!("Connected to Bluesky firehose");
    let ma = SumTreeSMA::<_, i64, 200>::new();
    let ctr = Arc::new(Mutex::new(ma));
    let ctr2 = ctr.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            let avg = ctr2.lock().await.get_average();
            info!("Average drift over 5s: {}ms", avg);
        }
    });
    'outer: loop {
        while let Ok(msg) = tokio::select! {
            msg = ws.read_frame() => {
                msg
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {
                if ws.is_closed() {
                    info!("Conn was closed!");
                }
                info!("Reconnecting to Bluesky firehose");
                let start = SystemTime::now();
                let since_the_epoch = start
                    .duration_since(UNIX_EPOCH).unwrap();
                let nu_url = url.clone() + format!("&cursor={}",&since_the_epoch.sub(Duration::from_secs(2)).as_micros().to_string()).as_str();
                ws = match ws::connect("jetstream1.us-east.bsky.network", nu_url).await{
                    Ok(ws) => ws,
                    Err(e) => {
                        info!("Error reconnecting to firehose: {}", e);
                        continue 'outer;
                    }
                };
                info!("Reconnected to Bluesky firehose");
                ws.read_frame().await
            }
        } {
            match msg.opcode {
                fastwebsockets::OpCode::Binary | fastwebsockets::OpCode::Text => {
                    match msg.payload {
                        fastwebsockets::Payload::Bytes(m) => {
                            match bsky::handle_event_fast(&m, &mut graph, compressed).await {
                                Err(e) => info!("Error handling event: {}", e),
                                Ok(drift) => {
                                    ctr.lock().await.add_sample(drift);
                                }
                            }
                        }
                        _ => {
                            panic!("Unsupported payload type {:?}", msg.payload);
                        }
                    };
                }
                fastwebsockets::OpCode::Close => {
                    info!("Closing connection, trying to reopen...");
                    ws = ws::connect("jetstream1.us-east.bsky.network", url.clone()).await?;
                    continue;
                }
                _ => {}
            }
        }
    }
}
