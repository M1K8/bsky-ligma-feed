use common::FetchMessage;
use graph::GraphModel;
use pprof::protos::Message;
use std::{env, process};
use std::{fs::File, io::Write, thread};
use tokio::sync::mpsc;

pub mod bsky;
pub mod common;
pub mod graph;
mod server;
mod forward_server;
mod ws;

//RUSTFLAGS="-Cprofile-generate=./pgo-data"     cargo build --release --target=x86_64-unknown-linux-gnu

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let profile = env::var("PROFILE_ENABLE").unwrap_or("".into());
    let compression = env::var("COMPRESS_ENABLE").unwrap_or("".into());
    let forward_mode = env::var("FORWARD_MODE").unwrap_or("".into());


    if !profile.is_empty() {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(1000)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .unwrap();

        ctrlc::set_handler(move || {
            println!("Shutting down");
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

    let (send, recv) = mpsc::channel::<FetchMessage>(100);
    let mut graph = GraphModel::new("bolt://localhost:7687", "user", "pass", recv)
        .await
        .unwrap();
    let server_conn = graph.inner();


    // If env says we need to forward DB requests, just do that & nothing else
    if !forward_mode.is_empty() {
        println!("Starting forward web server");
        forward_server::serve().await.unwrap();
        println!("Exiting forward web server");
        return Ok(())
    }
    // Spin this off to accept incoming requests (feed serving atm, will likely just be DB reads)
    thread::spawn(move || {
        let web_runtime: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
        println!("Starting web listener thread");
        let wait = web_runtime.spawn(async move {
            let _ = server_conn;
            server::serve(send).await.unwrap();
        });
        web_runtime.block_on(wait).unwrap();
        println!("Exiting web listener thread");
    });
    //

    // Connect to the websocket
    let url;
    let compress = !compression.is_empty();
    if compress {
        url = format!("wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*&compress={}", "true");
    } else {
        url =  format!("wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*&compress={}", "false");
    }
    let mut ws = ws::connect("jetstream1.us-east.bsky.network", url).await?;
    println!("Connected to Bluesky firehose");

    while let Ok(msg) = ws.read_frame().await {
        match msg.opcode {
            fastwebsockets::OpCode::Binary | fastwebsockets::OpCode::Text => {
                match msg.payload {
                    fastwebsockets::Payload::Bytes(m) => {
                        match bsky::handle_event_fast(&m, &mut graph, compress).await {
                            Err(e) => println!("Error handling event: {}", e),
                            _ => {}
                        }
                    }
                    _ => {
                        panic!("Unsupported payload type {:?}", msg.payload);
                    }
                };
            }
            fastwebsockets::OpCode::Close => {
                println!("Closing connection");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}
