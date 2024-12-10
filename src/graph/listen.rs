use core::error;
use std::time::SystemTime;
use std::{collections::HashMap, mem, sync::Arc, time::Duration};

use backoff::{future::retry, Error, ExponentialBackoffBuilder};
use dashmap::{DashMap, DashSet};
use neo4rs::{Graph, Query};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::{
    bsky,
    common::{FetchMessage, PostMsg, PostResp},
    graph::{
        queries::{self},
        util,
    },
};

macro_rules! process_next {
    ($next_expr:expr, $posts_expr:expr, $reason:expr) => {
        match $next_expr {
            Ok(v) => match v {
                Some(v) => {
                    let uri: String = v.get("url").unwrap();
                    let user: String = v.get("user").unwrap();
                    let reply: String = v.get("isReply").unwrap();
                    let uri = util::get_post_uri(user, uri);
                    let timestamp: u64 = v.get("ts").unwrap();
                    $posts_expr.insert(
                        uri.clone(),
                        PostMsg {
                            reason: format!("{}_{reply}", $reason).to_owned(),
                            uri,
                            timestamp,
                        },
                    );
                }
                None => {
                    break;
                }
            },
            Err(e) => {
                warn!("{:?}", e);
                break;
            }
        }
    };
}

pub async fn listen_channel(
    write_lock: Arc<RwLock<()>>,
    conn: Graph,
    read_conn: Graph,
    mut recv: mpsc::Receiver<FetchMessage>,
) -> Result<(), neo4rs::Error> {
    let mut client = reqwest::ClientBuilder::new();
    client = client.timeout(Duration::from_secs(10));
    let client = client.build().unwrap();

    let in_flight = Arc::new(DashSet::new());
    //let mut cached = HashMap::<String, Vec<PostMsg>>::new();
    let seen_map = Arc::new(DashSet::new());
    loop {
        let in_flight_loop = in_flight.clone();
        let mut msg = match recv.recv().await {
            Some(s) => s,
            None => continue,
        };

        if msg.did.is_empty() {
            match msg
                .resp
                .send(PostResp {
                    posts: vec![PostMsg {
                        uri: "".to_owned(),
                        reason: "".to_owned(),
                        timestamp: 0,
                    }],
                    cursor: Some("EMPTY_DID".to_owned()),
                })
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!("Error sending empty did resp back for{}: {}", &msg.did, e);
                    continue;
                }
            };
            continue;
        }
        if in_flight_loop.contains(&msg.did) {
            continue;
            //TODO - Return whatever is in the cache
            // TODO - cache: k uid_cursor v: vec<postmsg>
        }
        info!("Got event for {:?}", msg.did);
        in_flight_loop.insert(msg.did.clone());
        let cursor;
        if msg.cursor.is_some() {
            let cur = mem::take(&mut msg.cursor);
            cursor = cur.unwrap();
            info!("cursor is {cursor}");
        } else {
            warn!("NO CURSOR!");
            cursor = now();
        }

        // Firstly, check the cache for this user, then check if the cursor is within the last 5 mins, otherwise junk it (if the cleanup job hasnt already)
        // Moreover, change queries
        // See if user has been seen

        // Follows
        let did = msg.did.clone();
        let cl_follows = client.clone();

        let lock = write_lock.clone();
        let second_deg_conn = conn.clone();
        let seen_map = seen_map.clone();

        let bl_seen_map = seen_map.clone();

        match get_follows(&did, cl_follows).await {
            Ok(follows) => {
                // TODO - Have map of currently fetching users to prevent replay
                tokio::spawn(async move {
                    info!("Recursively fetching {} follows for {did}", follows.len());

                    let all_follows = Arc::new(DashSet::new());
                    if !seen_map.contains(&did) {
                        follows.iter().for_each(|f| {
                            all_follows.insert((f.0.clone(), f.1.clone(), did.clone()));
                        });
                    }
                    seen_map.insert(did.clone());
                    let mut set = JoinSet::new();
                    let all_followers_chunks: Vec<&[(String, String)]> =
                        follows.chunks(follows.len() / 12).collect();

                    for idx in 0..all_followers_chunks.len() {
                        let mut c = match all_followers_chunks.get(idx) {
                            Some(c) => *c,
                            None => {
                                continue;
                            }
                        };
                        // this is so fun i love doing this
                        let yoinked = mem::take(&mut c);
                        let chunk = Vec::from(yoinked);
                        //

                        let seen_map = seen_map.clone();
                        let all_follows = all_follows.clone();

                        set.spawn(async move {
                            for (did, _) in chunk {
                                if seen_map.contains(&did) {
                                    // Already fetched this one, no need to do it again
                                    continue;
                                }

                                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                                let mut cl_2nd_follows = reqwest::ClientBuilder::new();
                                cl_2nd_follows = cl_2nd_follows.timeout(Duration::from_secs(10));
                                let cl_2nd_follows = cl_2nd_follows.build().unwrap();
                                match get_follows(&did, cl_2nd_follows).await {
                                    Ok(mut f) => {
                                        f.iter_mut().for_each(|f| {
                                            all_follows.insert((
                                                mem::take(&mut f.0),
                                                mem::take(&mut f.1),
                                                did.clone(),
                                            ));
                                        });
                                        seen_map.insert(did.clone());
                                    }
                                    Err(e) => {
                                        if e.is::<RecNotFound>() {
                                            seen_map.insert(did.clone()); // this aint ever gonna work, so ignore it
                                            info!("{did} probably doesnt exist on this PDS, skipping...")
                                        } else {
                                            warn!(
                                                "Error getting 2nd degree follows for {did}: {:?}",
                                                e
                                            );
                                        }

                                        continue;
                                    }
                                };
                            }
                        });
                    }
                    set.join_all().await;
                    info!("There are {} chunks", all_followers_chunks.len());
                    info!(
                        "Done Recursively fetching {} for {did}; writing...",
                        follows.len()
                    );

                    match write_follows(all_follows, &second_deg_conn, lock).await {
                        Some(e) => warn!("Error writing 2nd degree follows for {did}: {:?}", e),
                        None => {}
                    }
                    in_flight_loop.remove(&did);
                });
            }
            Err(e) => {
                warn!("Error getting follows for {}: {}", &msg.did, e);
                continue;
            }
        };
        if !bl_seen_map.contains(&msg.did) {
            // Blocks
            let did_blocks = msg.did.clone();
            let cl_blocks = client.clone();
            match get_blocks(&did_blocks, cl_blocks, &conn, write_lock.clone()).await {
                Ok(_) => {}
                Err(e) => {
                    warn!("Error getting blocks for {}: {}", &msg.did, e);
                    continue;
                }
            };
        }
        match fetch_and_return_posts(msg, &read_conn, &cursor).await {
            Ok(_) => {}
            Err(_) => continue,
        }
    }
}

fn now() -> String {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .to_string()
}

async fn fetch_and_return_posts(
    msg: FetchMessage,
    read_conn: &Graph,
    time: &String,
) -> Result<(), Box<dyn error::Error>> {
    let mut res_vec = Vec::new();
    let mut cursor = time.clone();
    let mut first_time = false;
    while res_vec.len() < 30 && !first_time {
        // if the first call returns nothing, then we've reached the end
        if let Ok(posts) = fetch_posts(&msg, read_conn, &cursor).await {
            for p in posts.iter() {
                res_vec.push(p.value().clone());
            }

            if let Some(val) = res_vec.last() {
                cursor = val.timestamp.to_string();
            } else {
                info!("Reached the end");
                break;
            }
            info!("Cursor is {:?}", cursor);
            first_time = true;
        }
    }

    //TODO - Cache these
    if res_vec.len() > 30 {
        info!("Response is > 30; pls cache xx");
    }

    let c;
    if res_vec.len() == 0 {
        c = None;
    } else {
        c = Some(cursor);
    }

    res_vec.sort_unstable();
    for v in res_vec.iter() {
        info!("Adding {:?}", v);
    }

    match msg
        .resp
        .send(PostResp {
            posts: res_vec,
            cursor: c,
        })
        .await
    {
        Ok(_) => {}
        Err(e) => {
            warn!("Error replying to post request for {}: {:?}", msg.did, e);
            return Err(Box::new(e));
        }
    };
    Ok(())
}

async fn fetch_posts(
    msg: &FetchMessage,
    read_conn: &Graph,
    time: &String,
) -> Result<Arc<DashMap<String, PostMsg>>, Box<dyn error::Error>> {
    // Fetch posts
    warn!("Time is {time}");
    let qry1 = neo4rs::query(
        &queries::GET_BEST_2ND_DEG_LIKES
            .to_string()
            .replace("{}", &time),
    )
    .param("did", msg.did.clone());
    let qry2 = neo4rs::query(
        &queries::GET_BEST_2ND_DEG_REPOSTS
            .to_string()
            .replace("{}", &time),
    )
    .param("did", msg.did.clone());
    let qry3 = neo4rs::query(
        &queries::GET_FOLLOWING_PLUS_LIKES
            .to_string()
            .replace("{}", &time),
    )
    .param("did", msg.did.clone());
    let qry4 = neo4rs::query(
        &queries::GET_FOLLOWING_PLUS_REPOSTS
            .to_string()
            .replace("{}", &time),
    )
    .param("did", msg.did.clone());

    let res = tokio::try_join!(
        read_conn.execute(qry1),
        read_conn.execute(qry2),
        read_conn.execute(qry3),
        read_conn.execute(qry4)
    );
    let posts = Arc::new(DashMap::new());
    match res {
        Ok((mut l1, mut l2, mut l3, mut l4)) => {
            let p1 = posts.clone();
            let p2 = posts.clone();
            let p3 = posts.clone();
            let p4 = posts.clone();
            let f1 = tokio::spawn(async move {
                loop {
                    process_next!(l1.next().await, p1, "2ND_DEG_LIKES");
                }
            });

            let f2 = tokio::spawn(async move {
                loop {
                    process_next!(l2.next().await, p2, "2ND_DEG_REPOSTS");
                }
            });

            let f3 = tokio::spawn(async move {
                loop {
                    process_next!(l3.next().await, p3, "FPLUS_LIKES");
                }
            });

            let f4 = tokio::spawn(async move {
                loop {
                    process_next!(l4.next().await, p4, "FPLUS_REPOSTS");
                }
            });

            match tokio::try_join!(f1, f2, f3, f4) {
                Ok(_) => {}
                Err(e) => return Err(Box::new(neo4rs::Error::UnexpectedMessage(e.to_string()))),
            };
        }
        Err(e) => {
            warn!("Error joining post fetches for {}: {}", &msg.did, e);
            return Err(Box::new(e));
        }
    }

    Ok(posts)
}

async fn get_follows(
    did_follows: &String,
    cl_follows: reqwest::Client,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let follows = match bsky::get_follows(did_follows.clone(), cl_follows).await {
        Ok(f) => f,
        Err(e) => {
            let err_str = format!("{:?}", e);
            if err_str.contains("missing field `records`") && err_str.contains("400") {
                return Err(Box::new(RecNotFound {}));
            }
            return Err(Box::new(e));
        }
    };

    Ok(follows)
}

async fn get_blocks(
    did_blocks: &String,
    cl_blocks: reqwest::Client,
    conn: &Graph,
    write_lock: Arc<RwLock<()>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let blocks = match bsky::get_blocks(did_blocks.clone(), cl_blocks).await {
        Ok(f) => f,
        Err(e) => return Err(Box::new(e)),
    };

    let block_chunks: Vec<HashMap<String, String>> = blocks
        .iter()
        .map(|vals| {
            HashMap::from([
                ("out".to_owned(), vals.0.clone()),
                ("did".to_owned(), did_blocks.clone()),
                ("rkey".to_owned(), vals.1.clone()),
            ])
        })
        .collect();
    let block_chunks = block_chunks.chunks(60).collect::<Vec<_>>();
    for block_chunk in block_chunks {
        let qry = neo4rs::query(queries::ADD_BLOCK).param("blocks", block_chunk);
        let l = write_lock.read().await;
        match conn.run(qry).await {
            Ok(_) => {}
            Err(e) => {
                info!("Error on backfilling blocks for {}", &did_blocks);
                drop(l);
                return Err(Box::new(e));
            }
        };
        drop(l);
    }

    info!("Written {} blocks for {}", blocks.len(), &did_blocks);

    Ok(())
}

async fn write_follows(
    follows: Arc<DashSet<(String, String, String)>>,
    conn: &Graph,
    write_lock: Arc<RwLock<()>>,
) -> Option<Box<dyn std::error::Error>> {
    let follow_chunks: Vec<HashMap<String, String>> = follows
        .iter()
        .map(|vals| {
            HashMap::from([
                ("out".to_owned(), vals.0.clone()),
                ("rkey".to_owned(), vals.1.clone()),
                ("did".to_owned(), vals.2.clone()),
            ])
        })
        .collect();
    let len = follow_chunks.len();
    let chunks;
    if len < 20 as usize {
        chunks = follow_chunks.chunks(1).collect::<Vec<_>>();
    } else {
        chunks = follow_chunks.chunks(len / 20).collect::<Vec<_>>();
    }

    let mut qrys = Vec::new();
    for follow_chunk in chunks {
        let qry = neo4rs::query(queries::POPULATE_FOLLOW).param("follows", follow_chunk);
        qrys.push(qry);
    }
    let conn_cl = conn.clone();
    let l = write_lock.write().await;
    match retry(
        ExponentialBackoffBuilder::default()
            .with_initial_interval(Duration::from_millis(250))
            .with_max_elapsed_time(Some(Duration::from_millis(10000)))
            .build(),
        || async {
            let i: Vec<Query> = qrys.iter().map(|v| v.clone()).collect();
            let ilen = &i.len();
            let mut tx = conn_cl.start_txn().await.unwrap();
            match tx.run_queries(i).await {
                Ok(_) => match tx.commit().await {
                    Ok(_) => {
                        info!("Written {} queries", ilen);
                        Ok(())
                    }
                    Err(e) => Err(Error::Transient {
                        err: e,
                        retry_after: None,
                    }),
                },
                Err(e) => Err(Error::Transient {
                    err: e,
                    retry_after: None,
                }),
            }
        },
    )
    .await
    {
        Ok(_) => {
            info!("Written {} follows", len);
            drop(l);
        }
        Err(e) => {
            drop(l);
            warn!("Error committing: {}", e);
            return Some(Box::new(e));
        }
    }
    info!("Done!");
    None
}

#[derive(Debug)]
struct RecNotFound {}

impl std::fmt::Display for RecNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecNotFound")
    }
}

impl core::error::Error for RecNotFound {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}
