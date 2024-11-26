use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use neo4rs::Graph;
use tokio::{
    sync::{mpsc, Mutex},
    task,
};

use crate::{
    bsky,
    common::{FetchMessage, PostMsg, PostResp},
    graph::queries::{self, PURGE_OLD_POSTS},
};

const PURGE_TIME: u64 = 45 * 60;

pub fn get_post_uri(did: String, rkey: String) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
}
pub fn pluralize(word: &str) -> String {
    let word_len = word.len();
    let snip = &word[..word_len - 1];
    let last_char = word.chars().nth(word_len - 1).unwrap();

    if last_char == 'y' || word.ends_with("ay") {
        return format!("{}ies", snip);
    } else if last_char == 's' || last_char == 'x' || last_char == 'z' {
        return format!("{}es", word);
    } else if last_char == 'o' && word.ends_with("o") && !word.ends_with("oo") {
        return format!("{}oes", snip);
    } else if last_char == 'u' && word.ends_with("u") {
        return format!("{}i", snip);
    } else {
        return format!("{}s", word);
    }
}

pub async fn kickoff_purge(spin: Arc<Mutex<()>>, conn: Graph) -> Result<(), neo4rs::Error> {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(PURGE_TIME)).await;
        println!("Purging old posts");
        let _lock = spin.lock().await;
        let qry = neo4rs::query(PURGE_OLD_POSTS);
        conn.run(qry).await?;
        println!("Done!");
    }
}

pub async fn listen_channel(
    write_lock: Arc<Mutex<()>>,
    conn: Graph,
    mut recv: mpsc::Receiver<FetchMessage>,
) -> Result<(), neo4rs::Error> {
    let mut client = reqwest::ClientBuilder::new();
    client = client.timeout(Duration::from_secs(5));
    let client = client.build().unwrap();

    let mut already_seen: HashSet<String> = HashSet::new();
    loop {
        let msg = match recv.recv().await {
            Some(s) => s,
            None => continue,
        };

        println!("Got event for {:?}", msg.did);

        if !already_seen.contains(&msg.did) {
            let did_follows = msg.did.clone();
            let cl_follows = client.clone();
            let follows = match bsky::get_follows(did_follows, cl_follows).await {
                Ok(f) => f,
                Err(e) => return Err(neo4rs::Error::UnexpectedMessage(e.to_string())),
            };

            let follow_chunks: Vec<HashMap<String, String>> = follows
                .iter()
                .map(|vals| {
                    HashMap::from([
                        ("out".to_owned(), vals.0.clone()),
                        ("did".to_owned(), msg.did.clone()),
                        ("rkey".to_owned(), vals.1.clone()),
                    ])
                })
                .collect();
            let follow_chunks = follow_chunks.chunks(60).collect::<Vec<_>>();

            for follow_chunk in follow_chunks {
                let qry = neo4rs::query(queries::ADD_FOLLOW).param("follows", follow_chunk);
                let l = write_lock.lock().await;

                match conn.run(qry).await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error on backfilling follows for {}", &msg.did);
                        return Err(e);
                    }
                };
                drop(l);
            }

            msg.resp
                .send(PostResp {
                    posts: vec![PostMsg {
                        uri:
                            "at://did:plc:zxs7h3vusn4chpru4nvzw5sj/app.bsky.feed.post/3lbdbqqdxxc2w"
                                .to_owned(),
                        reason: "".to_owned(),
                    }],
                    cursor: Some("1".to_owned()),
                })
                .await
                .unwrap();

            println!("Written {} follows for {}", follows.len(), &msg.did);
        }

        already_seen.insert(msg.did.clone());
    }
}
