use crate::bsky::types::*;
use crate::graph::GraphModel;
use chrono::Utc;
use once_cell::sync::Lazy;

use std::mem;
use std::{collections::HashSet, time::SystemTime};
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use zstd::bulk::Decompressor;

mod types;

const DICT: &'static [u8; 112640] = include_bytes!("./dictionary");
static mut DECOMP: Lazy<Decompressor<'static>> =
    Lazy::new(|| zstd::bulk::Decompressor::with_dictionary(DICT).unwrap());

unsafe fn decompress_fast(m: &[u8]) -> Option<BskyEvent> {
    let msg = DECOMP.decompress(m, 409600); // 40kb
    match msg {
        Ok(m) => {
            match serde_json::from_slice(m.as_slice()) {
                Ok(m) => return Some(m),
                Err(err) => {
                    error!("{:?}", SystemTime::now());
                    panic!("Error decompressing payload: {err}")
                }
            };
        }
        Err(err) => panic!("Error getting payload: {err}"),
    };
}

pub async fn handle_event_fast(
    evt: &[u8],
    g: &mut GraphModel,
    mut rec: Option<mpsc::Receiver<()>>,
    compressed: bool,
) -> Result<(i64, Option<mpsc::Receiver<()>>), Box<dyn std::error::Error>> {
    let mut spam = HashSet::new();
    spam.insert("did:plc:xdx2v7gyd5dmfqt7v77gf457".to_owned());
    spam.insert("did:plc:a56vfzkrxo2bh443zgjxr4ix".to_owned());
    spam.insert("did:plc:cov6pwd7ajm2wgkrgbpej2f3".to_owned());

    // this ones fucking weird
    spam.insert("did:plc:fcnbisw7xl6lmtcnvioocffz".to_owned());
    // no hate, but bro...
    spam.insert("did:plc:ss7fj6p6yfirwq2hnlkfuntt".to_owned());

    let deser_evt: BskyEvent;
    if compressed {
        unsafe {
            deser_evt = decompress_fast(&evt).unwrap();
        }
    } else {
        match serde_json::from_slice(&evt) {
            Ok(m) => {
                deser_evt = m;
            }
            Err(err) => {
                panic!("unable to marhsal event: {:?}", err)
            }
        };
    }
    if spam.contains(&deser_evt.did) {
        return Ok((0, rec));
    }

    let mut commit: Commit = match deser_evt.commit {
        Some(m) => m,
        None => {
            return Ok((0, rec));
        }
    };
    let rkey = mem::take(&mut commit.rkey); //yoinky sploinky
    let now = Utc::now().timestamp_micros();
    let drift = (now - deser_evt.time_us) / 1000;

    if commit.operation == "create" {
        let mut is_reply = false;
        let mut is_image = false;
        let mut created_at = 0;
        match commit.collection.as_str() {
            "app.bsky.feed.post" => {
                match &commit.record {
                    Some(r) => {
                        is_image = r.images.is_some();
                        created_at = match chrono::DateTime::parse_from_rfc3339(&r.created_at) {
                            Ok(t) => {
                                if now - t.timestamp_micros()
                                    > chrono::Duration::hours(24).num_microseconds().unwrap()
                                {
                                    return Ok((0, rec));
                                }
                                t.timestamp_micros()
                            }
                            Err(_) => deser_evt.time_us,
                        };
                        match &r.reply {
                            Some(r) => {
                                let did_clone = deser_evt.did.clone();
                                let rkey_clone = rkey.clone();
                                let rkey_parent = parse_rkey(&r.parent.uri);
                                rec = g.add_reply(did_clone, rkey_clone, rkey_parent, rec).await;
                                is_reply = true;
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
                let recv = g
                    .add_post(deser_evt.did, rkey, &created_at, is_reply, is_image, rec)
                    .await;

                return Ok((drift, recv));
            }

            "app.bsky.feed.repost" => {
                let rkey_out = get_rkey(&commit);

                if rkey_out.is_empty() {
                    panic!("empty rkey");
                }

                let recv = g
                    .add_repost(deser_evt.did, rkey_out.to_string(), rkey, rec)
                    .await;
                return Ok((drift, recv));
            }

            "app.bsky.feed.like" => {
                let rkey_out = get_rkey(&commit);

                if rkey_out.is_empty() {
                    panic!("empty rkey");
                }

                let recv = g
                    .add_like(deser_evt.did, rkey_out.to_string(), rkey, rec)
                    .await;
                return Ok((drift, recv));
            }

            "app.bsky.graph.follow" => {
                let mut did_out = String::new();
                match &commit.record {
                    Some(r) => {
                        did_out = match &r.subject {
                            Some(s) => match s {
                                Subj::T1(s) => s.to_owned(),
                                Subj::T2(_) => return Ok((0, rec)),
                            },
                            None => return Ok((0, rec)),
                        };
                    }
                    None => {}
                }
                if did_out.is_empty() {
                    panic!("empty did_out");
                }
                let recv = g.add_follow(deser_evt.did, did_out, rkey, rec).await;
                return Ok((drift, recv));
            }

            "app.bsky.graph.block" => {
                let mut vblockee = String::new();
                match &commit.record {
                    Some(r) => {
                        vblockee = match &r.subject {
                            Some(s) => match s {
                                Subj::T1(s) => s.to_owned(),
                                Subj::T2(_) => return Ok((0, rec)),
                            },
                            None => return Ok((0, rec)),
                        };
                    }
                    None => {}
                }
                if vblockee.is_empty() {
                    panic!("empty vblockee");
                }
                let recv = g.add_block(vblockee, deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }
            _ => {}
        }
    } else if commit.operation == "delete" {
        match commit.collection.as_str() {
            "app.bsky.feed.post" => {
                let recv = g.rm_post(deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }
            "app.bsky.feed.repost" => {
                let recv = g.rm_repost(deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }

            "app.bsky.feed.like" => {
                let recv = g.rm_like(deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }
            "app.bsky.graph.follow" => {
                let recv = g.rm_follow(deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }
            "app.bsky.graph.block" => {
                let recv = g.rm_block(deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }
            _ => {}
        }
    }

    return Ok((0, rec));
}

fn parse_rkey(uri: &str) -> String {
    // the rkey are the last 13 characters
    uri.chars()
        .rev()
        .take(13)
        .collect::<Vec<_>>()
        .iter()
        .rev()
        .collect()
}

fn get_rkey(commit: &Commit) -> String {
    let rkey_out;
    match &commit.record {
        Some(r) => {
            rkey_out = match &r.subject {
                Some(s) => match s {
                    Subj::T1(_) => "".to_owned(),
                    Subj::T2(subject) => parse_rkey(&subject.uri),
                },
                None => "".to_owned(),
            };
        }
        None => rkey_out = "".to_owned(),
    }
    rkey_out
}

pub async fn get_follows(
    did: String,
    client: reqwest::Client,
) -> Result<Vec<(String, String)>, reqwest::Error> {
    info!("Getting follows for {:?}", did);
    let base_url = format!(
        "https://bsky.social/xrpc/com.atproto.repo.listRecords?repo={did}&collection=app.bsky.graph.follow&limit=100"
    );
    let mut follows: Vec<(String, String)> = Vec::new();
    let mut req = match client.get(&base_url).build() {
        Ok(r) => r,
        Err(e) => {
            return Err(e);
        }
    };
    let mut resp: FollowsResp = match client.execute(req).await {
        Ok(resp) => {
            let status = resp.status();
            match resp.json().await {
                Ok(r) => r,
                Err(e) => {
                    info!("resp returned {}: {:?}", status, e);
                    return Err(e);
                }
            }
        }
        Err(e) => return Err(e),
    };

    loop {
        for f in &mut resp.records {
            let subject = mem::take(&mut f.value.subject); // yoink the string, not gonna need it anymore in the vec anyway
            let rkey = parse_rkey(&f.uri);
            follows.push((subject, rkey));
        }
        match &resp.cursor {
            Some(c) => {
                let url = base_url.clone() + format!("&cursor={}", c).as_str();
                req = client.get(&url).build()?;
                let r = match client.execute(req).await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Error fetching follows for {} {:?}", &did, e);
                        continue;
                    }
                };

                let rr = r.status();
                resp = match r.json().await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Error getting follows for {}: {} : {:?}", did, rr, e);
                        break;
                    }
                };
            }
            None => {
                break;
            }
        }
    }

    Ok(follows)
}

pub async fn get_blocks(
    did: String,
    client: reqwest::Client,
) -> Result<Vec<(String, String)>, reqwest::Error> {
    info!("Getting blocks for {:?}", did);
    let base_url = format!(
        "https://bsky.social/xrpc/com.atproto.repo.listRecords?repo={did}&collection=app.bsky.graph.block&limit=100"
    );
    let mut blocks: Vec<(String, String)> = Vec::new();
    let mut req = match client.get(&base_url).build() {
        Ok(r) => r,
        Err(e) => {
            info!("req {:?}", e);
            return Err(e);
        }
    };
    let mut resp: FollowsResp = match client.execute(req).await?.json().await {
        Ok(r) => r,
        Err(e) => {
            info!("resp {:?}", e);
            return Err(e);
        }
    };

    loop {
        for f in &mut resp.records {
            let subject = mem::take(&mut f.value.subject); // yoink the string, not gonna need it anymore in the vec anyway
            let rkey = parse_rkey(&f.uri);
            blocks.push((subject, rkey));
        }
        match &resp.cursor {
            Some(c) => {
                //info!("Processing {:?}", c);
                let url = base_url.clone() + format!("&cursor={}", c).as_str();
                req = client.get(&url).build()?;
                let r = match client.execute(req).await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Error fetching blocks for {} {:?}", &did, e);
                        continue;
                    }
                };
                resp = match r.json().await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Error parsing blocks for {} {:?}", did, e);
                        continue;
                    }
                };
            }
            None => {
                break;
            }
        }
    }

    Ok(blocks)
}
