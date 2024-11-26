use axum::{
    body::Body,
    extract::{Query, State},
    http::Method,
    response::Response,
    routing::get,
    Json, Router,
};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};

use axum_server::tls_rustls::RustlsConfig;
use std::{collections::HashMap, env, net::SocketAddr, path::PathBuf, sync::Arc};
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
mod auth;
mod types;

struct StateStruct {
    client: reqwest::Client,
    edpt: String,
}
pub async fn serve(edpt: String) -> Result<(), Box<dyn std::error::Error>> {
    let cors = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_origin(Any);
    let config = RustlsConfig::from_pem_file(
        PathBuf::from(".").join("cert.pem"),
        PathBuf::from(".").join("key.pem"),
    )
    .await
    .unwrap();

    let state = Arc::new(StateStruct {
        client: reqwest::Client::new(),
        edpt,
    });

    let router = Router::new()
        .route("/", get(base))
        .route("/xrpc/app.bsky.feed.getFeedSkeleton", get(forward))
        .route("/xrpc/app.bsky.feed.describeFeedGenerator", get(describe))
        .route("/.well-known/did.json", get(well_known))
        .layer(ServiceBuilder::new().layer(cors))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    axum_server::bind_rustls(addr, config)
        .serve(router.into_make_service())
        .await
        .unwrap();
    Ok(())
}

async fn forward(
    Query(params): Query<HashMap<String, String>>,
    bearer: Option<TypedHeader<Authorization<Bearer>>>,
    State(state): axum::extract::State<Arc<StateStruct>>,
) -> Response<Body> {
    let iss;
    match &bearer {
        Some(s) => {
            iss = auth::verify_jwt(s.0 .0.token(), &"did:web:feed.m1k.sh".to_owned()).unwrap();
        }
        None => {
            return Response::builder()
                .status(401)
                .body(Body::from("unauthorized"))
                .unwrap();
        }
    };
    println!("user id {}", iss);

    let resp = match state
        .client
        .request(reqwest::Method::GET, state.edpt.as_str())
        .bearer_auth(bearer.unwrap().0 .0.token())
        .query(&params)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            println!("Error: {}", e);
            return Response::builder()
                .status(403)
                .body(Body::from(e.to_string()))
                .unwrap();
        }
    };
    let mut axum_res = Response::builder();
    *axum_res.headers_mut().unwrap() = resp.headers().clone();

    axum_res
        .status(&resp.status())
        .body(Body::from(resp.bytes().await.unwrap()))
        .unwrap()

    // Json(types::Response {
    //     cursor: Some("123".to_owned()),
    //     feed: vec![types::Post {
    //         post: "at://did:plc:zxs7h3vusn4chpru4nvzw5sj/app.bsky.feed.post/3lbdbqqdxxc2w"
    //             .to_owned(),
    //     }],
    // })
}

async fn base(
    Query(_): Query<HashMap<String, String>>,
    _: Option<TypedHeader<Authorization<Bearer>>>,
) -> Result<String, ()> {
    Ok("Hello!".into())
}

async fn well_known() -> Result<Json<types::WellKnown>, ()> {
    match env::var("FEEDGEN_SERVICE_DID") {
        Ok(service_did) => {
            let hostname = env::var("FEEDGEN_HOSTNAME").unwrap_or("".into());
            if !service_did.ends_with(hostname.as_str()) {
                println!("service_did does not end with hostname");
                return Err(());
            } else {
                let known_service = types::KnownService {
                    id: "#bsky_fg".to_owned(),
                    r#type: "BskyFeedGenerator".to_owned(),
                    service_endpoint: format!("https://{}", hostname),
                };
                let result = types::WellKnown {
                    context: vec!["https://www.w3.org/ns/did/v1".into()],
                    id: service_did,
                    service: vec![known_service],
                };
                return Ok(Json(result));
            }
        }
        Err(_) => {
            println!("service_did not found");
            return Err(());
        }
    }
}

async fn describe() -> Result<Json<types::Describe>, ()> {
    let hostname = env::var("FEEDGEN_HOSTNAME").unwrap();
    let dezscribe = types::Describe {
        did: format!("did:web:{hostname}"),
        feeds: vec![types::Feed {
            uri: format!("at://did:web:{hostname}/app.bsky.feed.generator/following_plus"),
        }],
    };

    Ok(Json(dezscribe))
}
