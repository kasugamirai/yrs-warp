use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::Response,
    routing::get,
    Router,
};
use futures::SinkExt;
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tower_http::services::ServeDir;
use yrs::sync::Awareness;
use yrs::{Doc, Text, Transact};
use yrs_warp::storage::sqlite::SqliteStore;
use yrs_warp::ws::{WarpSink, WarpStream};
use yrs_warp::AwarenessRef;
use yrs_warp::{broadcast::BroadcastGroup, storage::kv::DocOps};

const STATIC_FILES_DIR: &str = "examples/code-mirror/frontend/dist";
const DB_PATH: &str = "examples/code-mirror/yrs.db";
const DOC_NAME: &str = "codemirror";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Initialize tracing subscriber with more detailed logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .init();

    // Initialize SQLite store
    let store = Arc::new(SqliteStore::new(DB_PATH).expect("Failed to open SQLite database"));
    tracing::info!("SQLite store initialized at: {}", DB_PATH);

    // We're using a single static document shared among all the peers.
    let awareness: AwarenessRef = {
        let doc = Doc::new();

        // Load document state from storage
        {
            let mut txn = doc.transact_mut();
            match store.load_doc(DOC_NAME, &mut txn).await {
                Ok(_) => {
                    tracing::info!("Successfully loaded existing document from storage");
                }
                Err(e) => {
                    tracing::warn!("No existing document found or failed to load: {}", e);
                    // Initialize new document if no existing one found
                    let txt = doc.get_or_insert_text("codemirror");
                    txt.push(
                        &mut txn,
                        r#"function hello() {
  console.log('hello world');
}"#,
                    );
                    tracing::info!("Initialized new document with default content");
                }
            }
        }

        Arc::new(RwLock::new(Awareness::new(doc)))
    };

    // open a broadcast group that listens to awareness and document updates
    let bcast = Arc::new(BroadcastGroup::new(awareness.clone(), 32).await);
    tracing::info!("Broadcast group initialized");

    let app = Router::new()
        .route("/main", get(ws_handler))
        .nest_service("/", ServeDir::new(STATIC_FILES_DIR))
        .with_state((bcast, store));

    tracing::info!("Starting server on 0.0.0.0:8000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    axum::extract::State((bcast, store)): axum::extract::State<(
        Arc<BroadcastGroup>,
        Arc<SqliteStore>,
    )>,
) -> Response {
    ws.on_upgrade(move |socket| peer(socket, bcast, store))
}

async fn peer(ws: WebSocket, bcast: Arc<BroadcastGroup>, store: Arc<SqliteStore>) {
    let (ws_sink, ws_stream) = ws.split();
    let sink = Arc::new(Mutex::new(WarpSink::from(ws_sink)));
    let stream = WarpStream::from(ws_stream);

    let sink2 = sink.clone();
    let filtered_stream = Box::pin(stream.filter_map(move |msg| {
        let sink2 = sink2.clone();
        async move {
            match msg {
                Ok(vec) => {
                    if vec == vec![1] {
                        let mut sink = sink2.lock().await;
                        let _ = sink.send(vec![2]).await;
                        None
                    } else {
                        Some(Ok(vec))
                    }
                }
                Err(e) => Some(Err(e)),
            }
        }
    }));

    let sub = bcast.subscribe(sink, filtered_stream);
    if let Err(e) = sub.completed().await {
        eprintln!("broadcasting for channel finished abruptly: {}", e);
    } else {
        println!("broadcasting for channel finished successfully");
    }
}
