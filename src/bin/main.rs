use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        Path,
    },
    response::Response,
    routing::get,
    Router,
};
use std::sync::Arc;
use yrs_warp::broadcast_pool::BroadcastPool;
use yrs_warp::{
    broadcast::{BroadcastGroup, RedisConfig},
    storage::gcs::GcsError,
};
//use yrs_warp::storage::sqlite::SqliteStore;
use google_cloud_storage::{
    client::{self, Client},
    http::buckets::insert::{BucketCreationConfig, InsertBucketRequest},
};
use yrs_warp::storage::gcs::{GcsConfig, GcsStore};
use yrs_warp::ws::WarpConn;

//const DB_PATH: &str = "examples/code-mirror/yrs.db";
const REDIS_URL: &str = "redis://127.0.0.1:6379";
const REDIS_TTL: u64 = 3600; // Cache TTL in seconds
const BUCKET_NAME: &str = "yrs-dev";

async fn ensure_bucket(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let bucket = BucketCreationConfig {
        location: "US".to_string(),
        ..Default::default()
    };
    let request = InsertBucketRequest {
        name: BUCKET_NAME.to_string(),
        bucket,
        ..Default::default()
    };

    match client.insert_bucket(&request).await {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().contains("already exists") => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .init();

    // Initialize SQLite store
    //let store = Arc::new(SqliteStore::new(DB_PATH).expect("Failed to open SQLite database"));
    //tracing::info!("SQLite store initialized at: {}", DB_PATH);

    let gcs_config = GcsConfig {
        bucket_name: BUCKET_NAME.to_string(),
        endpoint: Some("http://localhost:4443".to_string()),
    };

    let store = GcsStore::new_with_config(gcs_config)
        .await
        .expect("Failed to create GCS store");

    // Ensure bucket exists
    ensure_bucket(&store.client)
        .await
        .expect("Failed to create bucket");

    let store = Arc::new(store);
    tracing::info!("GCS store initialized");

    // Configure Redis
    let redis_config = RedisConfig {
        url: REDIS_URL.to_string(),
        ttl: REDIS_TTL,
    };

    // Create broadcast pool
    let pool = Arc::new(BroadcastPool::new(store, redis_config));
    tracing::info!("Broadcast pool initialized");

    let app = Router::new()
        .route("/:doc_id", get(ws_handler))
        .with_state(pool);

    tracing::info!("Starting server on 0.0.0.0:8000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Path(doc_id): Path<String>,
    axum::extract::State(pool): axum::extract::State<Arc<BroadcastPool>>,
) -> Response {
    let doc_id = if doc_id.ends_with(":main") {
        doc_id[..doc_id.len() - 5].to_string()
    } else {
        doc_id
    };

    let bcast = pool.get_or_create_group(&doc_id).await;

    ws.on_upgrade(move |socket| handle_socket(socket, bcast))
}

async fn handle_socket(socket: WebSocket, bcast: Arc<BroadcastGroup>) {
    let conn = WarpConn::new(bcast, socket);
    if let Err(e) = conn.await {
        tracing::error!("WebSocket connection error: {}", e);
    }
}
