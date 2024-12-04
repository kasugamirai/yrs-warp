use std::sync::Arc;
use tokio::sync::RwLock;

pub mod broadcast;
pub mod broadcast_pool;
pub mod conn;
pub mod storage;
pub mod ws;

pub type AwarenessRef = Arc<RwLock<yrs::sync::Awareness>>;
