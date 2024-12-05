use crate::broadcast::{BroadcastConfig, BroadcastGroup, RedisConfig};
use crate::storage::kv::DocOps;
//use crate::storage::sqlite::SqliteStore;
use crate::storage::gcs::GcsStore;
use crate::AwarenessRef;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use yrs::sync::Awareness;
use yrs::{doc, Doc, Text, Transact};

pub struct BroadcastPool {
    store: Arc<GcsStore>,
    redis_config: RedisConfig,
    groups: RwLock<HashMap<String, Arc<BroadcastGroup>>>,
}

impl BroadcastPool {
    pub fn new(store: Arc<GcsStore>, redis_config: RedisConfig) -> Self {
        Self {
            store,
            redis_config,
            groups: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_or_create_group(&self, doc_id: &str) -> Arc<BroadcastGroup> {
        // Try to get existing broadcast group first
        if let Some(group) = self.groups.read().await.get(doc_id) {
            return group.clone();
        }

        // If not exists, create new broadcast group
        let mut groups = self.groups.write().await;
        if let Some(group) = groups.get(doc_id) {
            return group.clone();
        }

        // Create new document and broadcast group
        let awareness: AwarenessRef = {
            let doc = Doc::new();

            // Load document state
            {
                let mut txn = doc.transact_mut();
                match self.store.load_doc(doc_id, &mut txn).await {
                    Ok(_) => {
                        tracing::info!("Successfully loaded existing document: {}", doc_id);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "No existing document found or failed to load {}: {}",
                            doc_id,
                            e
                        );
                    }
                }
            }

            Arc::new(RwLock::new(Awareness::new(doc)))
        };

        // Create new broadcast group
        let group = Arc::new(
            BroadcastGroup::with_storage(
                awareness,
                128,
                self.store.clone(),
                BroadcastConfig {
                    storage_enabled: true,
                    doc_name: Some(doc_id.to_string()),
                    redis_config: Some(self.redis_config.clone()),
                },
            )
            .await,
        );

        groups.insert(doc_id.to_string(), group.clone());
        group
    }
}
