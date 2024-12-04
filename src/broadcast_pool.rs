use crate::broadcast::{BroadcastConfig, BroadcastGroup, RedisConfig};
use crate::storage::kv::DocOps;
use crate::storage::sqlite::SqliteStore;
use crate::AwarenessRef;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use yrs::sync::Awareness;
use yrs::{Doc, Text, Transact};

pub struct BroadcastPool {
    store: Arc<SqliteStore>,
    redis_config: RedisConfig,
    groups: RwLock<HashMap<String, Arc<BroadcastGroup>>>,
}

impl BroadcastPool {
    pub fn new(store: Arc<SqliteStore>, redis_config: RedisConfig) -> Self {
        Self {
            store,
            redis_config,
            groups: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_or_create_group(&self, doc_id: &str) -> Arc<BroadcastGroup> {
        // 先尝试获取已存在的广播组
        if let Some(group) = self.groups.read().await.get(doc_id) {
            return group.clone();
        }

        // 如果不存在，创建新的广播组
        let mut groups = self.groups.write().await;
        if let Some(group) = groups.get(doc_id) {
            return group.clone();
        }

        // 创建新的文档和广播组
        let awareness: AwarenessRef = {
            let doc = Doc::new();

            // 加载文档状态
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
                        // 初始化新文档
                        let txt = doc.get_or_insert_text("codemirror");
                        txt.push(
                            &mut txn,
                            r#"function hello() {
  console.log('hello world');
}"#,
                        );
                        tracing::info!("Initialized new document: {}", doc_id);
                    }
                }
            }

            Arc::new(RwLock::new(Awareness::new(doc)))
        };

        // 创建新的广播组
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
