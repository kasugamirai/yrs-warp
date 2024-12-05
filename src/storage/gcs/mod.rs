//! **yrs-gcs** is a persistence layer allowing to store [Yrs](https://docs.rs/yrs/latest/yrs/index.html)
//! documents and providing convenient utility functions to work with them, using Google Cloud Storage for
//! persistent backend.

pub use super::kv as store;
use super::kv::{DocOps, KVEntry, KVStore};
use google_cloud_storage::{
    client::{Client, ClientConfig},
    http::objects::delete::DeleteObjectRequest,
    http::objects::download::Range,
    http::objects::get::GetObjectRequest,
    http::objects::list::ListObjectsRequest,
    http::objects::upload::{Media, UploadObjectRequest, UploadType},
    http::objects::Object,
};
use hex;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub enum GcsError {
    #[error("Google API error: {0}")]
    GoogleApi(#[from] google_cloud_storage::http::Error),
}

/// Type wrapper around GCS Client struct. Used to extend GCS with [DocOps]
/// methods used for convenience when working with Yrs documents.
pub struct GcsStore {
    #[allow(dead_code)]
    pub client: Client,
    pub bucket: String,
}

#[derive(Debug, Clone)]
pub struct GcsConfig {
    pub bucket_name: String,
    pub endpoint: Option<String>,
}

impl std::fmt::Debug for GcsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsStore")
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl GcsStore {
    pub async fn new(bucket: String) -> Result<Self, GcsError> {
        let config = ClientConfig::default();
        let client = Client::new(config);
        Ok(Self { client, bucket })
    }

    pub async fn new_with_config(config: GcsConfig) -> Result<Self, GcsError> {
        let mut client_config = ClientConfig::default().anonymous();
        client_config.storage_endpoint = config
            .endpoint
            .unwrap_or_else(|| "http://localhost:4443".to_string());

        let client = Client::new(client_config);

        Ok(Self {
            client,
            bucket: config.bucket_name,
        })
    }

    pub async fn with_client(client: Client, bucket: String) -> Self {
        Self { client, bucket }
    }
}

impl<'db> DocOps<'db> for GcsStore {}

impl KVStore for GcsStore {
    type Error = GcsError;
    type Cursor = GcsRange;
    type Entry = GcsEntry;
    type Return = Vec<u8>;

    async fn get(&self, key: &[u8]) -> Result<Option<Self::Return>, Self::Error> {
        let key_hex = hex::encode(key);
        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            object: key_hex,
            ..Default::default()
        };

        match self
            .client
            .download_object(&request, &Range::default())
            .await
        {
            Ok(data) => Ok(Some(data)),
            Err(e) => Err(e.into()),
        }
    }

    async fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let key_hex = hex::encode(key);
        let upload_type = UploadType::Simple(Media::new(key_hex));
        let request = UploadObjectRequest {
            bucket: self.bucket.clone(),
            ..Default::default()
        };

        debug!("Writing to GCS storage - key: {:?}", key);
        debug!("Value length: {} bytes", value.len());

        self.client
            .upload_object(&request, value.to_vec(), &upload_type)
            .await
            .map_err(GcsError::from)?;
        Ok(())
    }

    async fn remove(&self, key: &[u8]) -> Result<(), Self::Error> {
        let key_hex = hex::encode(key);
        let request = DeleteObjectRequest {
            bucket: self.bucket.clone(),
            object: key_hex,
            ..Default::default()
        };

        match self.client.delete_object(&request).await {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("not found") => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn remove_range(&self, from: &[u8], to: &[u8]) -> Result<(), Self::Error> {
        let request = ListObjectsRequest {
            bucket: self.bucket.clone(),
            ..Default::default()
        };

        let objects = self
            .client
            .list_objects(&request)
            .await
            .map_err(GcsError::from)?
            .items
            .unwrap_or_default();

        let from_hex = hex::encode(from);
        let to_hex = hex::encode(to);

        for obj in objects {
            if obj.name.as_str() >= from_hex.as_str() && obj.name.as_str() <= to_hex.as_str() {
                let delete_request = DeleteObjectRequest {
                    bucket: self.bucket.clone(),
                    object: obj.name,
                    ..Default::default()
                };
                self.client
                    .delete_object(&delete_request)
                    .await
                    .map_err(GcsError::from)?;
            }
        }

        Ok(())
    }

    async fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error> {
        let request = ListObjectsRequest {
            bucket: self.bucket.clone(),
            ..Default::default()
        };

        let response = self
            .client
            .list_objects(&request)
            .await
            .map_err(GcsError::from)?;
        let from_hex = hex::encode(from);
        let to_hex = hex::encode(to);

        let mut objects: Vec<_> = response
            .items
            .unwrap_or_default()
            .into_iter()
            .filter(|obj| {
                obj.name.as_str() >= from_hex.as_str() && obj.name.as_str() <= to_hex.as_str()
            })
            .collect();

        objects.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(GcsRange {
            objects,
            current: 0,
        })
    }

    async fn peek_back(&self, key: &[u8]) -> Result<Option<Self::Entry>, Self::Error> {
        let key_hex = hex::encode(key);
        let request = ListObjectsRequest {
            bucket: self.bucket.clone(),
            ..Default::default()
        };

        let mut objects: Vec<_> = self
            .client
            .list_objects(&request)
            .await
            .map_err(GcsError::from)?
            .items
            .unwrap_or_default()
            .into_iter()
            .filter(|obj| obj.name.as_str() < key_hex.as_str())
            .collect();

        objects.sort_by(|a, b| a.name.cmp(&b.name));

        if let Some(obj) = objects.pop() {
            let get_request = GetObjectRequest {
                bucket: self.bucket.clone(),
                object: obj.name.clone(),
                ..Default::default()
            };

            let value = self
                .client
                .download_object(&get_request, &Range::default())
                .await
                .map_err(GcsError::from)?;

            Ok(Some(GcsEntry {
                key: hex::decode(&obj.name).unwrap_or_default(),
                value,
            }))
        } else {
            Ok(None)
        }
    }
}

pub struct GcsRange {
    objects: Vec<Object>,
    current: usize,
}

impl Iterator for GcsRange {
    type Item = GcsEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.objects.len() {
            return None;
        }
        let obj = &self.objects[self.current];
        self.current += 1;

        Some(GcsEntry {
            key: obj.name.clone().into_bytes(),
            value: vec![], // Need to download actual value
        })
    }
}

pub struct GcsEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl KVEntry for GcsEntry {
    fn key(&self) -> &[u8] {
        &self.key
    }
    fn value(&self) -> &[u8] {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use google_cloud_storage::http::buckets::insert::{BucketCreationConfig, InsertBucketRequest};

    use super::*;

    async fn ensure_test_bucket(client: &Client, bucket_name: &str) -> Result<(), GcsError> {
        let bucket = BucketCreationConfig {
            location: "US".to_string(),
            ..Default::default()
        };
        let request = InsertBucketRequest {
            name: bucket_name.to_string(),
            bucket,
            ..Default::default()
        };

        match client.insert_bucket(&request).await {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("already exists") => Ok(()),
            Err(e) => Err(GcsError::GoogleApi(e)),
        }
    }

    async fn create_test_store() -> GcsStore {
        let config = GcsConfig {
            bucket_name: "test-bucket".to_string(),
            endpoint: Some("http://localhost:4443".to_string()),
        };
        let store = GcsStore::new_with_config(config).await.unwrap();

        // Ensure bucket exists
        ensure_test_bucket(&store.client, &store.bucket)
            .await
            .unwrap();

        store
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let store = create_test_store().await;

        // Test upsert
        let key = b"test_key";
        let value = b"test_value";
        store.upsert(key, value).await.unwrap();

        // Test get
        let result = store.get(key).await.unwrap();
        assert_eq!(result, Some(value.to_vec()));

        // Test get non-existent
        let result = store.get(b"nonexistent").await.unwrap();
        assert_eq!(result, None);

        // Test remove
        store.remove(key).await.unwrap();
        let result = store.get(key).await;
        assert!(matches!(
            result,
            Err(GcsError::GoogleApi(e)) if e.to_string().contains("404")
        ));
    }

    #[tokio::test]
    async fn test_range_operations() {
        let store = create_test_store().await;

        // Insert test data
        let test_data = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        for (key, value) in &test_data {
            store.upsert(key, value).await.unwrap();
        }

        // Test iter_range
        let range = store.iter_range(b"key1", b"key3").await.unwrap();
        let entries: Vec<_> = range.collect();
        assert_eq!(entries.len(), 3);

        // Test remove_range
        store.remove_range(b"key1", b"key2").await.unwrap();

        // Verify key3 still exists but key1 and key2 are gone
        assert!(store.get(b"key3").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_peek_back() {
        let store = create_test_store().await;

        // Insert test data
        store.upsert(b"key1", b"value1").await.unwrap();
        store.upsert(b"key2", b"value2").await.unwrap();
        store.upsert(b"key3", b"value3").await.unwrap();

        // Test peek_back
        let result = store.peek_back(b"key3").await.unwrap();
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.key(), b"key2");
        assert_eq!(entry.value(), b"value2");
    }
}
