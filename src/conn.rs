#![allow(dead_code)]
use futures_util::sink::SinkExt;
use futures_util::StreamExt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use tokio::spawn;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use yrs::encoding::read::Cursor;
use yrs::sync::Awareness;
use yrs::sync::{DefaultProtocol, Error, Message, MessageReader, Protocol, SyncMessage};
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Transact, Update};

use crate::storage::kv::DocOps;
use crate::storage::sqlite::SqliteStore;
use redis::aio::MultiplexedConnection as RedisConnection;
use redis::AsyncCommands;

/// Connection configuration options
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Whether to enable persistent storage
    pub storage_enabled: bool,
    /// Document name for storage (required if storage enabled)
    pub doc_name: Option<String>,
    /// Redis configuration (optional)
    pub redis_config: Option<RedisConfig>,
}

/// Redis cache configuration
#[derive(Debug, Clone, Default)]
pub struct RedisConfig {
    /// Redis URL
    pub url: String,
    /// Cache TTL in seconds
    pub ttl: u64,
}

/// Connection handler over a pair of message streams, which implements a Yjs/Yrs awareness and
/// update exchange protocol.
pub struct Connection<Sink, Stream> {
    processing_loop: JoinHandle<Result<(), Error>>,
    awareness: Arc<RwLock<Awareness>>,
    inbox: Arc<Mutex<Sink>>,
    _stream: PhantomData<Stream>,
    storage_sub: StorageSubscription,
    redis: Option<Arc<Mutex<RedisConnection>>>,
}

struct StorageSubscription {
    inner: Arc<Mutex<Option<yrs::Subscription>>>,
}

impl StorageSubscription {
    fn new(sub: Option<yrs::Subscription>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(sub)),
        }
    }
}

impl Default for StorageSubscription {
    fn default() -> Self {
        Self::new(None)
    }
}

impl std::fmt::Debug for StorageSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageSubscription")
            .field("inner", &"<subscription>")
            .finish()
    }
}

impl<Sink, Stream, E> Connection<Sink, Stream>
where
    Sink: SinkExt<Vec<u8>, Error = E> + Send + Sync + Unpin + 'static,
    E: Into<Error> + Send + Sync,
{
    pub async fn send(&self, msg: Vec<u8>) -> Result<(), Error> {
        let mut inbox = self.inbox.lock().await;
        inbox.send(msg).await.map_err(Into::into)
    }

    pub async fn close(self) -> Result<(), E> {
        let mut inbox = self.inbox.lock().await;
        inbox.close().await
    }

    pub fn sink(&self) -> Weak<Mutex<Sink>> {
        Arc::downgrade(&self.inbox)
    }
}

impl<Sink, Stream, E> Connection<Sink, Stream>
where
    Stream: StreamExt<Item = Result<Vec<u8>, E>> + Send + Sync + Unpin + 'static,
    Sink: SinkExt<Vec<u8>, Error = E> + Send + Sync + Unpin + 'static,
    E: Into<Error> + Send + Sync,
{
    /// Creates a new connection with default protocol
    pub fn new(awareness: Arc<RwLock<Awareness>>, sink: Sink, stream: Stream) -> Self {
        Self::with_protocol(awareness, sink, stream, DefaultProtocol)
    }

    /// Returns an underlying [Awareness] structure
    pub fn awareness(&self) -> &Arc<RwLock<Awareness>> {
        &self.awareness
    }

    /// Creates a new connection with storage and Redis cache support
    pub async fn with_storage(
        awareness: Arc<RwLock<Awareness>>,
        sink: Sink,
        stream: Stream,
        store: Arc<SqliteStore>,
        config: ConnectionConfig,
    ) -> Self {
        if !config.storage_enabled {
            return Self::new(awareness, sink, stream);
        }

        let doc_name = config
            .doc_name
            .expect("doc_name required when storage enabled");
        let redis_ttl = config.redis_config.as_ref().map(|c| c.ttl as usize);

        // Initialize Redis connection
        let redis = match config.redis_config {
            Some(redis_config) => match Self::init_redis_connection(&redis_config.url).await {
                Ok(conn) => {
                    if let Ok(()) = Self::load_from_redis(&conn, &doc_name, &awareness).await {
                        return Self::with_redis(awareness, sink, stream, conn);
                    }
                    Some(conn)
                }
                Err(e) => {
                    tracing::error!("Failed to initialize Redis connection: {}", e);
                    None
                }
            },
            None => None,
        };

        // Load from persistent storage
        Self::load_from_storage(&store, &doc_name, &awareness).await;

        // Set up storage subscription
        let storage_sub =
            Self::setup_storage_subscription(&awareness, store, doc_name, redis.clone(), redis_ttl)
                .await;

        let mut conn = Self::with_protocol(awareness, sink, stream, DefaultProtocol);
        conn.storage_sub = storage_sub;
        conn.redis = redis;
        conn
    }

    async fn init_redis_connection(
        url: &str,
    ) -> Result<Arc<Mutex<RedisConnection>>, redis::RedisError> {
        let client = redis::Client::open(url)?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Arc::new(Mutex::new(conn)))
    }

    async fn load_from_redis(
        redis: &Arc<Mutex<RedisConnection>>,
        doc_name: &str,
        awareness: &Arc<RwLock<Awareness>>,
    ) -> Result<(), Error> {
        let mut conn = redis.lock().await;
        let cache_key = format!("doc:{}", doc_name);

        let cached_data: Vec<u8> = conn
            .get(&cache_key)
            .await
            .map_err(|e| Error::Other(e.into()))?;
        let update = Update::decode_v1(&cached_data)?;

        let awareness_guard = awareness.write().await;
        let mut txn = awareness_guard.doc().transact_mut();
        txn.apply_update(update)?;

        tracing::debug!("Successfully loaded document from Redis cache");
        Ok(())
    }

    async fn load_from_storage(
        store: &Arc<SqliteStore>,
        doc_name: &str,
        awareness: &Arc<RwLock<Awareness>>,
    ) {
        let awareness = awareness.write().await;
        let mut txn = awareness.doc().transact_mut();
        match store.load_doc(doc_name, &mut txn).await {
            Ok(_) => {
                tracing::info!("Successfully loaded document '{}' from storage", doc_name);
            }
            Err(e) => {
                tracing::error!("Failed to load document '{}' from storage: {}", doc_name, e);
            }
        }
    }

    async fn setup_storage_subscription(
        awareness: &Arc<RwLock<Awareness>>,
        store: Arc<SqliteStore>,
        doc_name: String,
        redis: Option<Arc<Mutex<RedisConnection>>>,
        redis_ttl: Option<usize>,
    ) -> StorageSubscription {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let storage_sub = {
            let awareness = awareness.read().await;
            awareness
                .doc()
                .observe_update_v1(move |_, update| {
                    if let Err(e) = tx.send(update.update.clone()) {
                        tracing::error!("Failed to send update to storage channel: {}", e);
                    }
                })
                .unwrap()
        };

        let doc_name_clone = doc_name.clone();
        tokio::spawn(async move {
            while let Some(update) = rx.recv().await {
                Self::handle_update(update, &doc_name_clone, &store, &redis, redis_ttl).await;
            }
        });

        StorageSubscription::new(Some(storage_sub))
    }

    async fn handle_update(
        update: Vec<u8>,
        doc_name: &str,
        store: &Arc<SqliteStore>,
        redis: &Option<Arc<Mutex<RedisConnection>>>,
        redis_ttl: Option<usize>,
    ) {
        // Store in persistent storage
        if let Err(e) = store.push_update(doc_name, &update).await {
            tracing::error!("Failed to store update: {}", e);
            return;
        }

        // Update Redis cache if enabled
        if let (Some(redis), Some(ttl)) = (redis, redis_ttl) {
            let mut conn = redis.lock().await;
            let cache_key = format!("doc:{}", doc_name);
            if let Err(e) = conn
                .set_ex::<_, _, String>(&cache_key, update.as_slice(), ttl.try_into().unwrap())
                .await
            {
                tracing::error!("Failed to update Redis cache: {}", e);
            }
        }
    }

    /// Creates a new connection with Redis cache only
    fn with_redis(
        awareness: Arc<RwLock<Awareness>>,
        sink: Sink,
        stream: Stream,
        redis: Arc<Mutex<RedisConnection>>,
    ) -> Self {
        let mut conn = Self::with_protocol(awareness, sink, stream, DefaultProtocol);
        conn.redis = Some(redis);
        conn
    }

    /// Creates a new connection with custom protocol
    pub fn with_protocol<P>(
        awareness: Arc<RwLock<Awareness>>,
        sink: Sink,
        mut stream: Stream,
        protocol: P,
    ) -> Self
    where
        P: Protocol + Send + Sync + 'static,
    {
        let sink = Arc::new(Mutex::new(sink));
        let inbox = sink.clone();
        let loop_sink = Arc::downgrade(&sink);
        let loop_awareness = Arc::downgrade(&awareness);

        let processing_loop = spawn(async move {
            Self::run_processing_loop(protocol, loop_awareness, loop_sink, &mut stream).await
        });

        Connection {
            processing_loop,
            awareness,
            inbox,
            _stream: PhantomData,
            storage_sub: StorageSubscription::default(),
            redis: None,
        }
    }

    async fn run_processing_loop<P: Protocol>(
        protocol: P,
        loop_awareness: Weak<RwLock<Awareness>>,
        loop_sink: Weak<Mutex<Sink>>,
        stream: &mut Stream,
    ) -> Result<(), Error> {
        // Send initial sync messages
        if let Some(sink) = loop_sink.upgrade() {
            if let Some(awareness) = loop_awareness.upgrade() {
                let payload = {
                    let mut encoder = EncoderV1::new();
                    let awareness = awareness.read().await;
                    protocol.start(&awareness, &mut encoder)?;
                    encoder.to_vec()
                };
                if !payload.is_empty() {
                    let mut s = sink.lock().await;
                    s.send(payload).await.map_err(Into::into)?;
                }
            }
        }

        // Process incoming messages
        while let Some(input) = stream.next().await {
            match input {
                Ok(data) => {
                    if let (Some(sink), Some(awareness)) =
                        (loop_sink.upgrade(), loop_awareness.upgrade())
                    {
                        Self::process(&protocol, &awareness, &sink, data).await?;
                    } else {
                        return Ok(());
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    async fn process<P: Protocol>(
        protocol: &P,
        awareness: &Arc<RwLock<Awareness>>,
        sink: &Arc<Mutex<Sink>>,
        input: Vec<u8>,
    ) -> Result<(), Error> {
        let mut decoder = DecoderV1::new(Cursor::new(&input));
        let reader = MessageReader::new(&mut decoder);
        for msg in reader {
            if let Some(reply) = handle_msg(protocol, awareness, msg?).await? {
                let mut sender = sink.lock().await;
                sender.send(reply.encode_v1()).await.map_err(Into::into)?;
            }
        }
        Ok(())
    }
}

impl<Sink, Stream> Future for Connection<Sink, Stream> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.processing_loop).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::Other(e.into()))),
        }
    }
}

async fn handle_msg<P: Protocol>(
    protocol: &P,
    awareness: &Arc<RwLock<Awareness>>,
    msg: Message,
) -> Result<Option<Message>, Error> {
    match msg {
        Message::Sync(msg) => match msg {
            SyncMessage::SyncStep1(sv) => {
                let awareness = awareness.read().await;
                protocol.handle_sync_step1(&awareness, sv)
            }
            SyncMessage::SyncStep2(update) => {
                let awareness = awareness.write().await;
                protocol.handle_sync_step2(&awareness, Update::decode_v1(&update)?)
            }
            SyncMessage::Update(update) => {
                let awareness = awareness.write().await;
                protocol.handle_update(&awareness, Update::decode_v1(&update)?)
            }
        },
        Message::Auth(reason) => {
            let awareness = awareness.read().await;
            protocol.handle_auth(&awareness, reason)
        }
        Message::AwarenessQuery => {
            let awareness = awareness.read().await;
            protocol.handle_awareness_query(&awareness)
        }
        Message::Awareness(update) => {
            let awareness = awareness.write().await;
            protocol.handle_awareness_update(&awareness, update)
        }
        Message::Custom(tag, data) => {
            let awareness = awareness.write().await;
            protocol.missing_handle(&awareness, tag, data)
        }
    }
}

impl<Sink, Stream> Unpin for Connection<Sink, Stream> {}
