//! Postgres is the object store. One row is one object. The etag is the
//! S3 MD5: hex digest of a PutObject body, or the multipart form after
//! CompleteMultipartUpload. If-Match compares that token.
//!
//! A lost HTTP response after commit is still ambiguous, the same way a
//! lost S3 response is. A statement that returns no row did not write.

use crate::s3util::{
    common_prefix, content_etag, like_prefix_end, md5_bytes, multipart_etag, successor, Cursor,
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_postgres::{Client, NoTls};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS objects (
    bucket text NOT NULL,
    key bytea NOT NULL,
    body bytea NOT NULL,
    etag text NOT NULL,
    modified_at timestamptz NOT NULL,
    size bigint NOT NULL,
    content_type text,
    cache_control text,
    content_disposition text,
    content_encoding text,
    content_language text,
    storage_class text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (bucket, key)
);
CREATE TABLE IF NOT EXISTS uploads (
    bucket text NOT NULL,
    upload_id text NOT NULL,
    key bytea NOT NULL,
    created_at timestamptz NOT NULL,
    content_type text,
    cache_control text,
    content_disposition text,
    content_encoding text,
    content_language text,
    storage_class text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (bucket, upload_id)
);
CREATE TABLE IF NOT EXISTS upload_parts (
    bucket text NOT NULL,
    upload_id text NOT NULL,
    part_number integer NOT NULL,
    body bytea NOT NULL,
    etag text NOT NULL,
    PRIMARY KEY (bucket, upload_id, part_number)
);
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'objects'
      AND column_name = 'etag'
      AND data_type = 'bigint'
  ) THEN
    ALTER TABLE objects ALTER COLUMN etag TYPE text USING etag::text;
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'upload_parts'
      AND column_name = 'etag'
      AND data_type = 'bigint'
  ) THEN
    ALTER TABLE upload_parts ALTER COLUMN etag TYPE text USING etag::text;
  END IF;
END $$;
";

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("{0}")]
    Message(String),
    #[error("the conditional write was rejected")]
    Precondition,
    #[error("the object does not exist")]
    NotFound,
    #[error("the multipart upload does not exist")]
    NoSuchUpload,
    #[error("postgres: {0}")]
    Postgres(#[from] tokio_postgres::Error),
}

#[derive(Clone, Debug)]
pub struct ObjectAttrs {
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub storage_class: Option<String>,
    pub metadata: serde_json::Map<String, Value>,
}

impl Default for ObjectAttrs {
    fn default() -> Self {
        Self {
            content_type: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            storage_class: None,
            metadata: serde_json::Map::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Condition {
    Overwrite,
    /// `If-None-Match: *`. The write lands only when the key is absent.
    Create,
    IfMatch(String),
    /// `If-None-Match` of one etag. The write lands when the key is absent
    /// or the current token is different.
    IfNoneMatch(String),
}

#[derive(Clone, Debug)]
pub struct ObjectMeta {
    pub key: Vec<u8>,
    pub size: i64,
    pub etag: String,
    pub modified_at: DateTime<Utc>,
    pub attrs: ObjectAttrs,
}

#[derive(Clone, Debug)]
pub struct ObjectBody {
    pub meta: ObjectMeta,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug)]
pub enum ListEntry {
    Object(ObjectMeta),
    Prefix(Vec<u8>),
}

#[derive(Clone, Debug)]
pub struct ListPage {
    pub entries: Vec<ListEntry>,
    pub next: Option<Cursor>,
}

#[derive(Clone, Debug)]
pub struct PartInfo {
    pub number: i32,
    pub etag: String,
    pub size: i64,
}

#[derive(Clone, Debug)]
pub struct UploadInfo {
    pub key: Vec<u8>,
    pub upload_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

#[derive(Clone)]
struct PgPool {
    inner: std::sync::Arc<PoolInner>,
}

struct PoolInner {
    database_url: String,
    checkout: Mutex<mpsc::Receiver<Client>>,
    recycle: mpsc::Sender<Client>,
}

struct Pooled {
    client: Option<Client>,
    pool: PgPool,
    /// False after a transaction is opened until that transaction is
    /// committed or rolled back. Dropping the connection then is safer
    /// than handing it out while a rollback is still in flight.
    recycle: bool,
}

impl Drop for Pooled {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };
        if !self.recycle || client.is_closed() {
            self.pool.spawn_one();
            return;
        }
        if self.pool.inner.recycle.try_send(client).is_err() {
            self.pool.spawn_one();
        }
    }
}

impl PgPool {
    async fn connect(database_url: &str, size: usize) -> Result<Self, StoreError> {
        let size = size.max(1);
        let (recycle, receiver) = mpsc::channel(size);
        let pool = Self {
            inner: std::sync::Arc::new(PoolInner {
                database_url: database_url.to_string(),
                checkout: Mutex::new(receiver),
                recycle,
            }),
        };
        let setup = connect_client(database_url).await?;
        setup
            .batch_execute(&format!(
                "SELECT pg_advisory_lock(8675309); {SCHEMA} SELECT pg_advisory_unlock(8675309);"
            ))
            .await?;
        drop(setup);
        for _ in 0..size {
            let client = connect_client(database_url).await?;
            pool.inner
                .recycle
                .send(client)
                .await
                .map_err(|_| StoreError::Message("pool closed during startup".into()))?;
        }
        Ok(pool)
    }

    fn spawn_one(&self) {
        let pool = self.clone();
        tokio::spawn(async move {
            match connect_client(&pool.inner.database_url).await {
                Ok(client) => {
                    if pool.inner.recycle.send(client).await.is_err() {
                        // The process is shutting down.
                    }
                }
                Err(error) => {
                    eprintln!("pgs3 failed to replace a database connection: {error}");
                }
            }
        });
    }

    async fn get(&self) -> Result<Pooled, StoreError> {
        let mut receiver = self.inner.checkout.lock().await;
        let client = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
            .await
            .map_err(|_| StoreError::Message("timed out waiting for a database connection".into()))?
            .ok_or_else(|| StoreError::Message("database pool is closed".into()))?;
        drop(receiver);
        if client.is_closed() {
            self.spawn_one();
            return Err(StoreError::Message(
                "database connection closed before it could be used".into(),
            ));
        }
        Ok(Pooled {
            client: Some(client),
            pool: self.clone(),
            recycle: true,
        })
    }
}

async fn connect_client(database_url: &str) -> Result<Client, StoreError> {
    let mut config: tokio_postgres::Config = database_url
        .parse()
        .map_err(|error| StoreError::Message(format!("database url: {error}")))?;
    config.application_name("pgs3");
    let (client, connection) = config.connect(NoTls).await.map_err(|error| {
        StoreError::Message(format!(
            "connect to postgres failed ({error}); this build does not speak TLS, so use a local or private connection"
        ))
    })?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("pgs3 database connection ended: {error}");
        }
    });
    client
        .batch_execute("SET statement_timeout = '25s'")
        .await?;
    Ok(client)
}

impl Pooled {
    fn client(&self) -> &Client {
        self.client.as_ref().expect("pooled client is live")
    }

    fn client_mut(&mut self) -> &mut Client {
        self.client.as_mut().expect("pooled client is live")
    }
}

impl Store {
    pub async fn connect(database_url: &str, pool_size: usize) -> Result<Self, StoreError> {
        Ok(Self {
            pool: PgPool::connect(database_url, pool_size).await?,
        })
    }

    pub async fn ping(&self) -> Result<(), StoreError> {
        let pooled = self.pool.get().await?;
        pooled.client().execute("SELECT 1", &[]).await?;
        Ok(())
    }

    pub async fn put(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        attrs: &ObjectAttrs,
        condition: Condition,
    ) -> Result<ObjectMeta, StoreError> {
        let pooled = self.pool.get().await?;
        let client = pooled.client();
        let size = i64::try_from(body.len())
            .map_err(|_| StoreError::Message("object is larger than postgres can store".into()))?;
        let key_bytes: &[u8] = key.as_bytes();
        let metadata = Value::Object(attrs.metadata.clone());
        let new_etag = content_etag(body);
        let row = match &condition {
            Condition::Overwrite => {
                client
                    .query_opt(
                        OVERWRITE,
                        &[
                            &bucket,
                            &key_bytes,
                            &body,
                            &size,
                            &attrs.content_type,
                            &attrs.cache_control,
                            &attrs.content_disposition,
                            &attrs.content_encoding,
                            &attrs.content_language,
                            &attrs.storage_class,
                            &metadata,
                            &new_etag,
                        ],
                    )
                    .await?
            }
            Condition::Create => {
                client
                    .query_opt(
                        CREATE,
                        &[
                            &bucket,
                            &key_bytes,
                            &body,
                            &size,
                            &attrs.content_type,
                            &attrs.cache_control,
                            &attrs.content_disposition,
                            &attrs.content_encoding,
                            &attrs.content_language,
                            &attrs.storage_class,
                            &metadata,
                            &new_etag,
                        ],
                    )
                    .await?
            }
            Condition::IfMatch(etag) | Condition::IfNoneMatch(etag) => {
                let statement = match condition {
                    Condition::IfNoneMatch(_) => IF_NONE_MATCH,
                    _ => IF_MATCH,
                };
                client
                    .query_opt(
                        statement,
                        &[
                            &bucket,
                            &key_bytes,
                            &body,
                            &size,
                            &attrs.content_type,
                            &attrs.cache_control,
                            &attrs.content_disposition,
                            &attrs.content_encoding,
                            &attrs.content_language,
                            &attrs.storage_class,
                            &metadata,
                            etag,
                            &new_etag,
                        ],
                    )
                    .await?
            }
        };
        let Some(row) = row else {
            return Err(self.rejected(bucket, key, condition).await);
        };
        Ok(meta_from_write(key, size, attrs, &row)?)
    }

    /// A conditional write that changed no row. `If-Match` against a missing
    /// key is `NoSuchKey`; every other miss is a precondition failure.
    async fn rejected(&self, bucket: &str, key: &str, condition: Condition) -> StoreError {
        if !matches!(condition, Condition::IfMatch(_)) {
            return StoreError::Precondition;
        }
        match self.head(bucket, key).await {
            Ok(_) => StoreError::Precondition,
            Err(StoreError::NotFound) => StoreError::NotFound,
            Err(other) => other,
        }
    }

    pub async fn copy(
        &self,
        src_bucket: &str,
        src_key: &str,
        bucket: &str,
        key: &str,
        condition: Condition,
    ) -> Result<ObjectMeta, StoreError> {
        let source = self.get(src_bucket, src_key).await?;
        self.put(bucket, key, &source.body, &source.meta.attrs, condition)
            .await
    }

    pub async fn get(&self, bucket: &str, key: &str) -> Result<ObjectBody, StoreError> {
        let pooled = self.pool.get().await?;
        let row = pooled
            .client()
            .query_opt(
                &format!("SELECT {META_COLUMNS}, body FROM objects WHERE bucket = $1 AND key = $2"),
                &[&bucket, &key.as_bytes()],
            )
            .await?;
        let Some(row) = row else {
            return Err(StoreError::NotFound);
        };
        let meta = meta_from_row(&row, 0)?;
        let body: Vec<u8> = row.get(META_LEN);
        Ok(ObjectBody { meta, body })
    }

    pub async fn head(&self, bucket: &str, key: &str) -> Result<ObjectMeta, StoreError> {
        let pooled = self.pool.get().await?;
        let row = pooled
            .client()
            .query_opt(
                &format!("SELECT {META_COLUMNS} FROM objects WHERE bucket = $1 AND key = $2"),
                &[&bucket, &key.as_bytes()],
            )
            .await?;
        match row {
            Some(row) => meta_from_row(&row, 0),
            None => Err(StoreError::NotFound),
        }
    }

    pub async fn get_range(
        &self,
        bucket: &str,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<ObjectBody, StoreError> {
        if range.end < range.start {
            return Err(StoreError::Message("range is inverted".into()));
        }
        let length = range.end - range.start;
        if length > i32::MAX as u64 || range.start > i32::MAX as u64 {
            return Err(StoreError::Message(
                "range does not fit a postgres substring".into(),
            ));
        }
        let pooled = self.pool.get().await?;
        let start = i32::try_from(range.start).unwrap() + 1;
        let len = i32::try_from(length).unwrap();
        let row = pooled
            .client()
            .query_opt(
                &format!(
                    "SELECT {META_COLUMNS}, substring(body FROM $3::int FOR $4::int)
                     FROM objects WHERE bucket = $1 AND key = $2"
                ),
                &[&bucket, &key.as_bytes(), &start, &len],
            )
            .await?;
        let Some(row) = row else {
            return Err(StoreError::NotFound);
        };
        let meta = meta_from_row(&row, 0)?;
        let body: Vec<u8> = row.get(META_LEN);
        Ok(ObjectBody { meta, body })
    }

    pub async fn delete(&self, bucket: &str, key: &str) -> Result<(), StoreError> {
        let pooled = self.pool.get().await?;
        pooled
            .client()
            .execute(
                "DELETE FROM objects WHERE bucket = $1 AND key = $2",
                &[&bucket, &key.as_bytes()],
            )
            .await?;
        Ok(())
    }

    pub async fn list(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        cursor: Cursor,
        max_keys: usize,
        // Common prefixes at or before this marker were already returned.
        // ListObjects v1 addresses the next page with that prefix string,
        // and `key > "a/"` still lands inside the `a/` group.
        hide_prefixes_le: Option<&[u8]>,
    ) -> Result<ListPage, StoreError> {
        if max_keys == 0 {
            return Ok(ListPage {
                entries: Vec::new(),
                next: None,
            });
        }
        let prefix_bytes = prefix.as_bytes().to_vec();
        let end = like_prefix_end(&prefix_bytes);
        let delimiter = delimiter.filter(|value| !value.is_empty());
        let mut cursor = cursor;
        let mut entries = Vec::new();
        loop {
            let room = (max_keys + 1 - entries.len()).clamp(1, 500);
            let rows = self
                .fetch_keys(bucket, &prefix_bytes, end.as_deref(), &cursor, room)
                .await?;
            if rows.is_empty() {
                return Ok(ListPage {
                    entries,
                    next: None,
                });
            }
            let fetched = rows.len();
            let mut refetch = false;
            for row in rows {
                if entries.len() == max_keys {
                    return Ok(ListPage {
                        entries,
                        next: Some(Cursor {
                            inclusive: true,
                            bound: row.key,
                        }),
                    });
                }
                if let Some(delimiter) = delimiter {
                    if let Some(common) =
                        common_prefix(&row.key, &prefix_bytes, delimiter.as_bytes())
                    {
                        if hide_prefixes_le.is_some_and(|hide| common <= hide) {
                            match successor(common) {
                                Some(bound) => {
                                    cursor = Cursor {
                                        inclusive: true,
                                        bound,
                                    };
                                    refetch = true;
                                    break;
                                }
                                None => {
                                    return Ok(ListPage {
                                        entries,
                                        next: None,
                                    });
                                }
                            }
                        }
                        entries.push(ListEntry::Prefix(common.to_vec()));
                        match crate::s3util::successor(common) {
                            Some(bound) => {
                                cursor = Cursor {
                                    inclusive: true,
                                    bound,
                                };
                                refetch = true;
                                break;
                            }
                            None => {
                                return Ok(ListPage {
                                    entries,
                                    next: None,
                                });
                            }
                        }
                    }
                }
                cursor = Cursor {
                    inclusive: false,
                    bound: row.key.clone(),
                };
                entries.push(ListEntry::Object(ObjectMeta {
                    key: row.key,
                    size: row.size,
                    etag: row.etag,
                    modified_at: row.modified_at,
                    attrs: ObjectAttrs::default(),
                }));
            }
            // A short page is the end of the key range. A full page is not:
            // the limit is capped, so keep walking.
            if !refetch && fetched < room {
                return Ok(ListPage {
                    entries,
                    next: None,
                });
            }
        }
    }

    async fn fetch_keys(
        &self,
        bucket: &str,
        prefix: &[u8],
        end: Option<&[u8]>,
        cursor: &Cursor,
        limit: usize,
    ) -> Result<Vec<KeyRow>, StoreError> {
        let pooled = self.pool.get().await?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = pooled
            .client()
            .query(
                "SELECT key, size, etag, modified_at
                 FROM objects
                 WHERE bucket = $1
                   AND key >= $2
                   AND ($3::bytea IS NULL OR key < $3)
                   AND (
                        ($4::bool AND key >= $5)
                        OR (NOT $4::bool AND key > $5)
                   )
                 ORDER BY key
                 LIMIT $6",
                &[
                    &bucket,
                    &prefix,
                    &end,
                    &cursor.inclusive,
                    &cursor.bound,
                    &limit,
                ],
            )
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(KeyRow {
                    key: row.get(0),
                    size: row.get(1),
                    etag: row.get(2),
                    modified_at: row.get(3),
                })
            })
            .collect()
    }

    pub async fn create_upload(
        &self,
        bucket: &str,
        key: &str,
        attrs: &ObjectAttrs,
    ) -> Result<String, StoreError> {
        let upload_id = new_upload_id();
        let pooled = self.pool.get().await?;
        let metadata = Value::Object(attrs.metadata.clone());
        pooled
            .client()
            .execute(
                "INSERT INTO uploads (
                    bucket, upload_id, key, created_at, content_type, cache_control,
                    content_disposition, content_encoding, content_language, storage_class, metadata
                 ) VALUES ($1, $2, $3, now(), $4, $5, $6, $7, $8, $9, $10)",
                &[
                    &bucket,
                    &upload_id,
                    &key.as_bytes(),
                    &attrs.content_type,
                    &attrs.cache_control,
                    &attrs.content_disposition,
                    &attrs.content_encoding,
                    &attrs.content_language,
                    &attrs.storage_class,
                    &metadata,
                ],
            )
            .await?;
        Ok(upload_id)
    }

    pub async fn put_part(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: i32,
        body: &[u8],
    ) -> Result<String, StoreError> {
        if !(1..=10_000).contains(&part_number) {
            return Err(StoreError::Message(
                "partNumber must be between 1 and 10000".into(),
            ));
        }
        let etag = content_etag(body);
        let pooled = self.pool.get().await?;
        let row = pooled
            .client()
            .query_opt(
                "INSERT INTO upload_parts (bucket, upload_id, part_number, body, etag)
                 SELECT $1, upload_id, $3, $4, $5
                 FROM uploads
                 WHERE bucket = $1 AND upload_id = $2
                 ON CONFLICT (bucket, upload_id, part_number) DO UPDATE SET
                   body = EXCLUDED.body,
                   etag = EXCLUDED.etag
                 RETURNING etag",
                &[&bucket, &upload_id, &part_number, &body, &etag],
            )
            .await?;
        match row {
            Some(row) => Ok(row.get(0)),
            None => Err(StoreError::NoSuchUpload),
        }
    }

    pub async fn complete_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(i32, String)],
        condition: Condition,
        max_object_bytes: usize,
    ) -> Result<ObjectMeta, StoreError> {
        let mut pooled = self.pool.get().await?;
        pooled.recycle = false;
        let transaction = pooled.client_mut().transaction().await?;
        let upload = transaction
            .query_opt(
                "SELECT key, content_type, cache_control, content_disposition,
                        content_encoding, content_language, storage_class, metadata
                 FROM uploads
                 WHERE bucket = $1 AND upload_id = $2
                 FOR UPDATE",
                &[&bucket, &upload_id],
            )
            .await?;
        let Some(upload) = upload else {
            return Err(StoreError::NoSuchUpload);
        };
        let stored_key: Vec<u8> = upload.get(0);
        if stored_key != key.as_bytes() {
            return Err(StoreError::Message(
                "upload id belongs to a different key".into(),
            ));
        }
        let mut attrs = attrs_from_row(&upload, 1)?;
        // A completed upload with no content type is binary/octet-stream.
        // A plain PutObject with no content type is application/octet-stream.
        if attrs.content_type.as_deref().unwrap_or("").is_empty() {
            attrs.content_type = Some("binary/octet-stream".into());
        }
        let mut body = Vec::new();
        let mut digests = Vec::with_capacity(parts.len() * 16);
        for (number, etag) in parts {
            let row = transaction
                .query_opt(
                    "SELECT body, etag FROM upload_parts
                     WHERE bucket = $1 AND upload_id = $2 AND part_number = $3",
                    &[&bucket, &upload_id, number],
                )
                .await?;
            let Some(row) = row else {
                return Err(StoreError::Message(format!(
                    "part {number} was not uploaded"
                )));
            };
            let part_etag: String = row.get(1);
            if part_etag != *etag {
                return Err(StoreError::Message(format!(
                    "part {number} etag does not match the uploaded part"
                )));
            }
            let part: Vec<u8> = row.get(0);
            if body.len().saturating_add(part.len()) > max_object_bytes {
                return Err(StoreError::Message(
                    "assembled object exceeds the configured maximum".into(),
                ));
            }
            body.extend_from_slice(&part);
            digests.extend_from_slice(&md5_bytes(&part));
        }
        let new_etag = multipart_etag(&digests, parts.len());
        let size = i64::try_from(body.len()).unwrap_or(i64::MAX);
        let key_bytes: &[u8] = key.as_bytes();
        let metadata = Value::Object(attrs.metadata.clone());
        let written = match &condition {
            Condition::Create => {
                transaction
                    .query_opt(
                        CREATE,
                        &[
                            &bucket,
                            &key_bytes,
                            &body,
                            &size,
                            &attrs.content_type,
                            &attrs.cache_control,
                            &attrs.content_disposition,
                            &attrs.content_encoding,
                            &attrs.content_language,
                            &attrs.storage_class,
                            &metadata,
                            &new_etag,
                        ],
                    )
                    .await?
            }
            Condition::Overwrite => {
                transaction
                    .query_opt(
                        OVERWRITE,
                        &[
                            &bucket,
                            &key_bytes,
                            &body,
                            &size,
                            &attrs.content_type,
                            &attrs.cache_control,
                            &attrs.content_disposition,
                            &attrs.content_encoding,
                            &attrs.content_language,
                            &attrs.storage_class,
                            &metadata,
                            &new_etag,
                        ],
                    )
                    .await?
            }
            Condition::IfMatch(etag) | Condition::IfNoneMatch(etag) => {
                let statement = match condition {
                    Condition::IfNoneMatch(_) => IF_NONE_MATCH,
                    _ => IF_MATCH,
                };
                transaction
                    .query_opt(
                        statement,
                        &[
                            &bucket,
                            &key_bytes,
                            &body,
                            &size,
                            &attrs.content_type,
                            &attrs.cache_control,
                            &attrs.content_disposition,
                            &attrs.content_encoding,
                            &attrs.content_language,
                            &attrs.storage_class,
                            &metadata,
                            etag,
                            &new_etag,
                        ],
                    )
                    .await?
            }
        };
        let Some(written) = written else {
            let exists = transaction
                .query_opt(
                    "SELECT 1 FROM objects WHERE bucket = $1 AND key = $2",
                    &[&bucket, &key_bytes],
                )
                .await?;
            transaction.rollback().await?;
            pooled.recycle = true;
            return Err(match (condition, exists) {
                (Condition::IfMatch(_), None) => StoreError::NotFound,
                _ => StoreError::Precondition,
            });
        };
        transaction
            .execute(
                "DELETE FROM upload_parts WHERE bucket = $1 AND upload_id = $2",
                &[&bucket, &upload_id],
            )
            .await?;
        transaction
            .execute(
                "DELETE FROM uploads WHERE bucket = $1 AND upload_id = $2",
                &[&bucket, &upload_id],
            )
            .await?;
        transaction.commit().await?;
        pooled.recycle = true;
        meta_from_write(key, size, &attrs, &written)
    }

    pub async fn abort_upload(&self, bucket: &str, upload_id: &str) -> Result<(), StoreError> {
        let pooled = self.pool.get().await?;
        let client = pooled.client();
        client
            .execute(
                "DELETE FROM upload_parts WHERE bucket = $1 AND upload_id = $2",
                &[&bucket, &upload_id],
            )
            .await?;
        let removed = client
            .execute(
                "DELETE FROM uploads WHERE bucket = $1 AND upload_id = $2",
                &[&bucket, &upload_id],
            )
            .await?;
        if removed == 0 {
            return Err(StoreError::NoSuchUpload);
        }
        Ok(())
    }

    pub async fn list_parts(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> Result<(Vec<u8>, Vec<PartInfo>), StoreError> {
        let pooled = self.pool.get().await?;
        let client = pooled.client();
        let upload = client
            .query_opt(
                "SELECT key FROM uploads WHERE bucket = $1 AND upload_id = $2",
                &[&bucket, &upload_id],
            )
            .await?;
        let Some(upload) = upload else {
            return Err(StoreError::NoSuchUpload);
        };
        let key: Vec<u8> = upload.get(0);
        let rows = client
            .query(
                "SELECT part_number, etag, octet_length(body)::bigint
                 FROM upload_parts
                 WHERE bucket = $1 AND upload_id = $2
                 ORDER BY part_number",
                &[&bucket, &upload_id],
            )
            .await?;
        let parts = rows
            .into_iter()
            .map(|row| PartInfo {
                number: row.get(0),
                etag: row.get(1),
                size: row.get(2),
            })
            .collect();
        Ok((key, parts))
    }

    pub async fn list_uploads(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<UploadInfo>, StoreError> {
        let pooled = self.pool.get().await?;
        let prefix_bytes = prefix.as_bytes().to_vec();
        let end = like_prefix_end(&prefix_bytes);
        let rows = pooled
            .client()
            .query(
                "SELECT key, upload_id, created_at
                 FROM uploads
                 WHERE bucket = $1
                   AND key >= $2
                   AND ($3::bytea IS NULL OR key < $3)
                 ORDER BY key, upload_id
                 LIMIT 1000",
                &[&bucket, &prefix_bytes, &end],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| UploadInfo {
                key: row.get(0),
                upload_id: row.get(1),
                created_at: row.get(2),
            })
            .collect())
    }
}

struct KeyRow {
    key: Vec<u8>,
    size: i64,
    etag: String,
    modified_at: DateTime<Utc>,
}

const META_COLUMNS: &str = "
    key, size, etag, modified_at, content_type, cache_control, content_disposition,
    content_encoding, content_language, storage_class, metadata
";
const META_LEN: usize = 11;

const OVERWRITE: &str = "
INSERT INTO objects (
    bucket, key, body, etag, modified_at, size,
    content_type, cache_control, content_disposition, content_encoding,
    content_language, storage_class, metadata
) VALUES (
    $1, $2, $3, $12, now(), $4,
    $5, $6, $7, $8, $9, $10, $11
)
ON CONFLICT (bucket, key) DO UPDATE SET
    body = EXCLUDED.body,
    etag = EXCLUDED.etag,
    modified_at = EXCLUDED.modified_at,
    size = EXCLUDED.size,
    content_type = EXCLUDED.content_type,
    cache_control = EXCLUDED.cache_control,
    content_disposition = EXCLUDED.content_disposition,
    content_encoding = EXCLUDED.content_encoding,
    content_language = EXCLUDED.content_language,
    storage_class = EXCLUDED.storage_class,
    metadata = EXCLUDED.metadata
RETURNING etag, modified_at
";

const CREATE: &str = "
INSERT INTO objects (
    bucket, key, body, etag, modified_at, size,
    content_type, cache_control, content_disposition, content_encoding,
    content_language, storage_class, metadata
) VALUES (
    $1, $2, $3, $12, now(), $4,
    $5, $6, $7, $8, $9, $10, $11
)
ON CONFLICT (bucket, key) DO NOTHING
RETURNING etag, modified_at
";

const IF_MATCH: &str = "
UPDATE objects SET
    body = $3,
    etag = $13,
    modified_at = now(),
    size = $4,
    content_type = $5,
    cache_control = $6,
    content_disposition = $7,
    content_encoding = $8,
    content_language = $9,
    storage_class = $10,
    metadata = $11
WHERE bucket = $1 AND key = $2 AND etag = $12
RETURNING etag, modified_at
";

const IF_NONE_MATCH: &str = "
INSERT INTO objects (
    bucket, key, body, etag, modified_at, size,
    content_type, cache_control, content_disposition, content_encoding,
    content_language, storage_class, metadata
) VALUES (
    $1, $2, $3, $13, now(), $4,
    $5, $6, $7, $8, $9, $10, $11
)
ON CONFLICT (bucket, key) DO UPDATE SET
    body = EXCLUDED.body,
    etag = EXCLUDED.etag,
    modified_at = EXCLUDED.modified_at,
    size = EXCLUDED.size,
    content_type = EXCLUDED.content_type,
    cache_control = EXCLUDED.cache_control,
    content_disposition = EXCLUDED.content_disposition,
    content_encoding = EXCLUDED.content_encoding,
    content_language = EXCLUDED.content_language,
    storage_class = EXCLUDED.storage_class,
    metadata = EXCLUDED.metadata
WHERE objects.etag <> $12
RETURNING etag, modified_at
";

fn meta_from_write(
    key: &str,
    size: i64,
    attrs: &ObjectAttrs,
    row: &tokio_postgres::Row,
) -> Result<ObjectMeta, StoreError> {
    Ok(ObjectMeta {
        key: key.as_bytes().to_vec(),
        size,
        etag: row.get(0),
        modified_at: row.get(1),
        attrs: attrs.clone(),
    })
}

fn meta_from_row(row: &tokio_postgres::Row, start: usize) -> Result<ObjectMeta, StoreError> {
    let key: Vec<u8> = row.get(start);
    Ok(ObjectMeta {
        key,
        size: row.get(start + 1),
        etag: row.get(start + 2),
        modified_at: row.get(start + 3),
        attrs: attrs_from_row(row, start + 4)?,
    })
}

fn attrs_from_row(row: &tokio_postgres::Row, start: usize) -> Result<ObjectAttrs, StoreError> {
    let metadata: Value = row.get(start + 6);
    let metadata = match metadata {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    Ok(ObjectAttrs {
        content_type: row.get(start),
        cache_control: row.get(start + 1),
        content_disposition: row.get(start + 2),
        content_encoding: row.get(start + 3),
        content_language: row.get(start + 4),
        storage_class: row.get(start + 5),
        metadata,
    })
}

fn new_upload_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).expect("operating system random source");
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
