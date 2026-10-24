//! One `object_store` 0.12 client, pointed at pgs3 and at RustFS.
//! The same calls go to both, and the results are compared.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::StreamExt;
use hmac::{Hmac, Mac};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{
    Error, GetOptions, GetRange, ObjectMeta, ObjectStore, PutMode,
    PutOptions, PutPayload, PutResult, RetryConfig, UpdateVersion,
};
use pgs3::Store;
use sha2::{Digest, Sha256};

const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const REGION: &str = "us-east-1";
const KEYS: &[&str] = &["a", "a/b", "a/c", "b", "c/d"];

struct Servers {
    pgs3: u16,
    rustfs_port: u16,
    rustfs: Child,
}

impl Drop for Servers {
    fn drop(&mut self) {
        let _ = self.rustfs.kill();
        let _ = self.rustfs.wait();
    }
}

fn hold() -> &'static Servers {
    static STARTED: std::sync::OnceLock<Servers> = std::sync::OnceLock::new();
    STARTED.get_or_init(|| {
        let (rustfs_port, rustfs) = spawn_rustfs();
        Servers {
            pgs3: spawn_pgs3(),
            rustfs_port,
            rustfs,
        }
    })
}

fn spawn_pgs3() -> u16 {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("pgs3 runtime");
        runtime.block_on(async move {
            let url = std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://pgstore:pgstore@127.0.0.1/pgstore".to_string());
            let store = Store::connect(&url, 8)
                .await
                .expect("connect to postgres; set DATABASE_URL if the default role is wrong");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            sender.send(port).unwrap();
            axum::serve(listener, pgs3::router(store, 8 * 1024 * 1024))
                .await
                .unwrap();
        });
    });
    receiver.recv().expect("pgs3 did not bind")
}

fn spawn_rustfs() -> (u16, Child) {
    let bin = rustfs_bin();
    let port = free_port();
    let volume = std::env::temp_dir().join(format!("pgs3-rustfs-{port}"));
    std::fs::create_dir_all(&volume).unwrap();
    let log_path = std::env::temp_dir().join(format!("pgs3-rustfs-{port}.log"));
    let log = std::fs::File::create(&log_path).unwrap();
    let child = Command::new(&bin)
        .arg("server")
        .arg(&volume)
        .env("RUSTFS_ACCESS_KEY", ACCESS_KEY)
        .env("RUSTFS_SECRET_KEY", SECRET_KEY)
        .env("RUSTFS_ADDRESS", format!("127.0.0.1:{port}"))
        .env("RUSTFS_REGION", REGION)
        .env("RUSTFS_CONSOLE_ENABLE", "false")
        .env("RUSTFS_OBS_LOGGER_LEVEL", "error")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap_or_else(|error| panic!("spawn {}: {error}", bin.display()));
    for _ in 0..50 {
        if rustfs_healthy(port) {
            return (port, child);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let log_text = std::fs::read_to_string(&log_path).unwrap_or_default();
    panic!("rustfs did not become healthy on {port}:\n{log_text}");
}

fn rustfs_healthy(port: u16) -> bool {
    use std::io::{Read, Write};
    let address = format!("127.0.0.1:{port}");
    let Ok(mut stream) = std::net::TcpStream::connect(&address) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
    let request = format!("GET /health HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut buf = [0u8; 80];
    let n = stream.read(&mut buf).unwrap_or(0);
    std::str::from_utf8(&buf[..n]).unwrap_or("").contains("200")
}

fn rustfs_bin() -> PathBuf {
    let path = std::env::var("RUSTFS_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/opt/rustfs-oracle/rustfs"));
    if path.exists() {
        path
    } else {
        panic!("RustFS oracle is missing at {}", path.display());
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn client(port: u16, bucket: &str) -> Arc<dyn ObjectStore> {
    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(REGION)
        .with_endpoint(format!("http://127.0.0.1:{port}"))
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_retry(RetryConfig {
            max_retries: 0,
            retry_timeout: Duration::from_secs(2),
            ..RetryConfig::default()
        })
        .build()
        .expect("object_store client");
    Arc::new(store)
}

async fn create_bucket(port: u16, bucket: &str) {
    let host = format!("127.0.0.1:{port}");
    let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let payload = sha256_hex(b"");
    let mut headers = BTreeMap::new();
    headers.insert("host".into(), host.clone());
    headers.insert("x-amz-content-sha256".into(), payload.clone());
    headers.insert("x-amz-date".into(), amz_date.clone());
    let authorization = sign("PUT", &format!("/{bucket}"), &headers, &amz_date, b"");
    let response = reqwest::Client::new()
        .put(format!("http://{host}/{bucket}"))
        .header("x-amz-content-sha256", payload)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization)
        .send()
        .await
        .expect("create bucket");
    assert!(
        response.status().is_success(),
        "create bucket {bucket}: {}",
        response.status()
    );
}

fn sign(
    method: &str,
    path: &str,
    headers: &BTreeMap<String, String>,
    amz_date: &str,
    body: &[u8],
) -> String {
    let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", value.trim()))
        .collect::<String>();
    let canonical = format!(
        "{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{}",
        sha256_hex(body)
    );
    let date = &amz_date[..8];
    let scope = format!("{date}/{REGION}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let mut mac = <Hmac<Sha256>>::new_from_slice(format!("AWS4{SECRET_KEY}").as_bytes()).unwrap();
    mac.update(date.as_bytes());
    let mut mac = <Hmac<Sha256>>::new_from_slice(&mac.finalize().into_bytes()).unwrap();
    mac.update(REGION.as_bytes());
    let mut mac = <Hmac<Sha256>>::new_from_slice(&mac.finalize().into_bytes()).unwrap();
    mac.update(b"s3");
    let mut mac = <Hmac<Sha256>>::new_from_slice(&mac.finalize().into_bytes()).unwrap();
    mac.update(b"aws4_request");
    let key = mac.finalize().into_bytes();
    let mut mac = <Hmac<Sha256>>::new_from_slice(&key).unwrap();
    mac.update(string_to_sign.as_bytes());
    let signature = hex_encode(&mac.finalize().into_bytes());
    format!(
        "AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_encode(&Sha256::digest(bytes))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

fn norm_etag(etag: Option<&str>) -> Option<String> {
    let etag = etag?.trim();
    let etag = etag.strip_prefix("W/").unwrap_or(etag).trim();
    let etag = etag.trim_matches('"');
    Some(etag.to_ascii_lowercase())
}

fn err_kind(error: &Error) -> &'static str {
    match error {
        Error::NotFound { .. } => "not-found",
        Error::AlreadyExists { .. } => "exists",
        Error::Precondition { .. } => "precondition",
        Error::NotModified { .. } => "not-modified",
        Error::Generic { .. } => "generic",
        Error::NotSupported { .. } => "unsupported",
        Error::NotImplemented => "unimplemented",
        Error::PermissionDenied { .. } => "denied",
        Error::Unauthenticated { .. } => "unauthenticated",
        Error::InvalidPath { .. } => "invalid-path",
        Error::JoinError { .. } => "join",
        Error::UnknownConfigurationKey { .. } => "config",
        _ => "other",
    }
}

fn put_tag(result: Result<PutResult, Error>) -> Result<Option<String>, &'static str> {
    match result {
        Ok(put) => Ok(norm_etag(put.e_tag.as_deref())),
        Err(error) => Err(err_kind(&error)),
    }
}

fn unit_tag(result: Result<(), Error>) -> Result<(), &'static str> {
    result.map_err(|error| err_kind(&error))
}

struct Saved {
    body: Vec<u8>,
    etag: String,
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        self.0
    }

    fn index(&mut self, len: usize) -> usize {
        (self.next() as usize) % len
    }

    fn bytes(&mut self) -> Vec<u8> {
        let len = self.index(24);
        (0..len).map(|_| self.next() as u8).collect()
    }
}

#[tokio::test]
async fn fuzz() {
    let servers = hold();
    let seeds = std::env::var("PGS3_FUZZ_SEEDS").unwrap_or_else(|_| "1,2,3,4,5,6,7,8".into());
    for seed in seeds.split(',') {
        let seed: u64 = seed
            .trim()
            .parse()
            .expect("PGS3_FUZZ_SEEDS is a comma list of integers");
        fuzz_one(servers, seed).await;
    }
}

async fn fuzz_one(servers: &Servers, seed: u64) {
    let bucket = format!("fuzz{seed}");
    create_bucket(servers.rustfs_port, &bucket).await;
    let left = client(servers.pgs3, &bucket);
    let right = client(servers.rustfs_port, &bucket);
    let mut objects: BTreeMap<String, Saved> = BTreeMap::new();
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));

    for step in 0..20 {
        let key = KEYS[rng.index(KEYS.len())].to_string();
        let label = format!("seed {seed} step {step} {key}");
        match rng.index(12) {
            0 | 1 => {
                let body = rng.bytes();
                put_overwrite(&left, &right, &mut objects, &label, &key, body).await;
            }
            2 => put_create(&left, &right, &mut objects, &label, &key).await,
            3 => put_update(&left, &right, &mut objects, &label, &key, true).await,
            4 => put_update(&left, &right, &mut objects, &label, &key, false).await,
            5 => read_both(&left, &right, &objects, &label, &key, None).await,
            6 => {
                let len = objects.get(&key).map(|saved| saved.body.len()).unwrap_or(0);
                if len == 0 {
                    continue;
                }
                let start = (rng.index(len) as u64).min(len as u64 - 1);
                let end = start + 1 + rng.index(len) as u64;
                read_both(
                    &left,
                    &right,
                    &objects,
                    &label,
                    &key,
                    Some(GetRange::Bounded(start..end)),
                )
                .await;
            }
            7 => head_both(&left, &right, &objects, &label, &key).await,
            8 => delete_one(&left, &right, &mut objects, &label, &key).await,
            9 => copy_one(&left, &right, &mut objects, &label, &key, &rng_key(&mut rng)).await,
            10 => list_both(&left, &right, &objects, &label).await,
            _ => multipart_one(&left, &right, &mut objects, &label, &key, rng.bytes()).await,
        }
    }
}

fn rng_key(rng: &mut Rng) -> String {
    KEYS[rng.index(KEYS.len())].to_string()
}

async fn put_overwrite(
    left: &Arc<dyn ObjectStore>,
    right: &Arc<dyn ObjectStore>,
    objects: &mut BTreeMap<String, Saved>,
    label: &str,
    key: &str,
    body: Vec<u8>,
) {
    let path = Path::from(key);
    let a = put_tag(left.put(&path, PutPayload::from(body.clone())).await);
    let b = put_tag(right.put(&path, PutPayload::from(body.clone())).await);
    assert_eq!(a, b, "{label} put");
    let etag = a.expect("put").expect("etag");
    objects.insert(key.to_string(), Saved { body, etag });
}

async fn put_create(
    left: &Arc<dyn ObjectStore>,
    right: &Arc<dyn ObjectStore>,
    objects: &mut BTreeMap<String, Saved>,
    label: &str,
    key: &str,
) {
    let path = Path::from(key);
    let body = b"created".to_vec();
    let options = PutOptions::from(PutMode::Create);
    let a = put_tag(
        left.put_opts(&path, PutPayload::from(body.clone()), options.clone())
            .await,
    );
    let b = put_tag(
        right
            .put_opts(&path, PutPayload::from(body.clone()), options)
            .await,
    );
    assert_eq!(a, b, "{label} create");
    if let Ok(Some(etag)) = a {
        objects.insert(key.to_string(), Saved { body, etag });
    }
}

async fn put_update(
    left: &Arc<dyn ObjectStore>,
    right: &Arc<dyn ObjectStore>,
    objects: &mut BTreeMap<String, Saved>,
    label: &str,
    key: &str,
    current: bool,
) {
    let path = Path::from(key);
    let etag = if current {
        objects.get(key).map(|saved| saved.etag.clone())
    } else {
        Some("00000000000000000000000000000000".into())
    };
    let Some(etag) = etag else {
        return;
    };
    let body = b"updated".to_vec();
    let options = PutOptions::from(PutMode::Update(UpdateVersion {
        e_tag: Some(etag),
        version: None,
    }));
    let a = put_tag(
        left.put_opts(&path, PutPayload::from(body.clone()), options.clone())
            .await,
    );
    let b = put_tag(
        right
            .put_opts(&path, PutPayload::from(body.clone()), options)
            .await,
    );
    assert_eq!(a, b, "{label} update current={current}");
    if let Ok(Some(etag)) = a {
        objects.insert(key.to_string(), Saved { body, etag });
    }
}

async fn read_both(
    left: &Arc<dyn ObjectStore>,
    right: &Arc<dyn ObjectStore>,
    objects: &BTreeMap<String, Saved>,
    label: &str,
    key: &str,
    range: Option<GetRange>,
) {
    let path = Path::from(key);
    let options = GetOptions {
        range: range.clone(),
        ..GetOptions::default()
    };
    let a = fetch(left, &path, options.clone()).await;
    let b = fetch(right, &path, options).await;
    assert_eq!(a, b, "{label} get {range:?}");
    if let (Ok(got), Some(saved)) = (&a, objects.get(key)) {
        if range.is_none() {
            assert_eq!(got.bytes, saved.body, "{label} body");
            assert_eq!(got.etag.as_deref(), Some(saved.etag.as_str()), "{label} etag");
        }
    }
}

async fn head_both(
    left: &Arc<dyn ObjectStore>,
    right: &Arc<dyn ObjectStore>,
    objects: &BTreeMap<String, Saved>,
    label: &str,
    key: &str,
) {
    let path = Path::from(key);
    let a = meta_of(left.head(&path).await);
    let b = meta_of(right.head(&path).await);
    assert_eq!(a, b, "{label} head");
    if let (Ok((size, etag)), Some(saved)) = (&a, objects.get(key)) {
        assert_eq!(*size, saved.body.len() as u64, "{label} size");
        assert_eq!(etag.as_deref(), Some(saved.etag.as_str()), "{label} head etag");
    }
}

fn meta_of(result: Result<ObjectMeta, Error>) -> Result<(u64, Option<String>), &'static str> {
    match result {
        Ok(meta) => Ok((meta.size, norm_etag(meta.e_tag.as_deref()))),
        Err(error) => Err(err_kind(&error)),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Got {
    bytes: Vec<u8>,
    etag: Option<String>,
    size: u64,
}

async fn fetch(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    options: GetOptions,
) -> Result<Got, &'static str> {
    match store.get_opts(path, options).await {
        Ok(got) => {
            let etag = norm_etag(got.meta.e_tag.as_deref());
            let size = got.meta.size;
            let bytes = got
                .bytes()
                .await
                .map_err(|error| err_kind(&error))?
                .to_vec();
            Ok(Got { bytes, etag, size })
        }
        Err(error) => Err(err_kind(&error)),
    }
}

async fn delete_one(
    left: &Arc<dyn ObjectStore>,
    right: &Arc<dyn ObjectStore>,
    objects: &mut BTreeMap<String, Saved>,
    label: &str,
    key: &str,
) {
    let path = Path::from(key);
    let a = unit_tag(left.delete(&path).await);
    let b = unit_tag(right.delete(&path).await);
    assert_eq!(a, b, "{label} delete");
    objects.remove(key);
}

async fn copy_one(
    left: &Arc<dyn ObjectStore>,
    right: &Arc<dyn ObjectStore>,
    objects: &mut BTreeMap<String, Saved>,
    label: &str,
    from: &str,
    to: &str,
) {
    if from == to {
        return;
    }
    let a = unit_tag(left.copy(&Path::from(from), &Path::from(to)).await);
    let b = unit_tag(right.copy(&Path::from(from), &Path::from(to)).await);
    assert_eq!(a, b, "{label} copy {from} -> {to}");
    if a.is_ok() {
        if let Some(saved) = objects.get(from) {
            let copied = Saved {
                body: saved.body.clone(),
                etag: saved.etag.clone(),
            };
            objects.insert(to.to_string(), copied);
        }
    }
}

async fn list_both(
    left: &Arc<dyn ObjectStore>,
    right: &Arc<dyn ObjectStore>,
    objects: &BTreeMap<String, Saved>,
    label: &str,
) {
    let a = list_of(left).await;
    let b = list_of(right).await;
    assert_eq!(a, b, "{label} list");
    let mut expected: Vec<(String, u64)> = objects
        .iter()
        .map(|(key, saved)| (key.clone(), saved.body.len() as u64))
        .collect();
    expected.sort();
    assert_eq!(a, expected, "{label} list model");
}

async fn list_of(store: &Arc<dyn ObjectStore>) -> Vec<(String, u64)> {
    let mut listed = Vec::new();
    let mut stream = store.list(None);
    while let Some(item) = stream.next().await {
        let meta = item.expect("list");
        listed.push((meta.location.to_string(), meta.size));
    }
    listed.sort();
    listed
}

async fn multipart_one(
    left: &Arc<dyn ObjectStore>,
    right: &Arc<dyn ObjectStore>,
    objects: &mut BTreeMap<String, Saved>,
    label: &str,
    key: &str,
    body: Vec<u8>,
) {
    let path = Path::from(key);
    let a = put_tag(one_part(left, &path, body.clone()).await);
    let b = put_tag(one_part(right, &path, body.clone()).await);
    assert_eq!(a, b, "{label} multipart");
    let etag = a.expect("multipart").expect("etag");
    objects.insert(key.to_string(), Saved { body, etag });
}

async fn one_part(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    body: Vec<u8>,
) -> Result<PutResult, Error> {
    let mut upload = store.put_multipart(path).await?;
    upload.put_part(PutPayload::from(body)).await?;
    upload.complete().await
}
