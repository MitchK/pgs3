//! Path-style S3 (`/{bucket}/{key}`): If-Match / If-None-Match, ranged
//! GET, ListObjectsV2, DeleteObjects, and multipart upload. Signatures are
//! not checked. The listener is the trust boundary.

use crate::s3util::{
    content_range_header, decode_cursor, encode_cursor, parse_complete_parts, parse_delete_keys,
    parse_etag_token, parse_range_header, quote_etag, resolve_range, xml_escape, Cursor,
};
use crate::store::{
    Condition, ListEntry, ObjectAttrs, ObjectMeta, PartInfo, Store, StoreError, UploadInfo,
};
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Request, State};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH,
    CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MATCH, IF_NONE_MATCH,
};
use http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode};
use percent_encoding::percent_decode_str;
use std::collections::HashMap;

#[derive(Clone)]
pub struct App {
    pub store: Store,
    pub max_object_bytes: usize,
}

pub fn router(store: Store, max_object_bytes: usize) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/:bucket", get(bucket_get).post(bucket_post))
        .route(
            "/:bucket/*key",
            get(object_get)
                .head(object_head)
                .put(object_put)
                .delete(object_delete)
                .post(object_post),
        )
        .layer(DefaultBodyLimit::max(max_object_bytes))
        .with_state(App {
            store,
            max_object_bytes,
        })
}

async fn health(State(app): State<App>) -> Response<Body> {
    match app.store.ping().await {
        Ok(()) => text(StatusCode::OK, "ok"),
        Err(error) => {
            eprintln!("pgs3 health check failed: {error}");
            text(StatusCode::SERVICE_UNAVAILABLE, "database unavailable")
        }
    }
}

async fn bucket_get(
    State(app): State<App>,
    Path(bucket): Path<String>,
    request: Request,
) -> Response<Body> {
    let query = query_map(request.uri());
    if let Err(response) = check_bucket(&bucket) {
        return response;
    }
    if query.contains_key("uploads") {
        let prefix = query.get("prefix").map(String::as_str).unwrap_or("");
        return match app.store.list_uploads(&bucket, prefix).await {
            Ok(uploads) => list_uploads_response(&bucket, prefix, &uploads),
            Err(error) => store_error(error),
        };
    }
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let delimiter = query.get("delimiter").cloned();
    let max_keys = match query.get("max-keys") {
        Some(value) => match value.parse::<usize>() {
            Ok(value) => value.min(1000),
            Err(_) => {
                return s3_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "max-keys is not an integer",
                )
            }
        },
        None => 1000,
    };
    let version2 = query.get("list-type").map(String::as_str) == Some("2");
    let (cursor, hide_prefixes_le) = if version2 {
        let cursor = if let Some(token) = query.get("continuation-token") {
            match decode_cursor(token) {
                Some(cursor) => cursor,
                None => {
                    return s3_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "continuation token is not valid",
                    )
                }
            }
        } else if let Some(start_after) = query.get("start-after") {
            Cursor {
                inclusive: false,
                bound: start_after.as_bytes().to_vec(),
            }
        } else {
            Cursor {
                inclusive: true,
                bound: prefix.as_bytes().to_vec(),
            }
        };
        (cursor, None)
    } else if let Some(marker) = query.get("marker").filter(|marker| !marker.is_empty()) {
        let mut bound = marker.as_bytes().to_vec();
        bound.push(0);
        (
            Cursor {
                inclusive: true,
                bound,
            },
            Some(marker.clone()),
        )
    } else {
        (
            Cursor {
                inclusive: true,
                bound: prefix.as_bytes().to_vec(),
            },
            None,
        )
    };
    match app
        .store
        .list(
            &bucket,
            &prefix,
            delimiter.as_deref(),
            cursor,
            max_keys,
            hide_prefixes_le.as_deref().map(str::as_bytes),
        )
        .await
    {
        Ok(page) => list_response(
            &bucket,
            &prefix,
            delimiter.as_deref(),
            max_keys,
            version2,
            query.get("continuation-token").map(String::as_str),
            query.get("start-after").map(String::as_str),
            query.get("marker").map(String::as_str),
            &page.entries,
            page.next.is_some(),
        ),
        Err(error) => store_error(error),
    }
}

async fn bucket_post(
    State(app): State<App>,
    Path(bucket): Path<String>,
    request: Request,
) -> Response<Body> {
    let query = query_map(request.uri());
    if !query.contains_key("delete") {
        return s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "the only bucket POST is DeleteObjects",
        );
    }
    if let Err(response) = check_bucket(&bucket) {
        return response;
    }
    let bytes = axum::body::to_bytes(request.into_body(), app.max_object_bytes)
        .await
        .unwrap_or_default();
    let xml = String::from_utf8_lossy(&bytes);
    let keys = match parse_delete_keys(&xml) {
        Ok(keys) => keys,
        Err(message) => return s3_error(StatusCode::BAD_REQUEST, "MalformedXML", &message),
    };
    let mut deleted = String::new();
    for key in keys {
        if let Err(error) = app.store.delete(&bucket, &key).await {
            return store_error(error);
        }
        deleted.push_str("<Deleted><Key>");
        deleted.push_str(&xml_escape(&key));
        deleted.push_str("</Key></Deleted>");
    }
    xml_response(
        StatusCode::OK,
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult>{deleted}</DeleteResult>"),
    )
}

async fn object_get(
    State(app): State<App>,
    Path((bucket, key)): Path<(String, String)>,
    request: Request,
) -> Response<Body> {
    let query = query_map(request.uri());
    if let Some(upload_id) = query.get("uploadId") {
        if let Err(response) = check_names(&bucket, &key) {
            return response;
        }
        return match app.store.list_parts(&bucket, upload_id).await {
            Ok((stored_key, parts)) => {
                if stored_key != key.as_bytes() {
                    return s3_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidRequest",
                        "upload id belongs to a different key",
                    );
                }
                list_parts_response(&bucket, &key, upload_id, &parts)
            }
            Err(error) => store_error(error),
        };
    }
    read_object(&app, &bucket, &key, request.headers(), false).await
}

async fn object_head(
    State(app): State<App>,
    Path((bucket, key)): Path<(String, String)>,
    request: Request,
) -> Response<Body> {
    read_object(&app, &bucket, &key, request.headers(), true).await
}

async fn read_object(
    app: &App,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    head: bool,
) -> Response<Body> {
    if let Err(response) = check_names(bucket, key) {
        return response;
    }
    let ranged = match headers.get(http::header::RANGE) {
        Some(value) => match value.to_str().ok().and_then(parse_range_header) {
            Some(range) => Some(range),
            None => {
                return s3_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidRange",
                    "the Range header is not a single byte range",
                )
            }
        },
        None => None,
    };
    let loaded = match ranged {
        None => app.store.get(bucket, key).await.map(|object| {
            let len = object.meta.size as u64;
            (object.meta, object.body, None, 0..len)
        }),
        Some(range) => match app.store.head(bucket, key).await {
            Ok(meta) => {
                let Some(resolved) = resolve_range(range, meta.size.max(0) as u64) else {
                    return s3_error(
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        "InvalidRange",
                        "the range starts past the end of the object",
                    );
                };
                match app.store.get_range(bucket, key, resolved.clone()).await {
                    Ok(object) => Ok((
                        object.meta,
                        object.body,
                        Some(content_range_header(&resolved, meta.size.max(0) as u64)),
                        resolved,
                    )),
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        },
    };
    match loaded {
        Ok((meta, body, content_range, _resolved)) => {
            match read_precondition(headers, &meta.etag) {
                ReadPrecondition::NotModified => not_modified(&meta.etag),
                ReadPrecondition::Failed => s3_error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "the conditional read was rejected",
                ),
                ReadPrecondition::Continue => object_response(meta, body, content_range, head),
            }
        }
        Err(error) => store_error(error),
    }
}

enum ReadPrecondition {
    Continue,
    NotModified,
    Failed,
}

fn read_precondition(headers: &HeaderMap, etag: &str) -> ReadPrecondition {
    let if_match = headers.get(IF_MATCH).and_then(|value| value.to_str().ok());
    let if_none = headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok());
    if if_match.is_some() && if_none.is_some() {
        return ReadPrecondition::Failed;
    }
    if let Some(value) = if_none {
        let matched = value.trim() == "*" || etag_matches(value, etag);
        return if matched {
            ReadPrecondition::NotModified
        } else {
            ReadPrecondition::Continue
        };
    }
    if let Some(value) = if_match {
        if value.trim() == "*" || etag_matches(value, etag) {
            ReadPrecondition::Continue
        } else {
            ReadPrecondition::Failed
        }
    } else {
        ReadPrecondition::Continue
    }
}

fn etag_matches(header: &str, etag: &str) -> bool {
    header
        .split(',')
        .any(|token| parse_etag_token(token).as_deref() == Some(etag))
}

fn not_modified(etag: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header(ETAG, quote_etag(etag))
        .body(Body::empty())
        .unwrap()
}

async fn object_put(
    State(app): State<App>,
    Path((bucket, key)): Path<(String, String)>,
    request: Request,
) -> Response<Body> {
    let query = query_map(request.uri());
    if query.contains_key("partNumber") || query.contains_key("uploadId") {
        return put_part(app, bucket, key, query, request).await;
    }
    if let Err(response) = check_names(&bucket, &key) {
        return response;
    }
    let (parts, body) = request.into_parts();
    let condition = match condition_from_headers(&parts.headers) {
        Ok(condition) => condition,
        Err(error) => return store_error(error),
    };
    let attrs = match attrs_from_headers(&parts.headers) {
        Ok(attrs) => attrs,
        Err(error) => return store_error(error),
    };
    if let Some(source) = parts.headers.get("x-amz-copy-source") {
        let source = match source.to_str() {
            Ok(source) => source,
            Err(_) => {
                return s3_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "copy source is not utf-8",
                )
            }
        };
        let (src_bucket, src_key) = match parse_copy_source(source) {
            Ok(parsed) => parsed,
            Err(error) => return store_error(error),
        };
        if src_bucket == bucket && src_key == key {
            return s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "the copy source and destination are the same object",
            );
        }
        return match app
            .store
            .copy(&src_bucket, &src_key, &bucket, &key, condition)
            .await
        {
            Ok(meta) => xml_response(
                StatusCode::OK,
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyObjectResult><ETag>{}</ETag></CopyObjectResult>",
                    xml_escape(&quote_etag(&meta.etag))
                ),
            ),
            Err(error) => store_error(error),
        };
    }
    let bytes = match axum::body::to_bytes(body, app.max_object_bytes).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return s3_error(
                StatusCode::BAD_REQUEST,
                "EntityTooLarge",
                "the object exceeds the configured maximum",
            )
        }
    };
    match app
        .store
        .put(&bucket, &key, &bytes, &attrs, condition)
        .await
    {
        Ok(meta) => put_response(&meta),
        Err(error) => store_error(error),
    }
}

async fn put_part(
    app: App,
    bucket: String,
    key: String,
    query: HashMap<String, String>,
    request: Request,
) -> Response<Body> {
    if let Err(response) = check_names(&bucket, &key) {
        return response;
    }
    let Some(upload_id) = query.get("uploadId").cloned() else {
        return s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "uploadId is required",
        );
    };
    let part_number = match query.get("partNumber").map(String::as_str) {
        Some(value) => match value.parse::<i32>() {
            Ok(value) if (1..=10_000).contains(&value) => value,
            _ => {
                return s3_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "partNumber must be between 1 and 10000",
                )
            }
        },
        None => {
            return s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "partNumber is required",
            )
        }
    };
    let bytes = match axum::body::to_bytes(request.into_body(), app.max_object_bytes).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return s3_error(
                StatusCode::BAD_REQUEST,
                "EntityTooLarge",
                "the part exceeds the configured maximum",
            )
        }
    };
    match app
        .store
        .put_part(&bucket, &upload_id, part_number, &bytes)
        .await
    {
        Ok(etag) => response_with_etag(&etag),
        Err(error) => store_error(error),
    }
}

async fn object_delete(
    State(app): State<App>,
    Path((bucket, key)): Path<(String, String)>,
    request: Request,
) -> Response<Body> {
    let query = query_map(request.uri());
    if let Some(upload_id) = query.get("uploadId") {
        if let Err(response) = check_names(&bucket, &key) {
            return response;
        }
        return match app.store.abort_upload(&bucket, upload_id).await {
            Ok(()) => empty(StatusCode::NO_CONTENT),
            Err(error) => store_error(error),
        };
    }
    if let Err(response) = check_names(&bucket, &key) {
        return response;
    }
    match app.store.delete(&bucket, &key).await {
        Ok(()) => empty(StatusCode::NO_CONTENT),
        Err(error) => store_error(error),
    }
}

async fn object_post(
    State(app): State<App>,
    Path((bucket, key)): Path<(String, String)>,
    request: Request,
) -> Response<Body> {
    let query = query_map(request.uri());
    if let Err(response) = check_names(&bucket, &key) {
        return response;
    }
    if query.contains_key("uploads") {
        let attrs = match attrs_from_headers(request.headers()) {
            Ok(attrs) => attrs,
            Err(error) => return store_error(error),
        };
        return match app.store.create_upload(&bucket, &key, &attrs).await {
            Ok(upload_id) => xml_response(
                StatusCode::OK,
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
                    xml_escape(&bucket),
                    xml_escape(&key),
                    xml_escape(&upload_id)
                ),
            ),
            Err(error) => store_error(error),
        };
    }
    let Some(upload_id) = query.get("uploadId").cloned() else {
        return s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "uploadId or uploads is required",
        );
    };
    let (parts, body) = request.into_parts();
    let condition = match condition_from_headers(&parts.headers) {
        Ok(condition) => condition,
        Err(error) => return store_error(error),
    };
    let bytes = match axum::body::to_bytes(body, app.max_object_bytes).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return s3_error(
                StatusCode::BAD_REQUEST,
                "EntityTooLarge",
                "the complete request is too large",
            )
        }
    };
    let xml = String::from_utf8_lossy(&bytes);
    let specs = match parse_complete_parts(&xml) {
        Ok(specs) => specs,
        Err(message) => return s3_error(StatusCode::BAD_REQUEST, "MalformedXML", &message),
    };
    let pairs: Vec<(i32, String)> = specs
        .into_iter()
        .map(|part| (part.number, part.etag))
        .collect();
    match app
        .store
        .complete_upload(
            &bucket,
            &key,
            &upload_id,
            &pairs,
            condition,
            app.max_object_bytes,
        )
        .await
    {
        Ok(meta) => xml_response(
            StatusCode::OK,
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
                xml_escape(&bucket),
                xml_escape(&key),
                xml_escape(&quote_etag(&meta.etag))
            ),
        ),
        Err(StoreError::Precondition) => s3_error(
            StatusCode::PRECONDITION_FAILED,
            "PreconditionFailed",
            "the conditional write was rejected",
        ),
        Err(error) => store_error(error),
    }
}

fn list_response(
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
    version2: bool,
    continuation_token: Option<&str>,
    start_after: Option<&str>,
    marker: Option<&str>,
    entries: &[ListEntry],
    truncated: bool,
) -> Response<Body> {
    let mut body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>{}</Name><Prefix>{}</Prefix>",
        xml_escape(bucket),
        xml_escape(prefix)
    );
    if version2 {
        body.push_str(&format!("<KeyCount>{}</KeyCount>", entries.len()));
    } else {
        body.push_str("<Marker>");
        body.push_str(&xml_escape(marker.unwrap_or("")));
        body.push_str("</Marker>");
    }
    body.push_str(&format!(
        "<MaxKeys>{max_keys}</MaxKeys><IsTruncated>{}</IsTruncated>",
        if truncated { "true" } else { "false" }
    ));
    if version2 {
        if let Some(token) = continuation_token {
            body.push_str("<ContinuationToken>");
            body.push_str(&xml_escape(token));
            body.push_str("</ContinuationToken>");
        }
        if let Some(start_after) = start_after {
            body.push_str("<StartAfter>");
            body.push_str(&xml_escape(start_after));
            body.push_str("</StartAfter>");
        }
    }
    for entry in entries {
        match entry {
            ListEntry::Object(object) => {
                let key = String::from_utf8_lossy(&object.key);
                body.push_str("<Contents><Key>");
                body.push_str(&xml_escape(&key));
                body.push_str("</Key><LastModified>");
                body.push_str(&list_time(object.modified_at));
                body.push_str("</LastModified><ETag>");
                body.push_str(&xml_escape(&quote_etag(&object.etag)));
                body.push_str("</ETag><Size>");
                body.push_str(&object.size.to_string());
                body.push_str("</Size><StorageClass>STANDARD</StorageClass></Contents>");
            }
            ListEntry::Prefix(prefix) => {
                let prefix = String::from_utf8_lossy(prefix);
                body.push_str("<CommonPrefixes><Prefix>");
                body.push_str(&xml_escape(&prefix));
                body.push_str("</Prefix></CommonPrefixes>");
            }
        }
    }
    if let Some(delimiter) = delimiter.filter(|delimiter| !delimiter.is_empty()) {
        body.push_str("<Delimiter>");
        body.push_str(&xml_escape(delimiter));
        body.push_str("</Delimiter>");
    }
    if truncated {
        if version2 {
            if let Some(cursor) = next_cursor(entries) {
                body.push_str("<NextContinuationToken>");
                body.push_str(&xml_escape(&encode_cursor(&cursor)));
                body.push_str("</NextContinuationToken>");
            }
        } else if let Some(marker) = entries.last().map(entry_marker) {
            body.push_str("<NextMarker>");
            body.push_str(&xml_escape(&marker));
            body.push_str("</NextMarker>");
        }
    }
    body.push_str("</ListBucketResult>");
    xml_response(StatusCode::OK, body)
}

/// The v2 token has to resume where this page stopped, including a common
/// prefix whose members were not listed one by one. The page's `next`
/// cursor already points there; recompute it from the last entry so the
/// response does not depend on the store handing the cursor back.
fn next_cursor(entries: &[ListEntry]) -> Option<Cursor> {
    let last = entries.last()?;
    match last {
        ListEntry::Object(object) => Some(Cursor {
            inclusive: false,
            bound: object.key.clone(),
        }),
        ListEntry::Prefix(prefix) => crate::s3util::successor(prefix).map(|bound| Cursor {
            inclusive: true,
            bound,
        }),
    }
}

fn entry_marker(entry: &ListEntry) -> String {
    match entry {
        ListEntry::Object(object) => String::from_utf8_lossy(&object.key).into_owned(),
        ListEntry::Prefix(prefix) => String::from_utf8_lossy(prefix).into_owned(),
    }
}

fn list_uploads_response(bucket: &str, prefix: &str, uploads: &[UploadInfo]) -> Response<Body> {
    let mut body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListMultipartUploadsResult><Bucket>{}</Bucket><Prefix>{}</Prefix><MaxUploads>1000</MaxUploads><IsTruncated>false</IsTruncated>",
        xml_escape(bucket),
        xml_escape(prefix)
    );
    for upload in uploads {
        let key = String::from_utf8_lossy(&upload.key);
        body.push_str("<Upload><Key>");
        body.push_str(&xml_escape(&key));
        body.push_str("</Key><UploadId>");
        body.push_str(&xml_escape(&upload.upload_id));
        body.push_str("</UploadId><Initiated>");
        body.push_str(&list_time(upload.created_at));
        body.push_str("</Initiated></Upload>");
    }
    body.push_str("</ListMultipartUploadsResult>");
    xml_response(StatusCode::OK, body)
}

fn list_parts_response(
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[PartInfo],
) -> Response<Body> {
    let mut body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListPartsResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId><MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated><PartNumberMarker>0</PartNumberMarker>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id)
    );
    for part in parts {
        body.push_str("<Part><PartNumber>");
        body.push_str(&part.number.to_string());
        body.push_str("</PartNumber><ETag>");
        body.push_str(&xml_escape(&quote_etag(&part.etag)));
        body.push_str("</ETag><Size>");
        body.push_str(&part.size.to_string());
        body.push_str("</Size></Part>");
    }
    body.push_str("</ListPartsResult>");
    xml_response(StatusCode::OK, body)
}

fn object_response(
    meta: ObjectMeta,
    body: Vec<u8>,
    content_range: Option<String>,
    head: bool,
) -> Response<Body> {
    let status = if content_range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    // A body and an explicit Content-Length together make hyper write the
    // header twice, and the next request on that connection fails. GET lets
    // the body supply the length. HEAD has no body, so the length is a header
    // and hyper allows that only for HEAD.
    let mut builder = Response::builder()
        .status(status)
        .header(ETAG, quote_etag(&meta.etag))
        .header(http::header::LAST_MODIFIED, http_date(meta.modified_at));
    if let Some(content_range) = content_range {
        builder = builder.header(CONTENT_RANGE, content_range);
    }
    builder = apply_attrs(builder, &meta);
    if head {
        builder = builder.header(CONTENT_LENGTH, body.len());
        builder.body(Body::empty())
    } else {
        builder.body(Body::from(Bytes::from(body)))
    }
    .unwrap_or_else(|_| text(StatusCode::INTERNAL_SERVER_ERROR, "response"))
}

fn put_response(meta: &ObjectMeta) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(ETAG, quote_etag(&meta.etag))
        .body(Body::empty())
        .unwrap()
}

fn response_with_etag(etag: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(ETAG, quote_etag(etag))
        .body(Body::empty())
        .unwrap()
}

fn apply_attrs(mut builder: http::response::Builder, meta: &ObjectMeta) -> http::response::Builder {
    let content_type = meta
        .attrs
        .content_type
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("application/octet-stream");
    builder = optional_header(builder, CONTENT_TYPE, Some(content_type));
    builder = optional_header(builder, CACHE_CONTROL, meta.attrs.cache_control.as_deref());
    builder = optional_header(
        builder,
        CONTENT_DISPOSITION,
        meta.attrs.content_disposition.as_deref(),
    );
    builder = optional_header(
        builder,
        CONTENT_ENCODING,
        meta.attrs.content_encoding.as_deref(),
    );
    builder = optional_header(
        builder,
        CONTENT_LANGUAGE,
        meta.attrs.content_language.as_deref(),
    );
    builder = optional_header(
        builder,
        HeaderName::from_static("x-amz-storage-class"),
        meta.attrs.storage_class.as_deref(),
    );
    for (name, value) in &meta.attrs.metadata {
        let Some(value) = value.as_str() else {
            continue;
        };
        let Ok(header) = HeaderName::try_from(format!("x-amz-meta-{name}")) else {
            continue;
        };
        builder = optional_header(builder, header, Some(value));
    }
    builder
}

fn optional_header(
    builder: http::response::Builder,
    name: HeaderName,
    value: Option<&str>,
) -> http::response::Builder {
    match value.and_then(|value| HeaderValue::from_str(value).ok()) {
        Some(value) => builder.header(name, value),
        None => builder,
    }
}

fn condition_from_headers(headers: &HeaderMap) -> Result<Condition, StoreError> {
    let if_match = header_text(headers, IF_MATCH)?;
    let if_none = header_text(headers, IF_NONE_MATCH)?;
    match (if_match, if_none) {
        (None, None) => Ok(Condition::Overwrite),
        (None, Some(value)) if value.trim() == "*" => Ok(Condition::Create),
        (None, Some(value)) => match parse_etag_token(&value) {
            // A token this store did not issue cannot be the current fence,
            // so the write is allowed.
            Some(etag) => Ok(Condition::IfNoneMatch(etag)),
            None => Ok(Condition::Overwrite),
        },
        (Some(value), None) => Ok(Condition::IfMatch(
            parse_etag_token(&value).unwrap_or_default(),
        )),
        (Some(_), Some(_)) => Err(StoreError::Precondition),
    }
}

fn attrs_from_headers(headers: &HeaderMap) -> Result<ObjectAttrs, StoreError> {
    let mut attrs = ObjectAttrs::default();
    attrs.content_type = header_text(headers, CONTENT_TYPE)?;
    attrs.cache_control = header_text(headers, CACHE_CONTROL)?;
    attrs.content_disposition = header_text(headers, CONTENT_DISPOSITION)?;
    attrs.content_encoding = header_text(headers, CONTENT_ENCODING)?;
    attrs.content_language = header_text(headers, CONTENT_LANGUAGE)?;
    attrs.storage_class = header_text(headers, HeaderName::from_static("x-amz-storage-class"))?;
    for (name, value) in headers.iter() {
        let Some(suffix) = name.as_str().strip_prefix("x-amz-meta-") else {
            continue;
        };
        if suffix.is_empty() {
            continue;
        }
        let value = value
            .to_str()
            .map_err(|_| StoreError::Message(format!("metadata {suffix} is not utf-8")))?;
        attrs.metadata.insert(
            suffix.to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }
    Ok(attrs)
}

fn header_text(headers: &HeaderMap, name: HeaderName) -> Result<Option<String>, StoreError> {
    match headers.get(name) {
        None => Ok(None),
        Some(value) => value
            .to_str()
            .map(|value| Some(value.to_string()))
            .map_err(|_| StoreError::Message("a request header is not utf-8".into())),
    }
}

fn parse_copy_source(value: &str) -> Result<(String, String), StoreError> {
    let value = value.trim().trim_start_matches('/');
    let value = value.split('?').next().unwrap_or(value);
    let (bucket, key) = value
        .split_once('/')
        .ok_or_else(|| StoreError::Message("copy source must be bucket/key".into()))?;
    let key = percent_decode_str(key)
        .decode_utf8()
        .map_err(|_| StoreError::Message("copy source key is not utf-8".into()))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(StoreError::Message(
            "copy source is missing a bucket or a key".into(),
        ));
    }
    Ok((bucket.to_string(), key.to_string()))
}

fn query_map(uri: &http::Uri) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let Some(query) = uri.query() else {
        return map;
    };
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        map.insert(decode_query(key), decode_query(value));
    }
    map
}

fn decode_query(value: &str) -> String {
    let spaced = value.replace('+', " ");
    percent_decode_str(&spaced).decode_utf8_lossy().into_owned()
}

fn check_names(bucket: &str, key: &str) -> Result<(), Response<Body>> {
    check_bucket(bucket)?;
    if key.is_empty() || key.len() > 1024 {
        return Err(s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "the key is empty or longer than 1024 bytes",
        ));
    }
    Ok(())
}

fn check_bucket(bucket: &str) -> Result<(), Response<Body>> {
    if bucket.is_empty()
        || bucket.len() > 255
        || bucket.contains('/')
        || bucket == "."
        || bucket == ".."
    {
        return Err(s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidBucketName",
            "the bucket name is not usable",
        ));
    }
    Ok(())
}

fn store_error(failure: StoreError) -> Response<Body> {
    match failure {
        StoreError::Precondition => s3_error(
            StatusCode::PRECONDITION_FAILED,
            "PreconditionFailed",
            "the conditional write was rejected",
        ),
        StoreError::NotFound => s3_error(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "the object does not exist",
        ),
        StoreError::NoSuchUpload => s3_error(
            StatusCode::NOT_FOUND,
            "NoSuchUpload",
            "the multipart upload does not exist",
        ),
        StoreError::Message(message) => {
            s3_error(StatusCode::BAD_REQUEST, "InvalidRequest", &message)
        }
        StoreError::Postgres(failure) => {
            eprintln!("pgs3 database error: {failure}");
            s3_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "DatabaseError",
                "the database request failed",
            )
        }
    }
}

fn s3_error(status: StatusCode, code: &str, message: &str) -> Response<Body> {
    xml_response(
        status,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{}</Code><Message>{}</Message></Error>",
            xml_escape(code),
            xml_escape(message)
        ),
    )
}

fn xml_response(status: StatusCode, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/xml")
        .body(Body::from(body))
        .unwrap()
}

fn text(status: StatusCode, body: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::from(body))
        .unwrap()
}

fn empty(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::empty())
        .unwrap()
}

fn list_time(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn http_date(time: DateTime<Utc>) -> String {
    let millis = time.timestamp_millis().max(0) as u64;
    let system = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(millis);
    httpdate::fmt_http_date(system)
}
