//! S3 header, token, and key helpers shared by the HTTP layer.
//!
//! A PutObject etag is the hex MD5 of the body. A completed multipart
//! upload uses the S3 form: MD5 of the concatenated raw part MD5s, a
//! hyphen, and the part count.

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use md5::{Digest, Md5};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub inclusive: bool,
    pub bound: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteRange {
    /// Inclusive HTTP end, as in `bytes=start-end`.
    Bounded {
        start: u64,
        end_inclusive: u64,
    },
    Offset(u64),
    Suffix(u64),
}

#[derive(Debug, PartialEq, Eq)]
pub struct PartSpec {
    pub number: i32,
    pub etag: String,
}

pub fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.pop() {
        if last < 0xFF {
            out.push(last + 1);
            return Some(out);
        }
    }
    None
}

pub fn encode_cursor(cursor: &Cursor) -> String {
    let mut raw = Vec::with_capacity(1 + cursor.bound.len());
    raw.push(if cursor.inclusive { 1 } else { 2 });
    raw.extend_from_slice(&cursor.bound);
    STANDARD.encode(raw)
}

pub fn decode_cursor(token: &str) -> Option<Cursor> {
    let raw = STANDARD.decode(token).ok()?;
    let (flag, bound) = raw.split_first()?;
    let inclusive = match flag {
        1 => true,
        2 => false,
        _ => return None,
    };
    Some(Cursor {
        inclusive,
        bound: bound.to_vec(),
    })
}

/// The common-prefix key for `key` under `prefix`, when `delimiter` splits
/// the remainder. `None` means `key` is an object at this level.
pub fn common_prefix<'a>(key: &'a [u8], prefix: &[u8], delimiter: &[u8]) -> Option<&'a [u8]> {
    if delimiter.is_empty() || !key.starts_with(prefix) {
        return None;
    }
    let rest = &key[prefix.len()..];
    let index = find_subslice(rest, delimiter)?;
    Some(&key[..prefix.len() + index + delimiter.len()])
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

pub fn content_etag(body: &[u8]) -> String {
    hex_md5(body)
}

/// S3's CompleteMultipartUpload etag. `digests` is the raw 16-byte MD5 of
/// each part, in the order the complete request listed them.
pub fn multipart_etag(digests: &[u8], part_count: usize) -> String {
    format!("{}-{part_count}", hex_md5(digests))
}

pub fn md5_bytes(body: &[u8]) -> [u8; 16] {
    Md5::digest(body).into()
}

fn hex_md5(body: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Md5::digest(body);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

/// Accept a quoted or weak S3 etag: 32 hex digits, or that plus `-{count}`.
pub fn parse_etag_token(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let raw = raw.strip_prefix("W/").unwrap_or(raw).trim();
    let raw = raw.trim_matches('"');
    let (hash, suffix) = match raw.split_once('-') {
        Some((hash, suffix)) => (hash, Some(suffix)),
        None => (raw, None),
    };
    if hash.len() != 32 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    if let Some(suffix) = suffix {
        if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
    }
    Some(raw.to_ascii_lowercase())
}

pub fn quote_etag(etag: &str) -> String {
    format!("\"{etag}\"")
}

pub fn parse_range_header(value: &str) -> Option<ByteRange> {
    let rest = value.trim().strip_prefix("bytes=")?;
    if rest.contains(',') {
        return None;
    }
    if let Some(suffix) = rest.strip_prefix('-') {
        if suffix.is_empty() {
            return None;
        }
        return Some(ByteRange::Suffix(suffix.parse().ok()?));
    }
    let (start, end) = rest.split_once('-')?;
    let start = start.parse().ok()?;
    if end.is_empty() {
        return Some(ByteRange::Offset(start));
    }
    let end_inclusive = end.parse().ok()?;
    if end_inclusive < start {
        return None;
    }
    Some(ByteRange::Bounded {
        start,
        end_inclusive,
    })
}

/// Byte range to return, matching `object_store`'s `GetRange::as_range`.
pub fn resolve_range(range: ByteRange, len: u64) -> Option<std::ops::Range<u64>> {
    match range {
        ByteRange::Bounded {
            start,
            end_inclusive,
        } => {
            if start >= len {
                return None;
            }
            let end = end_inclusive.saturating_add(1).min(len);
            Some(start..end)
        }
        ByteRange::Offset(start) => {
            if start >= len {
                return None;
            }
            Some(start..len)
        }
        ByteRange::Suffix(n) => Some(len.saturating_sub(n)..len),
    }
}

pub fn content_range_header(range: &std::ops::Range<u64>, len: u64) -> String {
    let end_inclusive = range.end.saturating_sub(1);
    format!("bytes {}-{end_inclusive}/{len}", range.start)
}

pub fn xml_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

pub fn xml_unescape(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn tag_text<'a>(body: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(&body[start..end])
}

pub fn parse_delete_keys(xml: &str) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    let mut rest = xml;
    while let Some(index) = rest.find("<Key>") {
        let after = &rest[index + 5..];
        let end = after
            .find("</Key>")
            .ok_or_else(|| "delete request has an unclosed Key".to_string())?;
        keys.push(xml_unescape(&after[..end]));
        rest = &after[end + 6..];
    }
    Ok(keys)
}

pub fn parse_complete_parts(xml: &str) -> Result<Vec<PartSpec>, String> {
    let mut parts = Vec::new();
    let mut rest = xml;
    while let Some(index) = rest.find("<Part>") {
        let after = &rest[index + 6..];
        let end = after
            .find("</Part>")
            .ok_or_else(|| "complete request has an unclosed Part".to_string())?;
        let body = &after[..end];
        let number: i32 = tag_text(body, "PartNumber")
            .ok_or_else(|| "part is missing PartNumber".to_string())?
            .trim()
            .parse()
            .map_err(|_| "part number is not an integer".to_string())?;
        if number < 1 {
            return Err("part number must be at least 1".to_string());
        }
        let etag_raw = tag_text(body, "ETag").ok_or_else(|| "part is missing ETag".to_string())?;
        let etag = parse_etag_token(&xml_unescape(etag_raw))
            .ok_or_else(|| format!("part {number} has an etag this store did not issue"))?;
        parts.push(PartSpec { number, etag });
        rest = &after[end + 7..];
    }
    Ok(parts)
}

pub fn like_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        None
    } else {
        successor(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successor_moves_the_last_byte() {
        assert_eq!(successor(b"cells/").as_deref(), Some(&b"cells0"[..]));
        assert_eq!(successor(b"\xff\xff"), None);
        assert_eq!(successor(b"a\xff").as_deref(), Some(&b"b"[..]));
    }

    #[test]
    fn cursor_round_trip_keeps_the_bound() {
        let cursor = Cursor {
            inclusive: false,
            bound: b"cells/b/".to_vec(),
        };
        let decoded = decode_cursor(&encode_cursor(&cursor)).unwrap();
        assert_eq!(decoded, cursor);
    }

    #[test]
    fn common_prefix_stops_at_the_delimiter() {
        assert_eq!(
            common_prefix(b"cells/ltx/e1", b"cells/", b"/"),
            Some(&b"cells/ltx/"[..])
        );
        assert_eq!(common_prefix(b"cells/own.json", b"cells/", b"/"), None);
    }

    #[test]
    fn etag_token_accepts_quoted_and_weak_forms() {
        let md5 = "d41d8cd98f00b204e9800998ecf8427e";
        assert_eq!(
            parse_etag_token(&format!("\"{md5}\"")).as_deref(),
            Some(md5)
        );
        assert_eq!(
            parse_etag_token("W/\"D41D8CD98F00B204E9800998ECF8427E-2\"").as_deref(),
            Some("d41d8cd98f00b204e9800998ecf8427e-2")
        );
        assert_eq!(parse_etag_token("nope"), None);
        assert_eq!(parse_etag_token("\"42\""), None);
        assert_eq!(quote_etag(md5), format!("\"{md5}\""));
    }

    #[test]
    fn etags_match_s3_md5() {
        assert_eq!(content_etag(b""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(content_etag(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
        let digest = md5_bytes(b"hello-part");
        assert_eq!(
            multipart_etag(&digest, 1),
            "6c5309db5cc8ded5f29bea6b7dca6a08-1"
        );
    }

    #[test]
    fn range_matches_object_store_clamping() {
        let range = parse_range_header("bytes=6-10").unwrap();
        assert_eq!(resolve_range(range, 16), Some(6..11));
        let past = parse_range_header("bytes=8-30").unwrap();
        assert_eq!(resolve_range(past, 10), Some(8..10));
        let suffix = parse_range_header("bytes=-4").unwrap();
        assert_eq!(resolve_range(suffix, 10), Some(6..10));
        assert_eq!(content_range_header(&(6..11), 16), "bytes 6-10/16");
    }

    #[test]
    fn xml_round_trip_and_part_parse() {
        assert_eq!(xml_unescape(&xml_escape("a<b>&\"c\"")), "a<b>&\"c\"");
        let xml = r#"<CompleteMultipartUpload><Part><ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>"#;
        let parts = parse_complete_parts(xml).unwrap();
        assert_eq!(
            parts,
            vec![PartSpec {
                number: 2,
                etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
            }]
        );
        let deleted =
            parse_delete_keys("<Delete><Object><Key>a&amp;b</Key></Object></Delete>").unwrap();
        assert_eq!(deleted, vec!["a&b".to_string()]);
    }
}
