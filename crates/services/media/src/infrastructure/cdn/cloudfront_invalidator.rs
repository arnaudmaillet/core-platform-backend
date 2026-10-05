//! CloudFront `CreateInvalidation` (API 2020-05-31), SigV4-signed with static
//! keys: the takedown path purges a quarantined / deleted asset's renditions
//! from the edge (public URLs are immutable and cached for a long time).

use chrono::Utc;
use uuid::Uuid;

use super::sigv4::{authorization, Credentials, SignableRequest};
use crate::domain::value_object::StorageKey;
use crate::error::MediaError;

/// CloudFront accepts at most this many paths per invalidation.
const MAX_PATHS: usize = 3_000;
const HOST: &str = "cloudfront.amazonaws.com";
/// CloudFront is a global service, signed in us-east-1.
const REGION: &str = "us-east-1";

#[derive(Debug, Clone)]
pub struct CloudFrontInvalidatorConfig {
    pub distribution_id:   String,
    pub access_key_id:     String,
    pub secret_access_key: String,
    /// `https://cloudfront.amazonaws.com` (overridable for tests).
    pub endpoint:          String,
}

pub struct CloudFrontInvalidator {
    http:   reqwest::Client,
    config: CloudFrontInvalidatorConfig,
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&apos;")
}

/// The `InvalidationBatch` XML for `keys` (paths are `/` + the storage key).
fn batch_xml(keys: &[StorageKey], caller_reference: &str) -> String {
    let items: String = keys.iter().map(|k| format!("<Path>/{}</Path>", xml_escape(k.as_str()))).collect();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <InvalidationBatch xmlns=\"http://cloudfront.amazonaws.com/doc/2020-05-31/\">\
         <Paths><Quantity>{}</Quantity><Items>{items}</Items></Paths>\
         <CallerReference>{}</CallerReference></InvalidationBatch>",
        keys.len(),
        xml_escape(caller_reference),
    )
}

impl CloudFrontInvalidator {
    pub fn new(http: reqwest::Client, config: CloudFrontInvalidatorConfig) -> Self {
        Self { http, config }
    }

    /// Invalidates `keys` (in batches of [`MAX_PATHS`]). Any failure is
    /// [`MediaError::CdnInvalidationFailed`] (retryable: the takedown is retried).
    pub async fn invalidate(&self, keys: &[StorageKey]) -> Result<(), MediaError> {
        for batch in keys.chunks(MAX_PATHS) {
            self.send(batch).await?;
        }
        Ok(())
    }

    async fn send(&self, keys: &[StorageKey]) -> Result<(), MediaError> {
        let path = format!("/2020-05-31/distribution/{}/invalidation", self.config.distribution_id);
        let body = batch_xml(keys, &Uuid::now_v7().to_string());
        let content_type = "application/xml";
        let amz_date = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let auth = authorization(
            &SignableRequest {
                method: "POST",
                host: HOST,
                path: &path,
                query: "",
                content_type,
                body: body.as_bytes(),
                amz_date: &amz_date,
                region: REGION,
                service: "cloudfront",
            },
            &Credentials {
                access_key_id: &self.config.access_key_id,
                secret_access_key: &self.config.secret_access_key,
            },
        );
        let response = self
            .http
            .post(format!("{}{path}", self.config.endpoint.trim_end_matches('/')))
            .header("host", HOST)
            .header("content-type", content_type)
            .header("x-amz-date", &amz_date)
            .header("authorization", auth)
            .body(body)
            .send()
            .await
            .map_err(|e| MediaError::CdnInvalidationFailed { reason: format!("CloudFront unreachable: {e}") })?;
        let status = response.status();
        if !status.is_success() {
            let detail: String = response.text().await.unwrap_or_default().chars().take(300).collect();
            return Err(MediaError::CdnInvalidationFailed { reason: format!("CloudFront answered {status}: {detail}") });
        }
        tracing::info!(paths = keys.len(), "cdn invalidation created");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    fn key(s: &str) -> StorageKey {
        StorageKey::from_raw(s)
    }

    #[test]
    fn the_batch_lists_every_path_escaped() {
        let xml = batch_xml(&[key("ab/cd/1.webp"), key("ab/cd/2.webp")], "ref-1");
        assert!(xml.contains("<Quantity>2</Quantity>"));
        assert!(xml.contains("<Path>/ab/cd/1.webp</Path><Path>/ab/cd/2.webp</Path>"));
        assert!(xml.contains("<CallerReference>ref-1</CallerReference>"));
        assert_eq!(xml_escape("a&b<c>"), "a&amp;b&lt;c&gt;");
    }

    /// A one-route server recording the request line, headers and body.
    async fn cloudfront(status: u16) -> (String, Arc<Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let seen = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let log = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = vec![0u8; 16 * 1024];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                log.lock().unwrap().push(String::from_utf8_lossy(&buf[..n]).into_owned());
                let reason = if status == 201 { "Created" } else { "Forbidden" };
                let _ = socket
                    .write_all(format!("HTTP/1.1 {status} {reason}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").as_bytes())
                    .await;
            }
        });
        (format!("http://{addr}"), seen)
    }

    fn invalidator(endpoint: String) -> CloudFrontInvalidator {
        CloudFrontInvalidator::new(reqwest::Client::new(), CloudFrontInvalidatorConfig {
            distribution_id: "E2EXAMPLE".into(),
            access_key_id: "AKID".into(),
            secret_access_key: "secret".into(),
            endpoint,
        })
    }

    #[tokio::test]
    async fn a_takedown_posts_a_signed_invalidation_for_the_distribution() {
        let (endpoint, seen) = cloudfront(201).await;
        invalidator(endpoint).invalidate(&[key("ab/cd/1.webp")]).await.unwrap();
        let request = seen.lock().unwrap().join("");
        assert!(request.starts_with("POST /2020-05-31/distribution/E2EXAMPLE/invalidation HTTP/1.1"));
        assert!(request.contains("authorization: AWS4-HMAC-SHA256 Credential=AKID/"));
        assert!(request.contains("/us-east-1/cloudfront/aws4_request"));
        assert!(request.contains("<Path>/ab/cd/1.webp</Path>"));
    }

    #[tokio::test]
    async fn a_refusal_fails_the_takedown_so_it_is_retried() {
        let (endpoint, _) = cloudfront(403).await;
        let err = invalidator(endpoint).invalidate(&[key("ab/cd/1.webp")]).await.unwrap_err();
        assert!(matches!(err, MediaError::CdnInvalidationFailed { .. }));
    }
}
