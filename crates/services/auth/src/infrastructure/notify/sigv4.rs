//! AWS Signature Version 4 for the one call auth makes to AWS (SNS `Publish`),
//! with static credentials — the fleet's AWS clients use static keys, not IRSA.
//! HMAC-SHA256 is built on `sha2` directly (RFC 2104).

use sha2::{Digest, Sha256};

const BLOCK: usize = 64;

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.iter().map(|b| b ^ 0x36).collect::<Vec<u8>>());
    inner.update(message);
    let mut outer = Sha256::new();
    outer.update(block.iter().map(|b| b ^ 0x5c).collect::<Vec<u8>>());
    outer.update(inner.finalize());
    outer.finalize().into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// RFC 3986 percent-encoding (everything but unreserved characters), as SigV4
/// and the AWS query protocol expect.
pub fn uri_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

pub struct Credentials<'a> {
    pub access_key_id:     &'a str,
    pub secret_access_key: &'a str,
}

/// One request to sign.
pub struct SignableRequest<'a> {
    pub method:       &'a str,
    pub host:         &'a str,
    pub path:         &'a str,
    /// Already canonical (sorted, encoded); empty when none.
    pub query:        &'a str,
    pub content_type: &'a str,
    pub body:         &'a [u8],
    /// `YYYYMMDDTHHMMSSZ`.
    pub amz_date:     &'a str,
    pub region:       &'a str,
    pub service:      &'a str,
}

/// The `Authorization` header value for `request`.
pub fn authorization(request: &SignableRequest<'_>, credentials: &Credentials<'_>) -> String {
    let date = &request.amz_date[..8];
    let signed_headers = "content-type;host;x-amz-date";
    let canonical_request = format!(
        "{}\n{}\n{}\ncontent-type:{}\nhost:{}\nx-amz-date:{}\n\n{}\n{}",
        request.method,
        request.path,
        request.query,
        request.content_type,
        request.host,
        request.amz_date,
        signed_headers,
        hex(&Sha256::digest(request.body)),
    );
    let scope = format!("{date}/{}/{}/aws4_request", request.region, request.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        request.amz_date,
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let k_date = hmac_sha256(format!("AWS4{}", credentials.secret_access_key).as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, request.region.as_bytes());
    let k_service = hmac_sha256(&k_region, request.service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        credentials.access_key_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_matches_rfc_4231_case_2() {
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    /// AWS's documented SigV4 example (IAM `ListUsers`, "Create a signed AWS API
    /// request" in the IAM user guide).
    #[test]
    fn signs_like_the_aws_documentation_example() {
        let header = authorization(
            &SignableRequest {
                method: "GET",
                host: "iam.amazonaws.com",
                path: "/",
                query: "Action=ListUsers&Version=2010-05-08",
                content_type: "application/x-www-form-urlencoded; charset=utf-8",
                body: b"",
                amz_date: "20150830T123600Z",
                region: "us-east-1",
                service: "iam",
            },
            &Credentials {
                access_key_id: "AKIDEXAMPLE",
                secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            },
        );
        assert_eq!(
            header,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date, \
             Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }

    #[test]
    fn encoding_keeps_only_unreserved_characters() {
        assert_eq!(uri_encode("+33 6/é~"), "%2B33%206%2F%C3%A9~");
    }
}
