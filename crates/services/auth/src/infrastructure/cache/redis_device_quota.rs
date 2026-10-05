//! Guest sessions per attested device per UTC day:
//! `auth:{attest-quota:<sha256(key id)>}:<YYYYMMDD>` (kept 2 days).

use async_trait::async_trait;
use fred::interfaces::LuaInterface;
use redis_storage::{RedisClient, RedisStorageError};
use sha2::{Digest, Sha256};

use crate::application::command::DeviceQuota;
use crate::error::AuthError;

/// KEYS = the day's counter · ARGV = the daily limit. Returns 1 when admitted.
const ADMIT: &str = r#"
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('EXPIRE', KEYS[1], 172800) end
if n > tonumber(ARGV[1]) then return 0 end
return 1
"#;

#[derive(Clone)]
pub struct RedisDeviceQuota {
    client: RedisClient,
}

impl RedisDeviceQuota {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl DeviceQuota for RedisDeviceQuota {
    async fn admit(&self, key_id: &str, daily: u32) -> Result<bool, AuthError> {
        let hash: String = Sha256::digest(key_id.as_bytes()).iter().map(|b| format!("{b:02x}")).collect();
        let day = chrono::Utc::now().format("%Y%m%d");
        let admitted: i64 = self
            .client
            .eval(ADMIT, vec![format!("auth:{{attest-quota:{hash}}}:{day}")], vec![daily.to_string()])
            .await
            .map_err(|e| AuthError::Cache(RedisStorageError::from(e)))?;
        Ok(admitted == 1)
    }
}
