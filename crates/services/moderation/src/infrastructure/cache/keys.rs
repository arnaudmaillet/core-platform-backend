//! Redis key builders. Single-key operations, but every key carries a `{…}` hash
//! tag on its entity so any future multi-key script stays slot-safe on Redis
//! Cluster (the mandatory slot-safety rule).

use crate::domain::value_object::ActorId;

/// Hot-path enforcement flag for an actor: `mod:enf:{<actor>}`.
pub fn enforcement_key(actor: &ActorId) -> String {
    format!("mod:enf:{{{actor}}}")
}

/// Known-bad corpus entry for a content hash: `mod:corpus:{<algo>}:<value>`.
pub fn corpus_key(algorithm: &str, value: &str) -> String {
    format!("mod:corpus:{{{algorithm}}}:{value}")
}

/// Classification debounce marker for a subject: `mod:cls:{<entity>:<id>}`.
pub fn classification_debounce_key(entity_type: &str, entity_id: &str) -> String {
    format!("mod:cls:{{{entity_type}:{entity_id}}}")
}

/// A reporter's report counter for one window: `mod:rq:{<reporter>}:<window>:<bucket>`.
pub fn report_quota_key(reporter: &str, window: &str, bucket: i64) -> String {
    format!("mod:rq:{{{reporter}}}:{window}:{bucket}")
}
