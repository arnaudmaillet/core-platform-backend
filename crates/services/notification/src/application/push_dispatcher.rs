//! Push delivery (#654): a notification written for the first time goes to its
//! recipient's iOS devices — unless their preferences hold it (category off,
//! paused, quiet hours). Fail-open: a push never delays nor fails the write;
//! a failure is logged. A token APNs says is gone is forgotten.

use std::sync::Arc;

use chrono::Utc;

use crate::application::port::{
    DeviceRegistry, NotificationPayload, PreferenceStore, PushNotifier, PushOutcome, PushSender,
    SenderNames, UnreadCounter,
};
use crate::domain::device::DevicePlatform;
use crate::domain::push_message::{PushMessage, PushSubject};
use crate::domain::value_object::ProfileId;
use crate::error::NotificationError;

#[derive(Clone)]
pub struct PushDispatcher {
    pub devices:     Arc<dyn DeviceRegistry>,
    pub preferences: Arc<dyn PreferenceStore>,
    pub counter:     Arc<dyn UnreadCounter>,
    pub names:       Arc<dyn SenderNames>,
    pub sender:      Arc<dyn PushSender>,
}

impl PushNotifier for PushDispatcher {
    fn notify(&self, notification: Arc<NotificationPayload>) {
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(error) = this.deliver(&notification).await {
                tracing::warn!(%error, notification_id = %notification.notification_id, "push not sent");
            }
        });
    }
}

impl PushDispatcher {
    /// Sends `notification`'s push; returns how many devices took it.
    pub async fn deliver(&self, notification: &NotificationPayload) -> Result<usize, NotificationError> {
        let Some(category) = notification.kind.push_category() else {
            return Ok(0);
        };
        let target = ProfileId::from_uuid(notification.target_profile_id);
        // No stored preferences: the adult defaults (a teen's are written at
        // their first device registration, before any push can reach them).
        let preferences = self.preferences.get(&target).await?.unwrap_or_default();
        if !preferences.push_allowed(category, Utc::now()) {
            return Ok(0);
        }
        // APNs only, until an FCM sender exists.
        let devices: Vec<_> =
            self.devices.devices(&target).await?.into_iter().filter(|d| d.platform == DevicePlatform::Ios).collect();
        if devices.is_empty() {
            return Ok(0);
        }

        let sender = ProfileId::from_uuid(notification.sender_profile_id);
        let name = self.names.display_name(&sender).await;
        let badge = match self.counter.get(&target).await {
            Ok(unread) => u32::try_from(unread.max(0)).ok(),
            Err(error) => {
                tracing::debug!(%error, "push without a badge: unread count unavailable");
                None
            }
        };
        let message = PushMessage::new(PushSubject {
            notification_id: notification.notification_id.to_string(),
            kind:            notification.kind,
            subject_kind:    notification.subject_kind.as_str(),
            subject_id:      notification.subject_id.to_string(),
            sender_count:    notification.sender_count,
            sender_name:     name.as_deref(),
            badge,
        });

        let mut delivered = 0;
        for device in &devices {
            match self.sender.send(device, &message).await {
                Ok(PushOutcome::Delivered) => delivered += 1,
                Ok(PushOutcome::TokenGone) => {
                    tracing::info!(device_id = %device.device_id, "push token gone: device forgotten");
                    if let Err(error) = self.devices.unregister(&target, &device.device_id).await {
                        tracing::warn!(%error, "could not forget a gone push token");
                    }
                }
                Err(error) => tracing::warn!(%error, device_id = %device.device_id, "push not delivered"),
            }
        }
        Ok(delivered)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use uuid::Uuid;

    use super::*;
    use crate::application::port::TokenHolder;
    use crate::domain::device::{Device, PushEnvironment};
    use crate::domain::preferences::{NotificationPreferences, PushCategory};
    use crate::domain::value_object::{NotificationKind, SubjectKind};

    #[derive(Default)]
    struct Fakes {
        devices:     Mutex<Vec<Device>>,
        preferences: Mutex<Option<NotificationPreferences>>,
        sent:        Mutex<Vec<(String, PushMessage)>>,
        /// Tokens the push network rejects as gone.
        gone:        Mutex<Vec<String>>,
    }

    #[async_trait]
    impl DeviceRegistry for Fakes {
        async fn register(&self, _: &ProfileId, _: &str, _: &Device) -> Result<(), NotificationError> {
            unimplemented!()
        }
        async fn unregister(&self, _: &ProfileId, device_id: &str) -> Result<(), NotificationError> {
            self.devices.lock().unwrap().retain(|d| d.device_id != device_id);
            Ok(())
        }
        async fn devices(&self, _: &ProfileId) -> Result<Vec<Device>, NotificationError> {
            Ok(self.devices.lock().unwrap().clone())
        }
        async fn token_holders(&self, _: &str) -> Result<Vec<TokenHolder>, NotificationError> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl PreferenceStore for Fakes {
        async fn get(&self, _: &ProfileId) -> Result<Option<NotificationPreferences>, NotificationError> {
            Ok(self.preferences.lock().unwrap().clone())
        }
        async fn put(&self, _: &ProfileId, _: &NotificationPreferences) -> Result<(), NotificationError> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl UnreadCounter for Fakes {
        async fn increment(&self, _: &ProfileId) -> Result<(), NotificationError> { unimplemented!() }
        async fn increment_once(&self, _: &ProfileId, _: &str) -> Result<bool, NotificationError> { unimplemented!() }
        async fn decrement(&self, _: &ProfileId) -> Result<(), NotificationError> { unimplemented!() }
        async fn reset(&self, _: &ProfileId) -> Result<(), NotificationError> { unimplemented!() }
        async fn get(&self, _: &ProfileId) -> Result<i64, NotificationError> { Ok(4) }
        async fn set_read_horizon(&self, _: &ProfileId, _: i64) -> Result<(), NotificationError> { unimplemented!() }
        async fn get_read_horizon(&self, _: &ProfileId) -> Result<i64, NotificationError> { unimplemented!() }
    }

    #[async_trait]
    impl SenderNames for Fakes {
        async fn display_name(&self, _: &ProfileId) -> Option<String> {
            Some("Alice".into())
        }
    }

    #[async_trait]
    impl PushSender for Fakes {
        async fn send(&self, device: &Device, message: &PushMessage) -> Result<PushOutcome, NotificationError> {
            if self.gone.lock().unwrap().contains(&device.token) {
                return Ok(PushOutcome::TokenGone);
            }
            self.sent.lock().unwrap().push((device.token.clone(), message.clone()));
            Ok(PushOutcome::Delivered)
        }
    }

    fn device(id: &str, platform: DevicePlatform) -> Device {
        Device {
            device_id:     id.into(),
            token:         format!("token-{id}"),
            platform,
            environment:   PushEnvironment::Production,
            registered_at: Utc::now(),
        }
    }

    fn setup() -> (Arc<Fakes>, PushDispatcher) {
        let fakes = Arc::new(Fakes::default());
        let dispatcher = PushDispatcher {
            devices:     Arc::clone(&fakes) as _,
            preferences: Arc::clone(&fakes) as _,
            counter:     Arc::clone(&fakes) as _,
            names:       Arc::clone(&fakes) as _,
            sender:      Arc::clone(&fakes) as _,
        };
        (fakes, dispatcher)
    }

    fn like() -> NotificationPayload {
        NotificationPayload {
            notification_id:   Uuid::now_v7(),
            target_profile_id: Uuid::now_v7(),
            sender_profile_id: Uuid::now_v7(),
            sample_sender_ids: Vec::new(),
            sender_count:      1,
            kind:              NotificationKind::Reaction,
            subject_kind:      SubjectKind::Post,
            subject_id:        Uuid::now_v7(),
            created_at_ms:     0,
        }
    }

    #[tokio::test]
    async fn a_like_goes_to_every_ios_device_with_the_name_and_badge() {
        let (fakes, dispatcher) = setup();
        *fakes.devices.lock().unwrap() =
            vec![device("a", DevicePlatform::Ios), device("b", DevicePlatform::Ios), device("c", DevicePlatform::Android)];
        let like = like();
        assert_eq!(dispatcher.deliver(&like).await.unwrap(), 2, "iOS only");
        let sent = fakes.sent.lock().unwrap();
        let (_, message) = &sent[0];
        assert_eq!(message.loc_key, "NTF_PUSH_REACTION");
        assert_eq!(message.loc_args, vec!["Alice".to_owned()]);
        assert_eq!(message.badge, Some(4));
        assert_eq!((message.subject_kind, message.subject_id.clone()), ("post", like.subject_id.to_string()));
    }

    #[tokio::test]
    async fn a_category_turned_off_stops_that_push() {
        let (fakes, dispatcher) = setup();
        *fakes.devices.lock().unwrap() = vec![device("a", DevicePlatform::Ios)];
        let mut preferences = NotificationPreferences::default();
        preferences.set_push(PushCategory::Likes, false);
        *fakes.preferences.lock().unwrap() = Some(preferences);
        assert_eq!(dispatcher.deliver(&like()).await.unwrap(), 0);
        // Another category still goes.
        let comment = NotificationPayload { kind: NotificationKind::Comment, ..like() };
        assert_eq!(dispatcher.deliver(&comment).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn a_pause_holds_every_push_and_appeals_are_never_pushed() {
        let (fakes, dispatcher) = setup();
        *fakes.devices.lock().unwrap() = vec![device("a", DevicePlatform::Ios)];
        let appeal = NotificationPayload { kind: NotificationKind::AppealOverturned, ..like() };
        assert_eq!(dispatcher.deliver(&appeal).await.unwrap(), 0, "feed only");
        let paused = NotificationPreferences { paused_until: Some(Utc::now() + chrono::Duration::hours(1)), ..Default::default() };
        *fakes.preferences.lock().unwrap() = Some(paused);
        assert_eq!(dispatcher.deliver(&like()).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_gone_token_is_forgotten() {
        let (fakes, dispatcher) = setup();
        *fakes.devices.lock().unwrap() = vec![device("a", DevicePlatform::Ios), device("b", DevicePlatform::Ios)];
        fakes.gone.lock().unwrap().push("token-a".into());
        assert_eq!(dispatcher.deliver(&like()).await.unwrap(), 1);
        let left: Vec<_> = fakes.devices.lock().unwrap().iter().map(|d| d.device_id.clone()).collect();
        assert_eq!(left, vec!["b".to_owned()]);
    }
}
