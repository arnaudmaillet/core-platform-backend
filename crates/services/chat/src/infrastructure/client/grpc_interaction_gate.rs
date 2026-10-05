//! Who may message whom over gRPC: social-graph's mesh-only
//! `CheckInteraction(MESSAGE)` (#656).

use async_trait::async_trait;
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;
use social_graph_api::{CheckInteractionRequest, CheckInteractionResponse, InteractionKind, InteractionRefusal};
use tonic::transport::Channel;

use crate::application::port::{InteractionGate, MessageVerdict};
use crate::domain::value_object::ProfileId;
use crate::error::ChatError;

/// `CheckInteraction` client. The channel is `Arc`-backed, so each call clones
/// the client; it must carry a request timeout (tonic has none by default).
pub struct GrpcInteractionGate {
    social_graph: SocialGraphServiceClient<Channel>,
}

impl GrpcInteractionGate {
    pub fn new(social_graph: Channel) -> Self {
        Self { social_graph: SocialGraphServiceClient::new(social_graph) }
    }
}

/// Maps the answer. A refusal without a reason (a server from before the
/// reason existed) is treated as a block: nothing delivered, nothing told.
fn verdict_of(response: &CheckInteractionResponse) -> MessageVerdict {
    if response.allowed {
        return if response.held { MessageVerdict::Request } else { MessageVerdict::Allowed };
    }
    match InteractionRefusal::try_from(response.refusal) {
        Ok(InteractionRefusal::Audience) => MessageVerdict::Request,
        Ok(InteractionRefusal::NoOne) => MessageVerdict::Refused,
        _ => MessageVerdict::Silenced,
    }
}

#[async_trait]
impl InteractionGate for GrpcInteractionGate {
    async fn may_message(&self, actor: &ProfileId, recipient: &ProfileId) -> Result<MessageVerdict, ChatError> {
        let response = self
            .social_graph
            .clone()
            .check_interaction(CheckInteractionRequest {
                actor_profile_id:  actor.as_str(),
                target_profile_id: recipient.as_str(),
                kind:              InteractionKind::Message as i32,
            })
            .await
            .map_err(|status| ChatError::InteractionCheckUnavailable { reason: status.to_string() })?
            .into_inner();
        Ok(verdict_of(&response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(allowed: bool, held: bool, refusal: InteractionRefusal) -> CheckInteractionResponse {
        CheckInteractionResponse { allowed, held, refusal: refusal as i32 }
    }

    #[test]
    fn each_answer_maps_to_its_verdict_and_an_unexplained_refusal_is_silent() {
        use InteractionRefusal::*;
        assert_eq!(verdict_of(&answer(true, false, Unspecified)), MessageVerdict::Allowed);
        assert_eq!(verdict_of(&answer(true, true, Unspecified)), MessageVerdict::Request, "held ⇒ a request");
        assert_eq!(verdict_of(&answer(false, false, Audience)), MessageVerdict::Request);
        assert_eq!(verdict_of(&answer(false, false, NoOne)), MessageVerdict::Refused);
        assert_eq!(verdict_of(&answer(false, false, Blocked)), MessageVerdict::Silenced);
        assert_eq!(verdict_of(&answer(false, false, Unspecified)), MessageVerdict::Silenced);
    }
}
