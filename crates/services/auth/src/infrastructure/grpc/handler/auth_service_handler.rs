use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;
use error::AppError;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use transport::grpc::edge;
use crate::application::command::{
    ChangeContactCommand, ChangeContactHandler, ChangePasswordCommand, ChangePasswordHandler, FederatedNonces, GuestAttestation, CompleteLoginCommand, IssuedSession, LoginCommand, LoginHandler, LoginOutcome, MfaCaller,
    MfaSettingsHandler, PasskeyAssertion, PasskeyHandler, PasskeyRegistration, PasskeySignIn, PasskeyView,
    LogoutAllSessionsCommand, LogoutAllSessionsHandler, LogoutCommand, LogoutHandler,
    RefreshCommand, RefreshHandler, SignUpCommand, SignUpCredential, SignUpHandler, SignUpOutcome,
    StartGuestSessionCommand, StartGuestSessionHandler, StartVerificationCommand, VerificationCodes,
    StepUpCredential, VerifyCredentialsCommand, VerifyCredentialsHandler,
};
use crate::application::port::{AuthnGrant, SignUpConsent, VerificationChannel};
use crate::application::query::{
    IntrospectHandler, IntrospectQuery, ListSessionsHandler, ListSessionsQuery, SessionSummary,
};
use crate::domain::value_object::{DeviceFingerprint, FederatedProvider, SessionStatus, SignInMethod};
use crate::error::AuthError;

// ── Proto inclusion ───────────────────────────────────────────────────────────
pub use auth_api as proto;

/// gRPC request handler for the `auth.v1` service.
///
/// Each method translates an inbound Protobuf request into an application
/// command/query, invokes the corresponding handler with a fresh correlation id
/// and the wall clock, and maps the result (or [`AuthError`]) back to Protobuf /
/// [`Status`]. The application handlers — not a CQRS bus — are held directly,
/// because the token-returning use-cases do not fit the command bus's `()` return.
#[derive(Clone)]
pub struct AuthServiceHandler {
    login: Arc<LoginHandler>,
    refresh: Arc<RefreshHandler>,
    logout: Arc<LogoutHandler>,
    logout_all: Arc<LogoutAllSessionsHandler>,
    introspect: Arc<IntrospectHandler>,
    list_sessions: Arc<ListSessionsHandler>,
    start_guest: Arc<StartGuestSessionHandler>,
    change_password: Arc<ChangePasswordHandler>,
    verify_credentials: Arc<VerifyCredentialsHandler>,
    sign_up: Option<Arc<SignUpHandler>>,
    codes: Option<Arc<VerificationCodes>>,
    change_contact: Option<Arc<ChangeContactHandler>>,
    mfa_settings: Option<Arc<MfaSettingsHandler>>,
    passkeys: Option<Arc<PasskeyHandler>>,
    passkey_sign_in: Option<Arc<PasskeySignIn>>,
    nonces: Option<Arc<FederatedNonces>>,
    attestation: Option<Arc<GuestAttestation>>,
    /// Proxies appending to `X-Forwarded-For` in front of the edge
    /// (`GRPC_TRUSTED_PROXY_HOPS`), to find the client's address.
    trusted_proxy_hops: usize,
}

impl AuthServiceHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        login: Arc<LoginHandler>,
        refresh: Arc<RefreshHandler>,
        logout: Arc<LogoutHandler>,
        logout_all: Arc<LogoutAllSessionsHandler>,
        introspect: Arc<IntrospectHandler>,
        list_sessions: Arc<ListSessionsHandler>,
        start_guest: Arc<StartGuestSessionHandler>,
        change_password: Arc<ChangePasswordHandler>,
        verify_credentials: Arc<VerifyCredentialsHandler>,
    ) -> Self {
        Self {
            login,
            refresh,
            logout,
            logout_all,
            introspect,
            list_sessions,
            start_guest,
            change_password,
            verify_credentials,
            sign_up: None,
            codes: None,
            change_contact: None,
            mfa_settings: None,
            passkeys: None,
            passkey_sign_in: None,
            nonces: None,
            attestation: None,
            trusted_proxy_hops: transport::grpc::client_ip::DEFAULT_TRUSTED_PROXY_HOPS,
        }
    }

    /// Sets how many trusted proxies append to `X-Forwarded-For`.
    pub fn with_trusted_proxy_hops(mut self, hops: usize) -> Self {
        self.trusted_proxy_hops = hops;
        self
    }

    /// The caller's address as the transport saw it (`None` when unknown).
    fn client_ip<T>(&self, request: &Request<T>) -> Option<String> {
        transport::grpc::client_ip::request_client_ip(request, self.trusted_proxy_hops).map(|ip| ip.to_string())
    }

    /// Enables ChangeContact (#651).
    pub fn with_change_contact(mut self, handler: Arc<ChangeContactHandler>) -> Self {
        self.change_contact = Some(handler);
        self
    }

    /// Edge `authenticated` + a recent credential proof (#651): the caller's
    /// email or phone becomes the address a `StartVerification` code proved.
    pub async fn change_contact(
        &self,
        request: Request<proto::ChangeContactRequest>,
    ) -> Result<Response<proto::ChangeContactResponse>, Status> {
        let handler = self
            .change_contact
            .as_ref()
            .ok_or_else(|| Status::unimplemented("changing an email or phone is not enabled"))?;
        // A takeover vector: only right after the holder proved a credential.
        edge::require_recent_auth(&request, edge::STEP_UP_MAX_AGE_SECS)?;
        let (account_id, session_id) = caller(&request)?;
        let client_ip = self.client_ip(&request);
        let req = request.into_inner();
        let cmd = ChangeContactCommand {
            account_id,
            session_id,
            challenge_id: req.challenge_id,
            code: req.code,
            client_ip,
            locale: Some(req.locale).filter(|l| !l.is_empty()),
        };
        let changed = handler
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::ChangeContactResponse {
            channel: match changed.channel {
                VerificationChannel::Email => proto::VerificationChannel::Email,
                VerificationChannel::Sms => proto::VerificationChannel::Sms,
            } as i32,
            destination: changed.destination,
        }))
    }

    /// Enables the two-step sign-in settings RPCs (#649).
    pub fn with_mfa_settings(mut self, handler: Arc<MfaSettingsHandler>) -> Self {
        self.mfa_settings = Some(handler);
        self
    }

    fn mfa_settings(&self) -> Result<&MfaSettingsHandler, Status> {
        self.mfa_settings.as_deref().ok_or_else(|| Status::unimplemented("two-step sign-in settings are not enabled"))
    }

    /// Edge `authenticated` + a recent credential proof (#649).
    pub async fn start_mfa_enrollment(
        &self,
        request: Request<proto::StartMfaEnrollmentRequest>,
    ) -> Result<Response<proto::StartMfaEnrollmentResponse>, Status> {
        let handler = self.mfa_settings()?;
        edge::require_recent_auth(&request, edge::STEP_UP_MAX_AGE_SECS)?;
        let (account_id, session_id) = caller(&request)?;
        let started = handler
            .start(Envelope::new(Uuid::now_v7(), MfaCaller { account_id, session_id }), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::StartMfaEnrollmentResponse {
            secret: started.secret,
            otpauth_uri: started.otpauth_uri,
            expires_in: started.expires_in_secs,
        }))
    }

    /// Edge `authenticated` (#649): the enrolment was started after a step-up.
    pub async fn confirm_mfa_enrollment(
        &self,
        request: Request<proto::ConfirmMfaEnrollmentRequest>,
    ) -> Result<Response<proto::BackupCodesResponse>, Status> {
        let handler = self.mfa_settings()?;
        let (account_id, session_id) = caller(&request)?;
        let code = request.into_inner().code;
        let codes = handler
            .confirm(Envelope::new(Uuid::now_v7(), (MfaCaller { account_id, session_id }, code)), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::BackupCodesResponse {
            backup_codes: codes.codes,
            sessions_revoked: codes.sessions_revoked,
        }))
    }

    /// Edge `authenticated` + a recent credential proof (#649).
    pub async fn disable_mfa(
        &self,
        request: Request<proto::DisableMfaRequest>,
    ) -> Result<Response<proto::DisableMfaResponse>, Status> {
        let handler = self.mfa_settings()?;
        edge::require_recent_auth(&request, edge::STEP_UP_MAX_AGE_SECS)?;
        let (account_id, session_id) = caller(&request)?;
        handler
            .disable(Envelope::new(Uuid::now_v7(), MfaCaller { account_id, session_id }), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::DisableMfaResponse {}))
    }

    /// Edge `authenticated` + a recent credential proof (#649).
    pub async fn regenerate_backup_codes(
        &self,
        request: Request<proto::RegenerateBackupCodesRequest>,
    ) -> Result<Response<proto::BackupCodesResponse>, Status> {
        let handler = self.mfa_settings()?;
        edge::require_recent_auth(&request, edge::STEP_UP_MAX_AGE_SECS)?;
        let (account_id, session_id) = caller(&request)?;
        let codes = handler
            .regenerate(Envelope::new(Uuid::now_v7(), MfaCaller { account_id, session_id }), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::BackupCodesResponse {
            backup_codes: codes.codes,
            sessions_revoked: codes.sessions_revoked,
        }))
    }

    /// Enables the passkey RPCs (#808): an RP id is configured.
    pub fn with_passkeys(mut self, handler: Arc<PasskeyHandler>) -> Self {
        self.passkeys = Some(handler);
        self
    }

    /// Enables StartPasskeySignIn (#808).
    pub fn with_passkey_sign_in(mut self, sign_in: Arc<PasskeySignIn>) -> Self {
        self.passkey_sign_in = Some(sign_in);
        self
    }

    /// Edge `public` (#808): a challenge for a passkey assertion.
    pub async fn start_passkey_sign_in(
        &self,
        _request: Request<proto::StartPasskeySignInRequest>,
    ) -> Result<Response<proto::PasskeySignInOptions>, Status> {
        let sign_in =
            self.passkey_sign_in.as_deref().ok_or_else(|| Status::unavailable("passkeys are not configured"))?;
        let options = sign_in.start().await.map_err(auth_error_to_status)?;
        Ok(Response::new(proto::PasskeySignInOptions {
            challenge:  options.challenge,
            rp_id:      options.rp_id,
            expires_in: options.expires_in_secs,
        }))
    }

    fn passkeys(&self) -> Result<&PasskeyHandler, Status> {
        self.passkeys.as_deref().ok_or_else(|| Status::unavailable("passkeys are not configured"))
    }

    /// Edge `authenticated` + a recent credential proof (#808).
    pub async fn start_passkey_registration(
        &self,
        request: Request<proto::StartPasskeyRegistrationRequest>,
    ) -> Result<Response<proto::PasskeyRegistrationOptions>, Status> {
        let handler = self.passkeys()?;
        edge::require_recent_auth(&request, edge::STEP_UP_MAX_AGE_SECS)?;
        let (account_id, session_id) = caller(&request)?;
        let options = handler
            .start_registration(Envelope::new(Uuid::now_v7(), MfaCaller { account_id, session_id }), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::PasskeyRegistrationOptions {
            challenge:           options.challenge,
            rp_id:               options.rp_id,
            user_id:             options.user_id,
            user_name:           options.user_name,
            exclude_credentials: options.exclude_credentials,
            expires_in:          options.expires_in_secs,
        }))
    }

    /// Edge `authenticated` (#808): the challenge is bound to the caller's
    /// account, and was started after a step-up.
    pub async fn finish_passkey_registration(
        &self,
        request: Request<proto::FinishPasskeyRegistrationRequest>,
    ) -> Result<Response<proto::Passkey>, Status> {
        let handler = self.passkeys()?;
        let (account_id, session_id) = caller(&request)?;
        let req = request.into_inner();
        let registration = PasskeyRegistration {
            challenge:          req.challenge,
            client_data_json:   req.client_data_json,
            attestation_object: req.attestation_object,
            name:               req.name,
        };
        let made = handler
            .finish_registration(
                Envelope::new(Uuid::now_v7(), (MfaCaller { account_id, session_id }, registration)),
                Utc::now(),
            )
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(passkey_to_proto(made)))
    }

    /// Edge `authenticated` (#808).
    pub async fn list_passkeys(
        &self,
        request: Request<proto::ListPasskeysRequest>,
    ) -> Result<Response<proto::ListPasskeysResponse>, Status> {
        let handler = self.passkeys()?;
        let (account_id, session_id) = caller(&request)?;
        let passkeys = handler
            .list(Envelope::new(Uuid::now_v7(), MfaCaller { account_id, session_id }), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::ListPasskeysResponse { passkeys: passkeys.into_iter().map(passkey_to_proto).collect() }))
    }

    /// Edge `authenticated` + a recent credential proof (#808).
    pub async fn remove_passkey(
        &self,
        request: Request<proto::RemovePasskeyRequest>,
    ) -> Result<Response<proto::ListPasskeysResponse>, Status> {
        let handler = self.passkeys()?;
        edge::require_recent_auth(&request, edge::STEP_UP_MAX_AGE_SECS)?;
        let (account_id, session_id) = caller(&request)?;
        let credential_id = request.into_inner().credential_id;
        let left = handler
            .remove(
                Envelope::new(Uuid::now_v7(), (MfaCaller { account_id, session_id }, credential_id)),
                Utc::now(),
            )
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::ListPasskeysResponse { passkeys: left.into_iter().map(passkey_to_proto).collect() }))
    }

    /// Enables StartVerification (email one-time codes).
    pub fn with_codes(mut self, codes: Arc<VerificationCodes>) -> Self {
        self.codes = Some(codes);
        self
    }

    /// Edge `public`: sends a one-time code (see `VerificationCodes`).
    pub async fn start_verification(
        &self,
        request: Request<proto::StartVerificationRequest>,
    ) -> Result<Response<proto::StartVerificationResponse>, Status> {
        let codes = self.codes.as_ref().ok_or_else(|| Status::unimplemented("verification codes are not enabled"))?;
        let client_ip = self.client_ip(&request);
        let req = request.into_inner();
        let channel = match proto::VerificationChannel::try_from(req.channel) {
            Ok(proto::VerificationChannel::Email) => VerificationChannel::Email,
            Ok(proto::VerificationChannel::Sms) => VerificationChannel::Sms,
            _ => return Err(Status::invalid_argument("channel must be EMAIL or SMS")),
        };
        let started = codes
            .start(StartVerificationCommand {
                channel,
                destination: req.destination,
                locale: Some(req.locale).filter(|l| !l.is_empty()),
                client_ip,
            })
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::StartVerificationResponse {
            challenge_id: started.challenge_id,
            expires_in_secs: started.expires_in_secs,
            resend_after_secs: started.resend_after_secs,
        }))
    }

    /// Enables StartDeviceAttestation (App Attest challenges).
    pub fn with_device_attestation(mut self, attestation: Arc<GuestAttestation>) -> Self {
        self.attestation = Some(attestation);
        self
    }

    /// Edge `public`: a single-use App Attest challenge.
    pub async fn start_device_attestation(
        &self,
        _request: Request<proto::StartDeviceAttestationRequest>,
    ) -> Result<Response<proto::StartDeviceAttestationResponse>, Status> {
        let attestation =
            self.attestation.as_ref().ok_or_else(|| Status::unimplemented("device attestation is not enabled"))?;
        let started = attestation.start().await.map_err(auth_error_to_status)?;
        Ok(Response::new(proto::StartDeviceAttestationResponse {
            challenge: started.challenge,
            expires_in_secs: started.expires_in_secs,
        }))
    }

    /// Enables StartFederatedSignIn (server-issued sign-in nonces).
    pub fn with_federated_nonces(mut self, nonces: Arc<FederatedNonces>) -> Self {
        self.nonces = Some(nonces);
        self
    }

    /// Edge `public`: a single-use nonce for a native Sign in with Apple / Google.
    pub async fn start_federated_sign_in(
        &self,
        _request: Request<proto::StartFederatedSignInRequest>,
    ) -> Result<Response<proto::StartFederatedSignInResponse>, Status> {
        let nonces = self.nonces.as_ref().ok_or_else(|| Status::unimplemented("sign-in nonces are not enabled"))?;
        let started = nonces.start().await.map_err(auth_error_to_status)?;
        Ok(Response::new(proto::StartFederatedSignInResponse {
            nonce: started.nonce,
            expires_in_secs: started.expires_in_secs,
        }))
    }

    /// Enables SignUp (native Sign in with Apple / Google).
    pub fn with_sign_up(mut self, sign_up: Arc<SignUpHandler>) -> Self {
        self.sign_up = Some(sign_up);
        self
    }

    /// Edge `public`: creates an account from a provider id_token (see `SignUpHandler`).
    pub async fn sign_up(
        &self,
        request: Request<proto::SignUpRequest>,
    ) -> Result<Response<proto::SignUpResponse>, Status> {
        let handler = self.sign_up.as_ref().ok_or_else(|| Status::unimplemented("sign-up is not enabled"))?;
        let client_ip = self.client_ip(&request);
        let req = request.into_inner();
        let credential = match (req.id_token, req.verification_code) {
            (Some(grant), None) => SignUpCredential::IdToken {
                provider: provider_from_proto(grant.provider)?,
                id_token: grant.id_token,
                nonce: grant.nonce,
            },
            (None, Some(code)) => SignUpCredential::Code { challenge_id: code.challenge_id, code: code.code },
            _ => {
                return Err(Status::invalid_argument(
                    "sign-up requires exactly one of id_token and verification_code",
                ));
            }
        };
        let consent = req.consent.unwrap_or_default();
        let cmd = SignUpCommand {
            credential,
            date_of_birth: req.date_of_birth,
            consent: SignUpConsent {
                policy_version: consent.policy_version,
                data_processing: consent.data_processing,
                marketing: consent.marketing,
                analytics: consent.analytics,
            },
            home_country: Some(req.home_country).filter(|c| !c.is_empty()),
            device: device_from_proto(req.device, client_ip.clone()),
            guest_refresh_token: Some(req.guest_refresh_token).filter(|t| !t.is_empty()),
            client_ip,
        };
        let outcome = handler
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        let outcome = match outcome {
            SignUpOutcome::SignedUp { account_id, session_id, access_token, refresh_token, access_expires_in } => {
                proto::sign_up_response::Outcome::SignedUp(proto::SignedUp {
                    account_id: account_id.as_str(),
                    tokens: Some(proto::TokenPair {
                        access_token,
                        refresh_token,
                        token_type: "Bearer".to_owned(),
                        expires_in: access_expires_in,
                        session_id: session_id.as_str(),
                    }),
                })
            }
            SignUpOutcome::ExistingAccount { method } => {
                proto::sign_up_response::Outcome::ExistingAccount(proto::ExistingAccount {
                    method: method_to_proto(method) as i32,
                })
            }
        };
        Ok(Response::new(proto::SignUpResponse { outcome: Some(outcome) }))
    }

    /// Edge `authenticated`: the account and session are the caller's (`sub`,
    /// `sid`). A mesh call has no holder to prove a password for.
    pub async fn change_password(
        &self,
        request: Request<proto::ChangePasswordRequest>,
    ) -> Result<Response<proto::ChangePasswordResponse>, Status> {
        let (account_id, session_id) = caller(&request)?;
        let req = request.into_inner();
        let cmd = ChangePasswordCommand {
            account_id,
            session_id,
            current_password: req.current_password,
            new_password: req.new_password,
            sign_out_other_sessions: req.sign_out_other_sessions,
        };
        let out = self
            .change_password
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::ChangePasswordResponse { sessions_revoked: out.sessions_revoked }))
    }

    /// Edge `authenticated` step-up: see `auth.v1.VerifyCredentials`.
    pub async fn verify_credentials(
        &self,
        request: Request<proto::VerifyCredentialsRequest>,
    ) -> Result<Response<proto::VerifyCredentialsResponse>, Status> {
        use proto::verify_credentials_request::Credential;
        let (account_id, session_id) = caller(&request)?;
        let credential = match request.into_inner().credential {
            Some(Credential::Password(p)) => StepUpCredential::Password(p),
            Some(Credential::MfaCode(c)) => StepUpCredential::MfaCode(c),
            Some(Credential::Passkey(a)) => StepUpCredential::Passkey(assertion_from_proto(a)),
            None => return Err(Status::invalid_argument("a credential is required")),
        };
        let cmd = VerifyCredentialsCommand { account_id, session_id, credential };
        let token = self
            .verify_credentials
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::VerifyCredentialsResponse {
            access_token: token.access_token,
            expires_in: token.access_expires_in,
            step_up_expires_in: token.step_up_expires_in,
        }))
    }

    pub async fn start_guest_session(
        &self,
        request: Request<proto::StartGuestSessionRequest>,
    ) -> Result<Response<proto::StartGuestSessionResponse>, Status> {
        let client_ip = self.client_ip(&request);
        let req = request.into_inner();
        let non_empty = |s: String| if s.trim().is_empty() { None } else { Some(s) };
        let cmd = StartGuestSessionCommand {
            device: device_from_proto(req.device, client_ip),
            attestation: non_empty(req.attestation),
            attest_key_id: non_empty(req.attest_key_id),
            attest_challenge: non_empty(req.attest_challenge),
            locale: non_empty(req.locale),
            region_hint: non_empty(req.region_hint),
            current_country: non_empty(req.current_country),
        };

        let issued = self
            .start_guest
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;

        Ok(Response::new(proto::StartGuestSessionResponse {
            guest_id: issued.account_id.as_str(),
            tokens: Some(token_pair(&issued)),
        }))
    }

    pub async fn login(
        &self,
        request: Request<proto::LoginRequest>,
    ) -> Result<Response<proto::LoginResponse>, Status> {
        let client_ip = self.client_ip(&request);
        let req = request.into_inner();
        let grant = grant_from_proto(req.credential)?;
        let cmd = LoginCommand {
            grant,
            device: device_from_proto(req.device, client_ip.clone()),
            guest_refresh_token: Some(req.guest_refresh_token).filter(|t| !t.is_empty()),
            client_ip,
        };

        let outcome = self
            .login
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;

        Ok(Response::new(match outcome {
            LoginOutcome::Issued(issued) => login_response(&issued),
            LoginOutcome::SecondFactorRequired(challenge) => proto::LoginResponse {
                account_id: challenge.account_id.as_str(),
                mfa_required: true,
                mfa_token: challenge.mfa_token,
                mfa_expires_in: challenge.expires_in_secs,
                ..Default::default()
            },
        }))
    }

    /// The second step of a sign-in (#649).
    pub async fn complete_login(
        &self,
        request: Request<proto::CompleteLoginRequest>,
    ) -> Result<Response<proto::LoginResponse>, Status> {
        let req = request.into_inner();
        let cmd = CompleteLoginCommand {
            mfa_token: req.mfa_token,
            code:      req.code,
            passkey:   req.passkey.map(assertion_from_proto),
        };
        let issued = self
            .login
            .complete(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(login_response(&issued)))
    }

    pub async fn refresh(
        &self,
        request: Request<proto::RefreshRequest>,
    ) -> Result<Response<proto::RefreshResponse>, Status> {
        let client_ip = self.client_ip(&request);
        let req = request.into_inner();
        let cmd = RefreshCommand {
            refresh_token: req.refresh_token,
            device: device_from_proto(req.device, client_ip),
        };

        let issued = self
            .refresh
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;

        Ok(Response::new(proto::RefreshResponse { tokens: Some(token_pair(&issued)) }))
    }

    pub async fn logout(
        &self,
        request: Request<proto::LogoutRequest>,
    ) -> Result<Response<proto::LogoutResponse>, Status> {
        // Edge: an empty session_id means "my current session" (the token's
        // `sid`), and the session must belong to the caller. Mesh: as supplied.
        let principal = edge::principal(&request);
        let supplied = &request.get_ref().session_id;
        let session_id = if supplied.is_empty() {
            principal.and_then(|p| p.session_id()).map(str::to_owned).unwrap_or_default()
        } else {
            supplied.clone()
        };
        let cmd = LogoutCommand {
            session_id,
            actor: principal.map(|p| p.account_id().to_owned()),
        };
        let out = self
            .logout
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::LogoutResponse { success: out.success }))
    }

    pub async fn logout_all_sessions(
        &self,
        request: Request<proto::LogoutAllSessionsRequest>,
    ) -> Result<Response<proto::LogoutAllSessionsResponse>, Status> {
        let account_id = edge_account(&request, &request.get_ref().account_id)?;
        let cmd = LogoutAllSessionsCommand { account_id };
        let out = self
            .logout_all
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::LogoutAllSessionsResponse {
            success: true,
            generation: out.generation,
            sessions_revoked: out.sessions_revoked,
        }))
    }

    pub async fn introspect(
        &self,
        request: Request<proto::IntrospectRequest>,
    ) -> Result<Response<proto::IntrospectResponse>, Status> {
        let query = IntrospectQuery { access_token: request.into_inner().access_token };
        let view = self
            .introspect
            .handle_at(Envelope::new(Uuid::now_v7(), query), Utc::now())
            .await
            .map_err(auth_error_to_status)?;

        Ok(Response::new(proto::IntrospectResponse {
            active: view.active,
            account_id: view.account_id.unwrap_or_default(),
            session_id: view.session_id.unwrap_or_default(),
            generation: view.generation,
            permissions: view.permissions,
            expires_at: view.expires_at.map(to_timestamp),
        }))
    }

    pub async fn list_sessions(
        &self,
        request: Request<proto::ListSessionsRequest>,
    ) -> Result<Response<proto::ListSessionsResponse>, Status> {
        use cqrs::QueryHandler;
        let query = ListSessionsQuery {
            account_id: edge_account(&request, &request.get_ref().account_id)?,
            // Lets the view flag the caller's own session.
            current_session_id: edge::principal(&request)
                .and_then(|p| p.session_id())
                .map(str::to_owned),
        };
        let sessions = self
            .list_sessions
            .handle(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(auth_error_to_status)?;

        Ok(Response::new(proto::ListSessionsResponse {
            sessions: sessions.into_iter().map(session_view).collect(),
        }))
    }
}

// ── Mapping helpers ───────────────────────────────────────────────────────────

/// Resolves an account-scoped request's `account_id`: on the edge an empty value
/// means "the calling principal's account", a non-empty one must be the caller's
/// own; over the mesh it is taken as supplied (validation rejects empty later).
fn edge_account<T>(request: &Request<T>, supplied: &str) -> Result<String, Status> {
    match edge::principal(request) {
        Some(p) if supplied.is_empty() => Ok(p.account_id().to_owned()),
        Some(_) => {
            edge::require_account(request, supplied)?;
            Ok(supplied.to_owned())
        }
        None => Ok(supplied.to_owned()),
    }
}

/// The verified caller's account and session, for the credential RPCs: they
/// act on the holder behind a token, so a mesh call (no principal) is refused.
fn caller<T>(request: &Request<T>) -> Result<(String, String), Status> {
    let principal = edge::principal(request)
        .ok_or_else(|| Status::unauthenticated("this call needs a signed-in holder"))?;
    let session_id = principal
        .session_id()
        .ok_or_else(|| Status::unauthenticated("the token carries no session"))?;
    Ok((principal.account_id().to_owned(), session_id.to_owned()))
}

fn grant_from_proto(
    credential: Option<proto::login_request::Credential>,
) -> Result<AuthnGrant, Status> {
    match credential {
        Some(proto::login_request::Credential::AuthorizationCode(g)) => {
            Ok(AuthnGrant::AuthorizationCode {
                code: g.code,
                redirect_uri: g.redirect_uri,
                code_verifier: g.code_verifier,
            })
        }
        Some(proto::login_request::Credential::Password(g)) => {
            Ok(AuthnGrant::Password { username: g.username, password: g.password })
        }
        Some(proto::login_request::Credential::IdToken(g)) => Ok(AuthnGrant::IdToken {
            provider: provider_from_proto(g.provider)?,
            id_token: g.id_token,
            nonce: g.nonce,
        }),
        Some(proto::login_request::Credential::VerificationCode(g)) => {
            Ok(AuthnGrant::Code { challenge_id: g.challenge_id, code: g.code })
        }
        Some(proto::login_request::Credential::Passkey(a)) => Ok(AuthnGrant::Passkey(assertion_from_proto(a))),
        None => Err(Status::invalid_argument("login requires a credential")),
    }
}

fn provider_from_proto(provider: i32) -> Result<FederatedProvider, Status> {
    match proto::FederatedProvider::try_from(provider) {
        Ok(proto::FederatedProvider::Apple) => Ok(FederatedProvider::Apple),
        Ok(proto::FederatedProvider::Google) => Ok(FederatedProvider::Google),
        _ => Err(Status::invalid_argument("provider must be APPLE or GOOGLE")),
    }
}

fn method_to_proto(method: SignInMethod) -> proto::SignInMethod {
    match method {
        SignInMethod::Apple => proto::SignInMethod::Apple,
        SignInMethod::Google => proto::SignInMethod::Google,
        SignInMethod::Password => proto::SignInMethod::Password,
        SignInMethod::EmailCode => proto::SignInMethod::EmailCode,
        SignInMethod::PhoneCode => proto::SignInMethod::PhoneCode,
    }
}

/// The device of a request. Its IP is the one the transport saw (`client_ip`)
/// whenever known: the request's own `ip_address` is client-written, only a
/// fallback for display, and never what code lockouts key on at the edge.
fn device_from_proto(device: Option<proto::DeviceContext>, client_ip: Option<String>) -> DeviceFingerprint {
    let non_empty = |s: String| if s.is_empty() { None } else { Some(s) };
    match device {
        Some(d) => DeviceFingerprint::new(
            non_empty(d.user_agent),
            client_ip.or_else(|| non_empty(d.ip_address)),
            non_empty(d.device_id),
        ),
        None => DeviceFingerprint::new(None, client_ip, None),
    }
}

fn login_response(issued: &IssuedSession) -> proto::LoginResponse {
    proto::LoginResponse {
        account_id: issued.account_id.as_str(),
        tokens: Some(token_pair(issued)),
        first_link: issued.first_link,
        reactivated: issued.reactivated,
        ..Default::default()
    }
}

fn token_pair(issued: &IssuedSession) -> proto::TokenPair {
    proto::TokenPair {
        access_token: issued.access_token.clone(),
        refresh_token: issued.refresh_token.clone(),
        token_type: "Bearer".to_owned(),
        expires_in: issued.access_expires_in,
        session_id: issued.session_id.as_str(),
    }
}

fn session_view(summary: SessionSummary) -> proto::SessionView {
    proto::SessionView {
        session_id: summary.session_id,
        status: session_status_to_proto(summary.status),
        generation: summary.generation,
        device: None,
        issued_at: Some(to_timestamp(summary.issued_at)),
        expires_at: Some(to_timestamp(summary.expires_at)),
        absolute_expiry: Some(to_timestamp(summary.absolute_expiry)),
        current: summary.current,
    }
}

fn session_status_to_proto(status: SessionStatus) -> i32 {
    let s = match status {
        SessionStatus::Active => proto::SessionStatus::Active,
        SessionStatus::Revoked => proto::SessionStatus::Revoked,
        SessionStatus::Expired => proto::SessionStatus::Expired,
    };
    s as i32
}

fn assertion_from_proto(a: proto::PasskeyAssertion) -> PasskeyAssertion {
    PasskeyAssertion {
        challenge:          a.challenge,
        credential_id:      a.credential_id,
        client_data_json:   a.client_data_json,
        authenticator_data: a.authenticator_data,
        signature:          a.signature,
        user_handle:        a.user_handle,
    }
}

fn passkey_to_proto(p: PasskeyView) -> proto::Passkey {
    proto::Passkey {
        credential_id: p.credential_id,
        name:          p.name,
        created_at:    Some(to_timestamp(p.created_at)),
        last_used_at:  p.last_used_at.map(to_timestamp),
        synced:        p.synced,
    }
}

fn to_timestamp(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds: dt.timestamp(), nanos: dt.timestamp_subsec_nanos() as i32 }
}

/// Maps an [`AuthError`] to a gRPC [`Status`] using its [`AppError`] metadata, so
/// the HTTP semantics defined once in `error.rs` drive the gRPC code too.
pub fn auth_error_to_status(err: AuthError) -> Status {
    let msg = err.to_string();
    let retryable = err.is_retryable();
    if let AuthError::VerificationRateLimited { retry_after_secs } | AuthError::MfaLocked { retry_after_secs } = err {
        let mut status = Status::resource_exhausted(msg);
        if let Ok(value) = retry_after_secs.to_string().parse() {
            status.metadata_mut().insert("retry-after-secs", value);
        }
        return status;
    }
    match err.http_status().as_u16() {
        401 => Status::unauthenticated(msg),
        403 => Status::permission_denied(msg),
        429 => Status::resource_exhausted(msg),
        404 => Status::not_found(msg),
        409 if retryable => Status::aborted(msg),
        409 => Status::already_exists(msg),
        400 | 422 => Status::failed_precondition(msg),
        502 | 503 => Status::unavailable(msg),
        _ => Status::internal(msg),
    }
}

#[cfg(test)]
mod client_ip_tests {
    use super::*;

    #[test]
    fn the_device_ip_is_the_transports_not_the_requests() {
        let spoofed = Some(proto::DeviceContext {
            user_agent: "app".into(),
            ip_address: "6.6.6.6".into(),
            device_id: "d".into(),
        });
        let device = device_from_proto(spoofed.clone(), Some("203.0.113.9".into()));
        assert_eq!(device.ip_address(), Some("203.0.113.9"));
        // Unknown to the transport: the request's value, for display only.
        assert_eq!(device_from_proto(spoofed, None).ip_address(), Some("6.6.6.6"));
        assert_eq!(device_from_proto(None, Some("203.0.113.9".into())).ip_address(), Some("203.0.113.9"));
    }
}
