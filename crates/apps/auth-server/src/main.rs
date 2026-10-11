//! `auth-server` — the deployable auth binary. A one-liner over the shared fleet
//! runtime: telemetry, externalized config + hot-reload, ingress layers, dynamic
//! health, and graceful shutdown are all the runtime's job.
//!
//! `auth-server bootstrap-staff <account_id>` (#880) issues the very first staff
//! passkey enrollment grant instead of serving: run it inside an auth pod with
//! `AUTH_STAFF_BOOTSTRAP=true`; it is refused once any staff passkey exists.

use std::net::SocketAddr;

use auth::service::AuthService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    if let Some(command) = args.next() {
        anyhow::ensure!(command == "bootstrap-staff", "unknown command {command:?} (only `bootstrap-staff <account_id>`)");
        let account_id = args.next().ok_or_else(|| anyhow::anyhow!("usage: auth-server bootstrap-staff <account_id>"))?;
        let issued = auth::bootstrap::bootstrap_staff(&account_id).await?;
        // The grant goes to this terminal only: hand it to the staff member,
        // who presents it when registering their passkey.
        println!("staff enrollment grant {} for {account_id}, valid until {}", issued.grant_id, issued.expires_at);
        println!("{}", issued.grant);
        return Ok(());
    }

    let addr: SocketAddr = std::env::var("AUTH_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50060".to_owned())
        .parse()?;

    service_runtime::serve::<AuthService>(addr).await
}
