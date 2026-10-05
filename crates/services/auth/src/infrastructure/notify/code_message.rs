//! The words of a one-time-code message, in the reader's language (French or
//! English for now).

/// `(subject, plain-text body)` for `code`, valid `minutes`.
pub fn code_message(code: &str, minutes: i64, locale: Option<&str>) -> (String, String) {
    let french = locale.is_some_and(|l| l.to_ascii_lowercase().starts_with("fr"));
    if french {
        (
            format!("{code} est ton code"),
            format!(
                "Ton code : {code}\n\nIl expire dans {minutes} minutes. Si tu n'as rien demandé, ignore ce message : \
                 personne ne peut se connecter sans ce code.\n"
            ),
        )
    } else {
        (
            format!("{code} is your code"),
            format!(
                "Your code: {code}\n\nIt expires in {minutes} minutes. If you did not ask for it, ignore this \
                 message: nobody can sign in without the code.\n"
            ),
        )
    }
}

/// `(subject, plain-text body)` telling the owner of an address that wrong
/// codes paused sign-in codes for it.
pub fn lockout_notice_message(locale: Option<&str>) -> (String, String) {
    if locale.is_some_and(|l| l.to_ascii_lowercase().starts_with("fr")) {
        (
            "Codes de connexion mis en pause pour ton adresse".to_owned(),
            "De nombreux codes erronés ont été saisis pour te connecter avec cette adresse. Pour protéger \
             ton compte, nous n'envoyons plus de code pour elle pendant 24 heures.\n\nSi c'était toi, \
             réessaie plus tard. Sinon, tu n'as rien à faire : personne ne peut se connecter sans un code \
             envoyé ici.\n"
                .to_owned(),
        )
    } else {
        (
            "Sign-in codes paused for your address".to_owned(),
            "Many wrong codes were entered to sign in with this address. To protect your account, we are \
             not sending codes to it for 24 hours.\n\nIf it was you, try again later. If not, there is \
             nothing to do: nobody can sign in without a code sent here.\n"
                .to_owned(),
        )
    }
}

/// The email telling the holder that the account's email (`email_changed`) or
/// phone number was just changed (#651), so a takeover is visible to them.
pub fn contact_changed_notice_message(email_changed: bool, locale: Option<&str>) -> (String, String) {
    if locale.is_some_and(|l| l.to_ascii_lowercase().starts_with("fr")) {
        let what = if email_changed { "adresse e-mail" } else { "numéro de téléphone" };
        (
            format!("Ton {what} a été modifié"),
            format!(
                "Le {what} de ton compte vient d'être modifié.\n\nSi c'était toi, tu n'as rien à faire. Sinon, \
                 sécurise ton compte tout de suite : change ton mot de passe et contacte-nous depuis l'app.\n"
            ),
        )
    } else {
        let what = if email_changed { "email address" } else { "phone number" };
        (
            format!("Your {what} was changed"),
            format!(
                "The {what} on your account was just changed.\n\nIf it was you, there is nothing to do. If \
                 not, secure your account now: change your password and contact us from the app.\n"
            ),
        )
    }
}

/// The email telling the holder their account was signed in to from a device
/// it never saw (#649), named by its user agent when known.
pub fn new_login_notice_message(device: Option<&str>, locale: Option<&str>) -> (String, String) {
    let device = device.map(|d| d.chars().take(120).collect::<String>()).filter(|d| !d.trim().is_empty());
    if locale.is_some_and(|l| l.to_ascii_lowercase().starts_with("fr")) {
        let from = device.map(|d| format!(" ({d})")).unwrap_or_default();
        (
            "Nouvelle connexion à ton compte".to_owned(),
            format!(
                "Ton compte vient d'être connecté depuis un nouvel appareil{from}.\n\nSi c'était toi, tu n'as rien \
                 à faire. Sinon, change ton mot de passe et déconnecte les autres appareils depuis l'app.\n"
            ),
        )
    } else {
        let from = device.map(|d| format!(" ({d})")).unwrap_or_default();
        (
            "New sign-in to your account".to_owned(),
            format!(
                "Your account was just signed in to from a new device{from}.\n\nIf it was you, there is nothing to \
                 do. If not, change your password and sign the other devices out from the app.\n"
            ),
        )
    }
}

/// The SMS text for `code` (short: one segment).
pub fn sms_message(code: &str, minutes: i64, locale: Option<&str>) -> String {
    if locale.is_some_and(|l| l.to_ascii_lowercase().starts_with("fr")) {
        format!("{code} est ton code. Il expire dans {minutes} min. Ne le partage avec personne.")
    } else {
        format!("{code} is your code. It expires in {minutes} min. Do not share it.")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sms_fits_one_segment() {
        for locale in [Some("fr"), None] {
            let text = sms_message("123456", 10, locale);
            assert!(text.starts_with("123456") && text.len() <= 160, "{text}");
        }
    }

    #[test]
    fn the_lockout_notice_follows_the_locale_and_carries_no_code() {
        let (subject, body) = lockout_notice_message(Some("fr"));
        assert!(subject.contains("pause") && body.contains("24 heures"));
        let (subject, body) = lockout_notice_message(None);
        assert!(subject.contains("paused") && body.contains("24 hours"));
    }

    #[test]
    fn the_message_follows_the_locale() {
        let (subject, body) = code_message("123456", 10, Some("fr-FR"));
        assert!(subject.contains("123456") && body.contains("10 minutes") && body.contains("Ton code"));
        let (subject, body) = code_message("123456", 10, None);
        assert!(subject.contains("is your code") && body.contains("Your code: 123456"));
    }

    #[test]
    fn the_contact_changed_notice_names_what_changed_in_the_holders_language() {
        let (subject, body) = contact_changed_notice_message(true, Some("fr-FR"));
        assert!(subject.contains("adresse e-mail") && body.contains("sécurise"), "{subject} / {body}");
        let (subject, _) = contact_changed_notice_message(false, None);
        assert_eq!(subject, "Your phone number was changed");
    }

    #[test]
    fn the_new_login_notice_names_the_device_when_known() {
        let (subject, body) = new_login_notice_message(Some("CorePlatform/1.4 iPhone15,3"), Some("fr"));
        assert_eq!(subject, "Nouvelle connexion à ton compte");
        assert!(body.contains("(CorePlatform/1.4 iPhone15,3)"), "{body}");
        let (_, body) = new_login_notice_message(None, None);
        assert!(body.contains("a new device.\n"), "{body}");
    }
}

