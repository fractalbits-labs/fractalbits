use crate::CmdResult;
use cmd_lib::*;
use std::path::Path;

const FIRESTORE_EMULATOR_VERSION: &str = "1.20.4";
const FIRESTORE_EMULATOR_BUILD: &str = "20260227192638";

/// Ensure the Firestore emulator JAR is available in third_party/.
pub fn ensure_firestore_emulator() -> CmdResult {
    let jar_path = resolve_firestore_jar();
    if Path::new(&jar_path).exists() {
        return Ok(());
    }

    let tarball_name =
        format!("google-cloud-sdk-cloud-firestore-emulator-{FIRESTORE_EMULATOR_BUILD}.tar.gz");
    let tarball_path = format!("third_party/{tarball_name}");

    if !Path::new(&tarball_path).exists() {
        let download_url =
            format!("https://dl.google.com/dl/cloudsdk/channels/rapid/components/{tarball_name}");
        run_cmd! {
            info "Downloading Firestore emulator v${FIRESTORE_EMULATOR_VERSION}...";
            mkdir -p third_party;
            curl -sL -o $tarball_path $download_url;
        }?;
    }

    let emulator_dir = resolve_firestore_dir();
    if !Path::new(&emulator_dir).exists() {
        run_cmd! {
            info "Extracting Firestore emulator...";
            mkdir -p $emulator_dir;
            tar -xzf $tarball_path -C $emulator_dir --strip-components=2;
        }?;
    }

    // Verify the JAR was extracted
    if !Path::new(&jar_path).exists() {
        return Err(std::io::Error::other(format!(
            "Firestore emulator JAR not found at {jar_path} after extraction"
        )));
    }

    Ok(())
}

/// Resolve the path to the Firestore emulator JAR.
pub fn resolve_firestore_jar() -> String {
    let pwd = run_fun!(pwd).unwrap_or_else(|_| ".".to_string());
    format!(
        "{pwd}/{}/cloud-firestore-emulator.jar",
        resolve_firestore_dir()
    )
}

/// Resolve the Firestore emulator directory.
fn resolve_firestore_dir() -> String {
    "third_party/cloud-firestore-emulator".to_string()
}
