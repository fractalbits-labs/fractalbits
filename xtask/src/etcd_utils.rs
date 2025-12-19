use crate::CmdResult;
use cmd_lib::*;
use std::path::Path;

const ETCD_VERSION: &str = "v3.6.7";
const ETCD_DIR: &str = "third_party/etcd";

pub fn ensure_etcd_local() -> CmdResult {
    let etcd_path = format!("{ETCD_DIR}/etcd");

    if run_cmd!(bash -c "command -v etcd" &>/dev/null).is_ok() || Path::new(&etcd_path).exists() {
        return Ok(());
    }

    download_etcd_for_arch("amd64")?;
    Ok(())
}

pub fn download_etcd_for_deploy() -> CmdResult {
    for arch in ["x86_64", "aarch64"] {
        let etcd_arch = if arch == "aarch64" { "arm64" } else { "amd64" };
        download_and_extract_to_deploy_dir(arch, etcd_arch)?;
    }
    Ok(())
}

fn download_etcd_for_arch(etcd_arch: &str) -> CmdResult {
    let etcd_tarball = format!("etcd-{ETCD_VERSION}-linux-{etcd_arch}.tar.gz");
    let tarball_path = format!("third_party/{etcd_tarball}");

    if !Path::new(&tarball_path).exists() {
        let download_url = format!(
            "https://github.com/etcd-io/etcd/releases/download/{ETCD_VERSION}/{etcd_tarball}"
        );
        run_cmd! {
            info "Downloading etcd binary for $etcd_arch...";
            mkdir -p third_party;
            curl -sL -o $tarball_path $download_url;
        }?;
    }

    let extracted_dir = format!("third_party/etcd-{ETCD_VERSION}-linux-{etcd_arch}");
    run_cmd! {
        info "Extracting etcd...";
        mkdir -p $ETCD_DIR;
        tar -xzf $tarball_path -C third_party;
        mv $extracted_dir/etcd $ETCD_DIR/;
        mv $extracted_dir/etcdctl $ETCD_DIR/;
        rm -rf $extracted_dir;
    }?;

    Ok(())
}

fn download_and_extract_to_deploy_dir(arch: &str, etcd_arch: &str) -> CmdResult {
    let etcd_tarball = format!("etcd-{ETCD_VERSION}-linux-{etcd_arch}.tar.gz");
    let tarball_path = format!("third_party/etcd/{etcd_tarball}");

    if !Path::new(&tarball_path).exists() {
        let download_url = format!(
            "https://github.com/etcd-io/etcd/releases/download/{ETCD_VERSION}/{etcd_tarball}"
        );
        run_cmd! {
            info "Downloading etcd binaries for $etcd_arch";
            mkdir -p third_party/etcd;
            curl -sL -o $tarball_path $download_url;
        }?;
    }

    let deploy_dir = format!("prebuilt/deploy/{arch}");
    let extract_dir = format!("etcd-{ETCD_VERSION}-linux-{etcd_arch}");
    run_cmd! {
        info "Extracting etcd binaries to $deploy_dir for $etcd_arch";
        tar -xzf $tarball_path -C third_party/etcd;
        cp third_party/etcd/$extract_dir/etcd $deploy_dir/etcd;
        cp third_party/etcd/$extract_dir/etcdctl $deploy_dir/etcdctl;
    }?;

    Ok(())
}

pub fn resolve_etcd_bin(binary_name: &str) -> String {
    if let Ok(path) = run_fun!(bash -c "command -v $binary_name") {
        return path;
    }

    let pwd = run_fun!(pwd).unwrap_or_else(|_| ".".to_string());
    format!("{pwd}/{ETCD_DIR}/{binary_name}")
}
