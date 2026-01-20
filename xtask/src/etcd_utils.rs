use crate::CmdResult;
use cmd_lib::*;
use std::path::Path;

const ETCD_VERSION: &str = "v3.6.7";

pub fn ensure_etcd_local() -> CmdResult {
    let arch = std::env::consts::ARCH;
    let etcd_arch = get_etcd_arch(arch);
    let etcd_path = resolve_etcd_bin("etcd");
    let etcdctl_path = resolve_etcd_bin("etcdctl");
    if Path::new(&etcd_path).exists() && Path::new(&etcdctl_path).exists() {
        return Ok(());
    }

    download_etcd_for_arch(etcd_arch)?;
    Ok(())
}

fn get_etcd_arch(cpu_arch: &str) -> &'static str {
    if cpu_arch == "aarch64" {
        "arm64"
    } else {
        "amd64"
    }
}

pub fn download_etcd_for_deploy() -> CmdResult {
    for arch in ["x86_64", "aarch64"] {
        let etcd_arch = get_etcd_arch(arch);
        download_and_extract_to_deploy_dir(arch, etcd_arch)?;
    }
    Ok(())
}

fn download_etcd_for_arch(etcd_arch: &str) -> CmdResult {
    let etcd_pkg = format!("etcd-{ETCD_VERSION}-linux-{etcd_arch}");
    let etcd_tarball = format!("{etcd_pkg}.tar.gz");

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

    let dir_path = format!("third_party/{etcd_pkg}");
    if !Path::new(&dir_path).exists() {
        run_cmd! {
            info "Extracting etcd...";
            tar -xzf $tarball_path -C third_party;
        }?;
    }

    Ok(())
}

fn download_and_extract_to_deploy_dir(arch: &str, etcd_arch: &str) -> CmdResult {
    download_etcd_for_arch(etcd_arch)?;
    let deploy_dir = format!("prebuilt/deploy/generic/{arch}");
    run_cmd! {
        info "Extracting etcd binaries to $deploy_dir for $etcd_arch";
        mkdir -p $deploy_dir;
        cp third_party/etcd-$ETCD_VERSION-linux-$etcd_arch/etcd $deploy_dir/etcd;
        cp third_party/etcd-$ETCD_VERSION-linux-$etcd_arch/etcdctl $deploy_dir/etcdctl;
    }?;

    Ok(())
}

pub fn resolve_etcd_bin(binary_name: &str) -> String {
    let arch = std::env::consts::ARCH;
    let etcd_arch = get_etcd_arch(arch);
    let pwd = run_fun!(pwd).unwrap_or_else(|_| ".".to_string());
    format!("{pwd}/third_party/etcd-{ETCD_VERSION}-linux-{etcd_arch}/{binary_name}")
}

/// Get the etcd directory for the host architecture
pub fn resolve_etcd_dir() -> String {
    let arch = std::env::consts::ARCH;
    resolve_etcd_dir_for_arch(arch)
}

/// Get the etcd directory for a specific architecture
pub fn resolve_etcd_dir_for_arch(arch: &str) -> String {
    let etcd_arch = get_etcd_arch(arch);
    format!("third_party/etcd-{ETCD_VERSION}-linux-{etcd_arch}")
}
