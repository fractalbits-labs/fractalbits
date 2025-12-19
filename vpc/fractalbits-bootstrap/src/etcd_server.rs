use crate::common::*;
use crate::config::EtcdConfig;
use cmd_lib::*;
use std::io::Error;

const ETCD_DATA_DIR: &str = "/data/local/etcd";
const ETCD_CLIENT_PORT: u16 = 2379;
const ETCD_PEER_PORT: u16 = 2380;
const ETCD_CONFIG_FILE: &str = "etcd.yaml";

pub fn bootstrap(etcd_config: &EtcdConfig) -> CmdResult {
    info!("Starting etcd bootstrap");

    download_etcd_binaries()?;

    let my_ip = get_private_ip()?;
    let member_name = get_my_member_name(&my_ip, &etcd_config.bss_ips)?;

    info!("Member name: {member_name}, IP: {my_ip}");

    let initial_cluster = generate_initial_cluster_config(&etcd_config.bss_ips);
    info!("Initial cluster: {initial_cluster}");

    create_etcd_data_dir()?;
    create_etcd_config_file(&member_name, &my_ip, &initial_cluster)?;
    create_etcd_systemd_service()?;

    info!("etcd bootstrap complete");
    Ok(())
}

fn download_etcd_binaries() -> CmdResult {
    info!("Downloading etcd binaries");
    download_binaries(&["etcd", "etcdctl"])
}

fn generate_initial_cluster_config(bss_ips: &[String]) -> String {
    let cluster_members: Vec<String> = bss_ips
        .iter()
        .enumerate()
        .map(|(i, ip)| format!("bss-{}=http://{}:{}", i + 1, ip, ETCD_PEER_PORT))
        .collect();

    cluster_members.join(",")
}

fn get_my_member_name(my_ip: &str, bss_ips: &[String]) -> Result<String, Error> {
    for (i, ip) in bss_ips.iter().enumerate() {
        if ip == my_ip {
            return Ok(format!("bss-{}", i + 1));
        }
    }
    Err(Error::other(format!(
        "My IP {} not found in BSS IPs: {:?}",
        my_ip, bss_ips
    )))
}

fn create_etcd_data_dir() -> CmdResult {
    info!("Creating etcd data directory: {ETCD_DATA_DIR}");
    run_cmd!(mkdir -p $ETCD_DATA_DIR)
}

fn create_etcd_config_file(member_name: &str, my_ip: &str, initial_cluster: &str) -> CmdResult {
    let config_path = format!("{ETC_PATH}{ETCD_CONFIG_FILE}");

    let config_content = format!(
        r##"name: {member_name}
data-dir: {ETCD_DATA_DIR}

listen-client-urls: http://0.0.0.0:{ETCD_CLIENT_PORT}
listen-peer-urls: http://0.0.0.0:{ETCD_PEER_PORT}

advertise-client-urls: http://{my_ip}:{ETCD_CLIENT_PORT}
initial-advertise-peer-urls: http://{my_ip}:{ETCD_PEER_PORT}

initial-cluster: {initial_cluster}
initial-cluster-state: new
initial-cluster-token: fractalbits-etcd-cluster

heartbeat-interval: 100
election-timeout: 1000
snapshot-count: 10000
max-snapshots: 5
max-wals: 5
"##
    );

    info!("Writing etcd config to {config_path}");
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $config_path;
    }?;

    Ok(())
}

fn create_etcd_systemd_service() -> CmdResult {
    let config_path = format!("{ETC_PATH}{ETCD_CONFIG_FILE}");
    let etcd_bin = format!("{BIN_PATH}etcd");

    let systemd_unit_content = format!(
        r##"[Unit]
Description=etcd distributed key-value store
After=network-online.target data-local.mount
Requires=data-local.mount

[Service]
Type=notify
ExecStart={etcd_bin} --config-file={config_path}
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

StartLimitIntervalSec=600
StartLimitBurst=3

[Install]
WantedBy=multi-user.target
"##
    );

    let service_file = "etcd.service";
    info!("Creating etcd systemd service");
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $systemd_unit_content > ${ETC_PATH}${service_file};
        systemctl enable ${ETC_PATH}${service_file} --force --quiet --now;
    }?;

    Ok(())
}
