use crate::config::{BootstrapConfig, DeployTarget, JournalType};
use cmd_lib::*;
use std::io::Error;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;
use xtask_common::cloud_storage;

// Re-exports from target-specific modules so callers don't need to change imports
pub use crate::aws::{
    create_ena_irq_affinity_service, create_s3_express_bucket, ensure_ec2_metadata,
    get_current_aws_az_id, get_current_aws_region, get_s3_express_bucket_name,
};
pub use crate::etcd::get_etcd_endpoints;
pub use crate::gcp::firestore_put_document;

pub const BIN_PATH: &str = "/opt/fractalbits/bin/";
pub const ETC_PATH: &str = "/opt/fractalbits/etc/";
pub const GUI_WEB_ROOT: &str = "/opt/fractalbits/www/";
pub const API_SERVER_CONFIG: &str = "api_server_cloud_config.toml";
pub const BSS_SERVER_CONFIG: &str = "bss_server_cloud_config.toml";
pub const NSS_SERVER_CONFIG: &str = "nss_server_cloud_config.toml";
pub const MIRRORD_CONFIG: &str = "mirrord_cloud_config.toml";
pub const ROOT_SERVER_CONFIG: &str = "root_server_cloud_config.toml";
pub const NSS_ROLE_AGENT_CONFIG: &str = "nss_role_agent_cloud_config.toml";
pub const BENCH_SERVER_BENCH_START_SCRIPT: &str = "bench_start.sh";
pub const BOOTSTRAP_CLUSTER_CONFIG: &str = "bootstrap_cluster.toml";
pub const BOOTSTRAP_DONE_FILE: &str = "/opt/fractalbits/.bootstrap_done";
pub const DDB_SERVICE_DISCOVERY_TABLE: &str = "fractalbits-service-discovery";
pub const NETWORK_TUNING_SYS_CONFIG: &str = "99-network-tuning.conf";
pub const STORAGE_TUNING_SYS_CONFIG: &str = "99-storage-tuning.conf";

// DDB Service Discovery Keys
pub const BSS_DATA_VG_CONFIG_KEY: &str = "bss-data-vg-config";
pub const BSS_METADATA_VG_CONFIG_KEY: &str = "bss-metadata-vg-config";
pub const BSS_JOURNAL_VG_CONFIG_KEY: &str = "bss-journal-vg-config";
pub const BSS_SERVER_KEY: &str = "bss-server";
pub const AZ_STATUS_KEY: &str = "az_status";
#[allow(dead_code)]
pub const CLOUDWATCH_AGENT_CONFIG: &str = "cloudwatch_agent_config.json";
pub const S3EXPRESS_LOCAL_BUCKET_CONFIG: &str = "s3express-local-bucket-config.json";
pub const S3EXPRESS_REMOTE_BUCKET_CONFIG: &str = "s3express-remote-bucket-config.json";

/// Shared binaries that are not CPU-specific (stored directly under {arch}/)
const SHARED_BINARIES: &[&str] = &["fractalbits-bootstrap", "etcd", "etcdctl", "warp"];

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OsType {
    AmazonLinux,
    Ubuntu,
}

impl OsType {
    pub fn detect() -> Self {
        if let Ok(content) = std::fs::read_to_string("/etc/os-release")
            && (content.contains("Ubuntu") || content.contains("Debian"))
        {
            return OsType::Ubuntu;
        }
        OsType::AmazonLinux
    }
}

pub fn common_setup(target: DeployTarget) -> CmdResult {
    create_network_tuning_sysctl_file()?;
    create_storage_tuning_sysctl_file()?;
    let os = OsType::detect();
    let perf_pkg = match os {
        OsType::Ubuntu => "linux-tools-generic",
        OsType::AmazonLinux => "perf",
    };
    match target {
        DeployTarget::Aws => {
            crate::aws::install_cloudwatch_agent(os)?;
            install_packages(&[perf_pkg, "lldb"])?;
        }
        DeployTarget::OnPrem | DeployTarget::Gcp => {
            install_packages(&[perf_pkg, "lldb"])?;
        }
    }
    Ok(())
}

pub fn download_binaries(config: &BootstrapConfig, file_list: &[&str]) -> CmdResult {
    for file_name in file_list {
        download_binary(config, file_name)?;
    }
    Ok(())
}

fn download_binary(config: &BootstrapConfig, file_name: &str) -> CmdResult {
    let bootstrap_bucket = config.get_bootstrap_bucket();
    let cpu_arch = run_fun!(arch)?;

    // Determine cloud path based on deploy target and binary type:
    // - On-prem, GCP, or use_generic_binaries: {bucket}/{arch}/{binary}
    // - AWS shared binaries: {bucket}/{arch}/{binary}
    // - AWS CPU-specific binaries: {bucket}/{arch}/{cpu}/{binary}
    let cloud_path = if config.global.deploy_target == DeployTarget::OnPrem
        || config.global.deploy_target == DeployTarget::Gcp
        || config.global.use_generic_binaries
        || SHARED_BINARIES.contains(&file_name)
    {
        format!("{bootstrap_bucket}/{cpu_arch}/{file_name}")
    } else {
        let cpu_target = crate::aws::get_cpu_target()?;
        format!("{bootstrap_bucket}/{cpu_arch}/{cpu_target}/{file_name}")
    };

    let local_path = format!("{BIN_PATH}{file_name}");
    info!("Downloading from {cloud_path} to {local_path}");
    cloud_storage::download_file(&cloud_path, &local_path)?;
    run_cmd!(chmod +x $local_path)
}

pub fn backup_config_to_workflow(config: &BootstrapConfig, cluster_id: &str) -> CmdResult {
    let bucket = &config.bootstrap_bucket;
    let target = config.global.deploy_target;
    let local_path = format!("{ETC_PATH}{BOOTSTRAP_CLUSTER_CONFIG}");
    let key = format!("workflow/{cluster_id}/{BOOTSTRAP_CLUSTER_CONFIG}");

    // Check if already backed up (only first instance needs to do this)
    if cloud_storage::head_object(bucket, &key, target) {
        return Ok(());
    }

    let uri = cloud_storage::object_uri(bucket, &key, target);
    info!("Backing up bootstrap config to {uri}");
    cloud_storage::upload_file(&local_path, &uri)
}

pub fn create_systemd_unit_file(service_name: &str, enable_now: bool) -> CmdResult {
    create_systemd_unit_file_with_journal_type(service_name, enable_now, None)
}

pub fn create_systemd_unit_file_with_journal_type(
    service_name: &str,
    enable_now: bool,
    journal_type: Option<JournalType>,
) -> CmdResult {
    let working_dir = "/data";
    let mut requires = String::new();
    let mut env_settings = String::new();
    let mut managed_service = false;
    let mut scheduling = "";
    let instance_id = crate::aws::get_aws_instance_id().unwrap_or_else(|_| "unknown".to_string());
    let exec_start = match service_name {
        "api_server" => {
            env_settings = format!(
                r##"
Environment="RUST_LOG=info"
Environment="HOST_ID={instance_id}""##
            );
            scheduling = "CPUSchedulingPolicy=fifo
CPUSchedulingPriority=50
IOSchedulingClass=realtime
IOSchedulingPriority=0";
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{API_SERVER_CONFIG}")
        }
        "gui_server" => {
            env_settings = format!(
                r##"
Environment="RUST_LOG=info"
Environment="GUI_WEB_ROOT={GUI_WEB_ROOT}"
Environment="HOST_ID={instance_id}"
"##
            );
            format!("{BIN_PATH}api_server -c {ETC_PATH}{API_SERVER_CONFIG}")
        }
        "nss" => {
            managed_service = true;
            env_settings = format!(
                r##"
EnvironmentFile=-{ETC_PATH}nss.env"##
            );
            requires = match journal_type {
                Some(JournalType::Nvme) => "data-local.mount".to_string(),
                Some(JournalType::Ebs) => String::new(),
                None => unreachable!(),
            };
            format!(
                r#"/bin/bash -c 'if [ -n "$LOGS" ]; then {BIN_PATH}nss_server serve -c {ETC_PATH}{NSS_SERVER_CONFIG} 2>&1 | ts "[%%Y-%%m-%%d %%H:%%M:%%S]" >> "$LOGS/nss.log"; else exec {BIN_PATH}nss_server serve -c {ETC_PATH}{NSS_SERVER_CONFIG}; fi'"#
            )
        }
        "mirrord" => {
            managed_service = true;
            env_settings = format!(
                r##"
EnvironmentFile=-{ETC_PATH}mirrord.env"##
            );
            requires = match journal_type {
                Some(JournalType::Nvme) => "data-local.mount".to_string(),
                Some(JournalType::Ebs) => String::new(),
                None => unreachable!(),
            };
            format!(
                r#"/bin/bash -c 'if [ -n "$LOGS" ]; then {BIN_PATH}{service_name} -c {ETC_PATH}{MIRRORD_CONFIG} 2>&1 | ts "[%%Y-%%m-%%d %%H:%%M:%%S]" >> "$LOGS/mirrord.log"; else exec {BIN_PATH}{service_name} -c {ETC_PATH}{MIRRORD_CONFIG}; fi'"#
            )
        }
        "rss" => {
            env_settings = format!(
                r##"
Environment="RUST_LOG=info"
EnvironmentFile=-{ETC_PATH}rss.env"##
            );
            scheduling = "CPUSchedulingPolicy=fifo
CPUSchedulingPriority=50
IOSchedulingClass=realtime
IOSchedulingPriority=0";
            format!("{BIN_PATH}root_server -c {ETC_PATH}{ROOT_SERVER_CONFIG}")
        }
        "bss" => {
            requires = "data-local.mount".to_string();
            format!("{BIN_PATH}bss_server serve -c {ETC_PATH}{BSS_SERVER_CONFIG}")
        }
        "bench_client" => {
            format!("{BIN_PATH}warp client")
        }
        "nss_role_agent" => {
            env_settings = r##"
Environment="RUST_LOG=info""##
                .to_string();
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{NSS_ROLE_AGENT_CONFIG}")
        }
        _ => unreachable!(),
    };
    // Only bss runs on nodes with local NVMe disks; other services have no suitable
    // coredump destination, so skip LimitCORE=infinity for them.
    let coredump_enabled = service_name == "bss";
    let coredump_setting = if coredump_enabled {
        "LimitCORE=infinity\n"
    } else {
        ""
    };
    let (restart_settings, auto_restart) = if managed_service {
        ("", "")
    } else {
        (
            r##"# Limit to 3 restarts within a 10-minute (600 second) interval
StartLimitIntervalSec=600
StartLimitBurst=3
        "##,
            "Restart=on-failure\nRestartSec=5",
        )
    };
    let systemd_unit_content = format!(
        r##"[Unit]
Description={service_name} Service
After=network-online.target {requires}
Requires={requires}
BindsTo={requires}
{restart_settings}

[Service]
{scheduling}
{auto_restart}
LimitNOFILE=65536
{coredump_setting}WorkingDirectory={working_dir}{env_settings}
ExecStart={exec_start}

[Install]
WantedBy=multi-user.target
"##
    );

    let service_file = format!("{service_name}.service");
    let enable_now_opt = if enable_now { "--now" } else { "" };
    run_cmd! {
        mkdir -p /data;
        mkdir -p $ETC_PATH;
        echo $systemd_unit_content > ${ETC_PATH}${service_file};
    }?;

    run_cmd! {
        info "Enabling ${ETC_PATH}${service_file} (enable_now=${enable_now})";
        systemctl enable ${ETC_PATH}${service_file} --force --quiet ${enable_now_opt};
    }?;
    Ok(())
}

pub fn create_logrotate_for_stats() -> CmdResult {
    let file = "stats_logs";
    let rotate_config_content = r##"/data/local/stats/*.stats {
    size 50M
    rotate 10
    notifempty
    missingok
    nocreate
    copytruncate
}
"##;

    run_cmd! {
        info "Enabling stats log rotate";
        mkdir -p $ETC_PATH;
        echo $rotate_config_content > ${ETC_PATH}${file};
        ln -sf ${ETC_PATH}${file} /etc/logrotate.d;
    }?;

    Ok(())
}

pub fn get_instance_id(deploy_target: DeployTarget) -> FunResult {
    match deploy_target {
        DeployTarget::OnPrem => run_fun!(hostname),
        DeployTarget::Aws => crate::aws::get_aws_instance_id(),
        DeployTarget::Gcp => crate::gcp::get_gcp_instance_id(),
    }
}

pub fn get_private_ip(deploy_target: DeployTarget) -> FunResult {
    match deploy_target {
        DeployTarget::OnPrem => run_fun!(hostname -I | awk r"{print $1}"),
        DeployTarget::Aws => crate::aws::get_aws_private_ip(),
        DeployTarget::Gcp => crate::gcp::get_gcp_private_ip(),
    }
}

pub fn get_private_ip_from_config(config: &BootstrapConfig, instance_id: &str) -> FunResult {
    if let Some(instance_config) = config.get_instance(instance_id)
        && let Some(ip) = &instance_config.private_ip
    {
        return Ok(ip.clone());
    }
    get_private_ip(config.global.deploy_target)
}

// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/storage-twp.html
pub fn format_local_nvme_disks(support_storage_twp: bool) -> CmdResult {
    let nvme_disks = run_fun! {
        nvme list | grep -v "Amazon Elastic Block Store" | grep -v "nvme_card-pd"
            | awk r##"/nvme[0-9]n[0-9]/ {print $1}"##
    }?;
    let nvme_disks: &Vec<&str> = &nvme_disks.split("\n").collect();
    let num_nvme_disks = nvme_disks.len();
    if num_nvme_disks == 0 {
        cmd_die!("Could not find any nvme disks");
    }
    if support_storage_twp {
        assert_eq!(1, num_nvme_disks);
    }

    let mut fs_type = std::env::var("NVME_FS_TYPE").unwrap_or_else(|_| "ext4".to_string());
    if num_nvme_disks == 1 {
        if support_storage_twp {
            fs_type = "ext4".to_string();
        };

        match fs_type.as_str() {
            "ext4" => {
                run_cmd! {
                    info "Creating ext4 on local nvme disks: ${nvme_disks:?}";
                    mkfs.ext4 -F -I 128 -m 0 -i 8192 -J size=4096
                        -E lazy_itable_init=0,lazy_journal_init=0
                        -O dir_index,extent,flex_bg,fast_commit $[nvme_disks] &>/dev/null;
                }?;
            }
            "xfs" => {
                run_cmd! {
                    info "Creating XFS on local nvme disks: ${nvme_disks:?} with metadata optimizations";
                    mkfs.xfs -f -q -b size=8192 -d agcount=64
                        -i size=512,sparse=1
                        -l size=1024m,lazy-count=1
                        -n size=8192 $[nvme_disks];
                }?;
            }
            _ => {
                return Err(Error::other(format!(
                    "Unsupported filesystem type: {fs_type}"
                )));
            }
        }

        let uuid = run_fun!(blkid -s UUID -o value $[nvme_disks])?;
        create_mount_unit(
            &format!("/dev/disk/by-uuid/{uuid}"),
            DATA_LOCAL_MNT,
            &fs_type,
        )?;

        run_cmd! {
            info "Mounting $DATA_LOCAL_MNT via systemd with optimized options";
            mkdir -p $DATA_LOCAL_MNT;
            systemctl daemon-reload;
            systemctl start data-local.mount;
        }?;

        return Ok(());
    }

    const DATA_LOCAL_MNT: &str = "/data/local";

    run_cmd! {
        info "Zeroing superblocks";
        mdadm -q --zero-superblock $[nvme_disks];

        info "Creating md0";
        mdadm -q --create /dev/md0 --level=0 --raid-devices=${num_nvme_disks} $[nvme_disks];

        info "Updating /etc/mdadm/mdadm.conf";
        mkdir -p /etc/mdadm;
        mdadm --detail --scan > /etc/mdadm/mdadm.conf;
    }?;

    match fs_type.as_str() {
        "ext4" => {
            let stripe_width = num_nvme_disks * 128;
            run_cmd! {
                info "Creating ext4 on /dev/md0";
                mkfs.ext4 -F -I 128 -m 0 -i 8192 -J size=4096
                    -E lazy_itable_init=0,lazy_journal_init=0,stride=128,stripe_width=${stripe_width}
                    -O dir_index,extent,flex_bg,fast_commit /dev/md0 &>/dev/null;
            }?;
        }
        "xfs" => {
            run_cmd! {
                info "Creating XFS on /dev/md0 with metadata optimizations";
                mkfs.xfs -q -b size=8192 -d agcount=64,su=1m,sw=1
                    -i size=512,sparse=1
                    -l size=1024m,lazy-count=1
                    -n size=8192 /dev/md0;
            }?;
        }
        _ => {
            return Err(Error::other(format!(
                "Unsupported filesystem type: {fs_type}"
            )));
        }
    }

    run_cmd! {
        info "Creating mount point directory";
        mkdir -p $DATA_LOCAL_MNT;
    }?;

    let md0_uuid = run_fun!(blkid -s UUID -o value /dev/md0)?;
    create_mount_unit(
        &format!("/dev/disk/by-uuid/{md0_uuid}"),
        DATA_LOCAL_MNT,
        &fs_type,
    )?;

    run_cmd! {
        info "Mounting $DATA_LOCAL_MNT via systemd with optimized options";
        systemctl daemon-reload;
        systemctl start data-local.mount;
    }?;

    Ok(())
}

pub fn create_mount_unit(what: &str, mount_point: &str, fs_type: &str) -> CmdResult {
    let mount_options = match fs_type {
        "xfs" => {
            "defaults,nofail,noatime,nodiratime,lazytime,logbufs=8,logbsize=256k,allocsize=1m,largeio,inode64"
        }
        "ext4" => {
            "defaults,nofail,noatime,nodiratime,lazytime,nobarrier,data=ordered,journal_checksum,delalloc,dioread_nolock"
        }
        _ => "defaults,nofail",
    };

    let content = format!(
        r##"[Unit]
Description=Mount {what} at {mount_point}

[Mount]
What={what}
Where={mount_point}
Type={fs_type}
Options={mount_options}

[Install]
WantedBy=multi-user.target
"##
    );
    let mount_unit_name = mount_point.trim_start_matches('/').replace('/', "-");
    run_cmd! {
        info "Creating systemd unit ${mount_unit_name}.mount";
        mkdir -p $ETC_PATH;
        echo $content > ${ETC_PATH}${mount_unit_name}.mount;
        systemctl enable ${ETC_PATH}${mount_unit_name}.mount;
    }?;

    Ok(())
}

pub fn create_coredump_config() -> CmdResult {
    let cores_location = "/data/local/coredumps";
    let file = "99-coredump.conf";
    let content = format!("kernel.core_pattern={cores_location}/core.%e.%p.%t");
    run_cmd! {
        info "Setting up coredump location ($cores_location)";
        mkdir -p $cores_location;
        mkdir -p $ETC_PATH;
        echo $content > ${ETC_PATH}${file};
        ln -sf ${ETC_PATH}${file} /etc/sysctl.d;
        sysctl -p /etc/sysctl.d/${file} >/dev/null;
    }
}

pub fn install_packages(packages: &[&str]) -> CmdResult {
    let os = OsType::detect();
    run_cmd!(info "Installing ${packages:?}")?;
    match os {
        OsType::Ubuntu => {
            run_cmd! {
                apt-get update -qq 2>&1 >/dev/null;
                apt-get install -y -qq $[packages] >/dev/null;
            }?;
        }
        OsType::AmazonLinux => {
            run_cmd!(yum install -y -q $[packages] >/dev/null)?;
        }
    }
    Ok(())
}

pub fn register_service(config: &BootstrapConfig, service_id: &str) -> CmdResult {
    if config.is_etcd_backend() {
        crate::etcd::create_etcd_register_and_deregister_service(config, service_id)
    } else if config.is_firestore_backend() {
        crate::gcp::create_firestore_register_and_deregister_service(config, service_id)
    } else {
        crate::aws::create_ddb_register_and_deregister_service(service_id)
    }
}

pub fn get_service_ips_with_backend(
    config: &BootstrapConfig,
    service_id: &str,
    expected_count: usize,
) -> Vec<String> {
    if config.is_etcd_backend() {
        let endpoints = crate::etcd::get_etcd_endpoints(config).expect("etcd endpoints required");
        crate::etcd::get_service_ips_etcd(&endpoints, service_id, expected_count)
    } else if config.is_firestore_backend() {
        crate::gcp::get_service_ips_firestore(config, service_id, expected_count)
    } else {
        crate::aws::get_service_ips(service_id, expected_count)
    }
}

/// Fetch a single string value from service discovery by key.
/// Returns the raw value string (e.g. the VG config JSON).
pub fn get_service_discovery_value(
    config: &BootstrapConfig,
    key: &str,
) -> Result<String, std::io::Error> {
    if config.is_etcd_backend() {
        let endpoints = crate::etcd::get_etcd_endpoints(config).expect("etcd endpoints required");
        let etcdctl = format!("{BIN_PATH}etcdctl");
        let etcd_key = format!("/fractalbits-service-discovery/{key}");
        let value = run_fun! {
            $etcdctl --endpoints=$endpoints get $etcd_key --print-value-only
        }?;
        let value = value.trim().to_string();
        if value.is_empty() {
            return Err(std::io::Error::other(format!(
                "Service discovery key '{key}' not found in etcd"
            )));
        }
        Ok(value)
    } else if config.is_firestore_backend() {
        let gcp = config
            .gcp
            .as_ref()
            .ok_or_else(|| std::io::Error::other("GCP config required"))?;
        let project_id = &gcp.project_id;
        let database_id = gcp.firestore_database.as_deref().unwrap_or("fractalbits");
        let token = crate::gcp::get_gcp_access_token()?;
        let url = format!(
            "https://firestore.googleapis.com/v1/projects/{project_id}/databases/{database_id}/documents/{DDB_SERVICE_DISCOVERY_TABLE}/{key}"
        );
        let output = run_fun!(curl -sf $url -H "Authorization: Bearer $token")?;
        let parsed: serde_json::Value = serde_json::from_str(&output).map_err(|e| {
            std::io::Error::other(format!("Failed to parse Firestore response: {e}"))
        })?;
        parsed["fields"]["value"]["stringValue"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| {
                std::io::Error::other(format!(
                    "Service discovery key '{key}' not found in Firestore"
                ))
            })
    } else {
        let key_json = format!(r#"{{"service_id":{{"S":"{key}"}}}}"#);
        // "value" is a DDB reserved word, must use expression-attribute-names
        let expr_attr = r##"{"#v":"value"}"##;
        let output = run_fun! {
            aws dynamodb get-item
                --table-name $DDB_SERVICE_DISCOVERY_TABLE
                --key $key_json
                --projection-expression "#v"
                --expression-attribute-names $expr_attr
                --query "Item.value.S"
                --output text
        }?;
        let output = output.trim().to_string();
        if output.is_empty() || output == "None" || output == "null" {
            return Err(std::io::Error::other(format!(
                "Service discovery key '{key}' not found in DDB"
            )));
        }
        Ok(output)
    }
}

fn create_network_tuning_sysctl_file() -> CmdResult {
    let content = r##"# Should be a symlink file in /etc/sysctl.d
# allow TCP with buffers up to 128MB
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
# increase TCP autotuning buffer limits.
net.ipv4.tcp_rmem = 4096 87380 67108864
net.ipv4.tcp_wmem = 4096 65536 67108864
# recommended for hosts with jumbo frames enabled
net.ipv4.tcp_mtu_probing=1
# recommended to enable 'fair queueing'
net.core.default_qdisc = fq
"##;

    run_cmd! {
        info "Applying network tunning configs";
        mkdir -p $ETC_PATH;
        echo $content > $ETC_PATH/$NETWORK_TUNING_SYS_CONFIG;
        ln -nsf $ETC_PATH/$NETWORK_TUNING_SYS_CONFIG /etc/sysctl.d/;
        sysctl --system --quiet &> /dev/null;

    }?;
    Ok(())
}

fn create_storage_tuning_sysctl_file() -> CmdResult {
    let content = r##"# Should be a symlink file in /etc/sysctl.d
# VFS cache tuning for directory-heavy blob storage workloads
# Keep directory/inode caches longer (default: 100, lower = keep longer)
vm.vfs_cache_pressure = 10
# Start async writeback earlier for predictable write latency (default: 10)
vm.dirty_background_ratio = 5
# Limit max dirty pages to prevent large flush stalls (default: 20)
vm.dirty_ratio = 10
# Reduce swapping to keep more file cache in memory (default: 60)
vm.swappiness = 10
# Disable periodic dirty-inode (lazytime timestamp) writeback.
# With lazytime mount option, timestamps are only written to the journal when
# the inode is flushed for another reason (e.g. fsync, data write), eliminating
# journal commits that exist solely to record atime/mtime/ctime.
# Requires kernel >= 6.19 (https://lkml.org/lkml/2026/1/2/673).
#vm.dirtytime_expire_seconds = 0
"##;

    run_cmd! {
        info "Applying storage tuning configs";
        mkdir -p $ETC_PATH;
        echo $content > $ETC_PATH/$STORAGE_TUNING_SYS_CONFIG;
        ln -nsf $ETC_PATH/$STORAGE_TUNING_SYS_CONFIG /etc/sysctl.d/;
        sysctl --system --quiet &> /dev/null;

    }?;
    Ok(())
}

pub fn create_nvme_tuning_service() -> CmdResult {
    let script_path = format!("{BIN_PATH}tune-nvme-directio.sh");
    let systemd_unit_content = format!(
        r##"[Unit]
Description=NVMe Direct I/O Tuning
After=local-fs.target
Before=api_server.service bss.service nss.service mirrord.service bench_client.service

[Service]
Type=oneshot
ExecStart={script_path}

[Install]
WantedBy=multi-user.target
"##
    );

    let script_content = r##"#!/bin/bash

echo "Tuning NVMe devices for Direct I/O workloads" >&2

nvme_devices=$(nvme list | grep -v "Amazon Elastic Block Store" | grep -v "nvme_card-pd" \
    | awk '/nvme[0-9]n[0-9]/ {print $1}' \
    | sed 's|/dev/||' || true)

if [ -z "$nvme_devices" ]; then
    echo "No local NVMe devices found, skipping Direct I/O tuning" >&2
    exit 0
fi

echo "Found NVMe devices: $nvme_devices" >&2

for device in $nvme_devices; do
    if [ ! -d "/sys/block/$device" ]; then
        echo "Device $device not found in /sys/block, skipping" >&2
        continue
    fi

    echo "Tuning $device for Direct I/O workloads" >&2

    # I/O scheduler - set to none for Direct I/O
    echo none > /sys/block/$device/queue/scheduler 2>/dev/null || \
        echo "  Warning: Could not set scheduler for $device" >&2

    # Read-ahead - minimal for Direct I/O
    echo 64 > /sys/block/$device/queue/read_ahead_kb 2>/dev/null || \
        echo "  Warning: Could not set read_ahead_kb for $device" >&2

    # Rotational flag (read-only on most NVMe, skip if fails)
    echo 0 > /sys/block/$device/queue/rotational 2>/dev/null || true

    # Don't use block I/O for entropy
    echo 0 > /sys/block/$device/queue/add_random 2>/dev/null || \
        echo "  Warning: Could not disable add_random for $device" >&2

    # Complete I/O on same CPU socket (may not be supported on all kernels)
    echo 2 > /sys/block/$device/queue/rq_affinity 2>/dev/null || \
        echo "  Warning: Could not set rq_affinity for $device" >&2

    echo "Successfully tuned $device" >&2
done

echo "NVMe Direct I/O tuning completed" >&2
"##;

    run_cmd! {
        echo $script_content > $script_path;
        chmod +x $script_path;

        mkdir -p $ETC_PATH;
        echo $systemd_unit_content > ${ETC_PATH}nvme-directio-tuning.service;
        systemctl enable --now ${ETC_PATH}nvme-directio-tuning.service;
    }?;
    Ok(())
}

pub fn num_cpus() -> Result<u64, Error> {
    let num_cpus_str = run_fun!(nproc)?;
    let num_cpus = num_cpus_str
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::other(format!("invalid num_cores: {num_cpus_str}")))?;
    Ok(num_cpus)
}

pub fn setup_serial_console_password() -> CmdResult {
    let os = OsType::detect();
    let username = match os {
        OsType::Ubuntu => "ubuntu",
        OsType::AmazonLinux => "ec2-user",
    };
    run_cmd! {
        info "Setting password for $username to enable serial console access";
        echo "$username:fractalbits!" | chpasswd;
    }?;
    Ok(())
}

pub fn check_port_ready(host: &str, port: u16) -> bool {
    let addr = match (host, port).to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(addr) => addr,
            None => return false,
        },
        Err(_) => return false,
    };

    TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_ok()
}

pub fn wait_for_service_ready(service_name: &str, port: u16, timeout_secs: u64) -> CmdResult {
    info!("Waiting for {service_name} to be ready...");
    let mut wait_secs = 0;

    while !check_port_ready("localhost", port) {
        wait_secs += 1;
        if wait_secs % 10 == 0 {
            info!("{service_name} not yet ready, waiting... ({wait_secs}s)");
        }
        if wait_secs >= timeout_secs {
            return Err(Error::other(format!(
                "Timeout waiting for {service_name} to be ready ({timeout_secs}s)"
            )));
        }
        std::thread::sleep(Duration::from_secs(1));
    }

    info!("{service_name} is ready (port {port} responding)");
    Ok(())
}

pub fn ensure_aws_cli() -> CmdResult {
    if run_cmd!(bash -c "command -v aws" >/dev/null 2>&1).is_ok() {
        return Ok(());
    }

    run_cmd!(snap install aws-cli --classic)
}
