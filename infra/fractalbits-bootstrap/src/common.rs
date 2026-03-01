use crate::config::{BootstrapConfig, DeployTarget, JournalType};
use cmd_lib::*;
use std::io::Error;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

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

/// Install amazon-ec2-utils on Ubuntu so that ec2-metadata is available early,
/// before common_setup() runs. On AL2023 it's pre-installed.
pub fn ensure_ec2_metadata() -> CmdResult {
    if OsType::detect() == OsType::Ubuntu {
        install_packages(&["amazon-ec2-utils"])?;
    }
    Ok(())
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
            install_cloudwatch_agent(os)?;
            install_packages(&[perf_pkg, "lldb"])?;
        }
        DeployTarget::OnPrem => {
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

    // Determine S3 path based on deploy target and binary type:
    // - On-prem or use_generic_binaries: all binaries at s3://bucket/{arch}/{binary}
    // - AWS shared binaries: s3://bucket/{arch}/{binary}
    // - AWS CPU-specific binaries: s3://bucket/{arch}/{cpu}/{binary}
    let s3_path = if config.global.deploy_target == DeployTarget::OnPrem
        || config.global.use_generic_binaries
        || SHARED_BINARIES.contains(&file_name)
    {
        format!("{bootstrap_bucket}/{cpu_arch}/{file_name}")
    } else {
        let cpu_target = get_cpu_target()?;
        format!("{bootstrap_bucket}/{cpu_arch}/{cpu_target}/{file_name}")
    };

    let local_path = format!("{BIN_PATH}{file_name}");
    download_from_s3(&s3_path, &local_path)?;
    run_cmd!(chmod +x $local_path)
}

pub fn get_ec2_instance_type() -> FunResult {
    run_fun!(ec2-metadata --instance-type | awk r"{print $2}")
}

/// Get the CPU target based on EC2 instance type.
/// Maps instance families to their optimal CPU targets for binary downloads.
pub fn get_cpu_target_from_instance_type(instance_type: &str) -> &'static str {
    let family = instance_type.split('.').next().unwrap_or("");

    match family {
        // x86_64 instance families
        "i3" => "broadwell",
        "i3en" => "skylake",
        // aarch64 - Graviton3 (7th gen)
        "c7g" | "m7g" | "r7g" | "c7gn" | "c7gd" | "m7gd" | "r7gd" => "neoverse-n1",
        // aarch64 - Graviton4 (8th gen)
        "c8g" | "m8g" | "r8g" | "x8g" | "i8g" | "im8g" => "neoverse-n2",
        _ => {
            cmd_die!("Unknown instance type: ${instance_type}, cannot determine CPU target")
        }
    }
}

/// Get the CPU target by detecting from EC2 instance type.
fn get_cpu_target() -> FunResult {
    let instance_type = get_ec2_instance_type()?;
    Ok(get_cpu_target_from_instance_type(&instance_type).to_string())
}

pub fn download_from_s3(s3_path: &str, local_path: &str) -> CmdResult {
    run_cmd!(
        info "Downloading from $s3_path to $local_path";
        aws s3 cp --no-progress $s3_path $local_path
    )
}

pub fn backup_config_to_workflow(config: &BootstrapConfig, cluster_id: &str) -> CmdResult {
    let bucket = config.get_bootstrap_bucket();
    let local_path = format!("{ETC_PATH}{BOOTSTRAP_CLUSTER_CONFIG}");
    let workflow_path = format!("{bucket}/workflow/{cluster_id}/{BOOTSTRAP_CLUSTER_CONFIG}");

    // Check if already backed up (only first instance needs to do this)
    let exists = run_fun!(aws s3 ls $workflow_path 2>/dev/null);
    if exists.is_ok() && !exists.unwrap().trim().is_empty() {
        return Ok(());
    }

    info!("Backing up bootstrap config to {workflow_path}");
    run_cmd!(aws s3 cp $local_path $workflow_path --quiet)?;
    Ok(())
}

pub fn create_systemd_unit_file(service_name: &str, enable_now: bool) -> CmdResult {
    create_systemd_unit_file_with_journal_type(service_name, enable_now, None, None)
}

pub fn create_systemd_unit_file_with_journal_type(
    service_name: &str,
    enable_now: bool,
    journal_type: Option<JournalType>,
    journal_uuid: Option<&str>,
) -> CmdResult {
    let working_dir = "/data";
    let mut requires = String::new();
    let mut env_settings = String::new();
    let mut managed_service = false;
    let mut scheduling = "";
    let instance_id = get_instance_id().unwrap_or_else(|_| "unknown".to_string());
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
            requires = match (journal_type, journal_uuid) {
                (Some(_), _) => "data-local.mount".to_string(),
                (None, _) => unreachable!(),
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
            requires = match (journal_type, journal_uuid) {
                (Some(_), _) => "data-local.mount".to_string(),
                (None, _) => unreachable!(),
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
            format!("{BIN_PATH}bss_server -c {ETC_PATH}{BSS_SERVER_CONFIG}")
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
LimitCORE=infinity
WorkingDirectory={working_dir}{env_settings}
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

// It could be replaced with `ec2-metadata --region`,
// however, which seemed not working on ubuntu (version 0.1.2)
pub fn get_current_aws_region() -> FunResult {
    let token = run_fun! {
        curl -X PUT "http://169.254.169.254/latest/api/token"
            -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" -s
    }?;
    run_fun! {
        curl -H "X-aws-ec2-metadata-token: $token"
            "http://169.254.169.254/latest/meta-data/placement/region" -s
    }
}

pub fn get_ec2_tag(instance_id: &str, region: &str, tag_name: &str) -> FunResult {
    let tag_value = run_fun!(
        aws ec2 describe-tags
            --region $region
            --filters "Name=resource-id,Values=$instance_id" "Name=key,Values=$tag_name"
            --query "Tags[0].Value" --output text
    )?;

    if tag_value.is_empty() || tag_value == "None" {
        return Err(Error::other(format!(
            "EC2 tag '{tag_name}' not found for instance {instance_id}"
        )));
    }
    Ok(tag_value)
}

pub fn get_instance_id() -> FunResult {
    run_fun!(ec2-metadata --instance-id | awk r"{print $2}")
}

pub fn get_private_ip() -> FunResult {
    run_fun!(ec2-metadata --local-ipv4 | awk r"{print $2}")
}

pub fn get_instance_id_from_config(config: &BootstrapConfig) -> FunResult {
    match config.global.deploy_target {
        DeployTarget::OnPrem => run_fun!(hostname),
        DeployTarget::Aws => get_instance_id(),
    }
}

pub fn get_private_ip_from_config(config: &BootstrapConfig, instance_id: &str) -> FunResult {
    if let Some(instance_config) = config.get_instance(instance_id)
        && let Some(ip) = &instance_config.private_ip
    {
        return Ok(ip.clone());
    }
    match config.global.deploy_target {
        DeployTarget::OnPrem => run_fun!(hostname -I | awk r"{print $1}"),
        DeployTarget::Aws => get_private_ip(),
    }
}

pub fn get_s3_express_bucket_name(az: &str) -> FunResult {
    // Generate bucket name in format: fractalbits-data-{account}--{az}--x-s3
    let account = get_account_id()?;
    Ok(format!("fractalbits-data-{account}--{az}--x-s3"))
}

pub fn get_current_aws_az_id() -> FunResult {
    let token = run_fun! {
        curl -X PUT "http://169.254.169.254/latest/api/token"
            -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" -s
    }?;
    run_fun! {
        curl -H "X-aws-ec2-metadata-token: $token"
            "http://169.254.169.254/latest/meta-data/placement/availability-zone-id"
    }
}

// Get AWS account ID using IMDS (Instance Metadata Service)
pub fn get_account_id() -> FunResult {
    let token = run_fun! {
        curl -X PUT "http://169.254.169.254/latest/api/token"
            -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" -s
    }?;
    let account = run_fun! {
        curl -H "X-aws-ec2-metadata-token: $token"
            "http://169.254.169.254/latest/dynamic/instance-identity/document" -s
        | grep accountId
        | sed r#"s/.*"accountId" : "\([0-9]*\)".*/\1/"#
    }?;
    if account.is_empty() {
        return Err(std::io::Error::other(
            "Failed to extract account ID from IMDS",
        ));
    }
    Ok(account)
}

pub fn create_s3_express_bucket(az: &str, config_file_name: &str) -> CmdResult {
    let bucket_name = get_s3_express_bucket_name(az)?;

    // Check if bucket already exists
    info!("Checking if S3 Express bucket {bucket_name} already exists...");
    let bucket_exists = run_cmd! {
        aws s3api head-bucket --bucket $bucket_name 2>/dev/null
    }
    .is_ok();

    if bucket_exists {
        info!("S3 Express bucket {bucket_name} already exists, skipping creation");
        return Ok(());
    }

    // Generate S3 Express bucket configuration JSON
    let config_content = format!(
        r#"{{
  "Location": {{
    "Type": "AvailabilityZone",
    "Name": "{az}"
  }},
  "Bucket": {{
    "DataRedundancy": "SingleAvailabilityZone",
    "Type": "Directory"
  }}
}}"#
    );

    let config_file_path = format!("{ETC_PATH}{config_file_name}");
    run_cmd! {
        echo $config_content > $config_file_path
    }?;

    // Create the S3 Express bucket
    info!("Creating S3 Express bucket: {bucket_name} in AZ: {az}");
    let config_file_opt = format!("file://{config_file_path}");
    run_cmd! {
        aws s3api create-bucket
            --bucket $bucket_name
            --create-bucket-configuration $config_file_opt
    }?;

    info!("S3 Express bucket ready: {bucket_name}");
    Ok(())
}

// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/storage-twp.html
pub fn format_local_nvme_disks(support_storage_twp: bool) -> CmdResult {
    let nvme_disks = run_fun! {
        nvme list | grep -v "Amazon Elastic Block Store"
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
            "defaults,nofail,noatime,nodiratime,logbufs=8,logbsize=256k,allocsize=1m,largeio,inode64"
        }
        "ext4" => {
            "defaults,nofail,noatime,nodiratime,nobarrier,data=ordered,journal_checksum,delalloc,dioread_nolock"
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

fn install_cloudwatch_agent(os: OsType) -> CmdResult {
    match os {
        OsType::AmazonLinux => {
            run_cmd!(yum install -y -q amazon-cloudwatch-agent >/dev/null)?;
        }
        OsType::Ubuntu => {
            let cpu_arch = run_fun!(arch)?;
            let deb_arch = match cpu_arch.as_str() {
                "aarch64" => "arm64",
                _ => "amd64",
            };
            let url = format!(
                "https://amazoncloudwatch-agent.s3.amazonaws.com/ubuntu/{deb_arch}/latest/amazon-cloudwatch-agent.deb"
            );
            run_cmd! {
                info "Downloading CloudWatch agent for Ubuntu";
                curl -sL -o /tmp/amazon-cloudwatch-agent.deb $url;
                dpkg -i /tmp/amazon-cloudwatch-agent.deb >/dev/null;
                rm -f /tmp/amazon-cloudwatch-agent.deb;
            }?;
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn create_cloudwatch_agent_config() -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let content = format!(
        r##"{{
  "agent": {{
    "region": "{aws_region}",
    "run_as_user": "root",
    "debug": false
  }},
  "metrics": {{
    "namespace": "Vpc/Fractalbits",
    "metrics_collected": {{
      "statsd": {{
        "metrics_aggregation_interval": 60,
        "metrics_collection_interval": 10,
        "service_address": ":8125"
      }},
      "cpu": {{
        "measurement": [
          "cpu_usage_idle"
        ],
        "metrics_collection_interval": 60,
        "resources": [
          "*"
        ],
        "totalcpu": true
      }}
    }}
  }}
}}"##
    );

    run_cmd! {
        echo $content > $ETC_PATH/$CLOUDWATCH_AGENT_CONFIG;
    }?;

    Ok(())
}

#[allow(dead_code)]
pub fn setup_cloudwatch_agent() -> CmdResult {
    create_cloudwatch_agent_config()?;

    run_cmd! {
        info "Creating CloudWatch agent configuration files";
        /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl
            -a fetch-config -m ec2 -c file:$ETC_PATH/$CLOUDWATCH_AGENT_CONFIG;
        info "Enabling Cloudwatch agent service";
        systemctl enable --now amazon-cloudwatch-agent;
    }?;

    Ok(())
}

pub fn create_ddb_register_and_deregister_service(service_id: &str) -> CmdResult {
    create_ddb_register_service(service_id)?;
    create_ddb_deregister_service(service_id)?;
    Ok(())
}

fn create_ddb_register_service(service_id: &str) -> CmdResult {
    let ddb_register_script = format!("{BIN_PATH}ddb-register.sh");
    let systemd_unit_content = format!(
        r##"[Unit]
Description=DynamoDB Service Registration
After=network-online.target

[Service]
Type=oneshot
ExecStart={ddb_register_script}

[Install]
WantedBy=multi-user.target
"##
    );

    let register_script_content = format!(
        r##"#!/bin/bash
set -e
service_id={service_id}
instance_id=$(ec2-metadata -i | awk '{{print $2}}')
private_ip=$(ec2-metadata -o | awk '{{print $2}}')

echo "Registering itself ($instance_id,$private_ip) to ddb table {DDB_SERVICE_DISCOVERY_TABLE} with service_id $service_id" >&2

# Retry mechanism with exponential backoff to handle race conditions
MAX_RETRIES=10
retry_count=0
success=false

while [ $retry_count -lt $MAX_RETRIES ] && [ "$success" = "false" ]; do
    retry_count=$((retry_count + 1))

    # Try to update existing item first
    if aws dynamodb update-item \
        --table-name {DDB_SERVICE_DISCOVERY_TABLE} \
        --key "{{\"service_id\": {{ \"S\": \"$service_id\"}}}} " \
        --update-expression "SET #instances.#instance_id = :ip" \
        --expression-attribute-names "{{\"#instances\": \"instances\", \"#instance_id\": \"$instance_id\"}}" \
        --expression-attribute-values "{{\":ip\": {{ \"S\": \"$private_ip\"}}}}" \
        --condition-expression "attribute_exists(service_id)" 2>/dev/null; then
        echo "Updated existing service entry on attempt $retry_count" >&2
        success=true
    else
        # Try to create new item if update failed
        echo "Attempting to create new service entry (attempt $retry_count)" >&2
        if aws dynamodb put-item \
            --table-name {DDB_SERVICE_DISCOVERY_TABLE} \
            --item "{{\"service_id\": {{\"S\": \"$service_id\"}}, \"instances\": {{\"M\": {{\"$instance_id\": {{\"S\": \"$private_ip\"}}}}}}}}" \
            --condition-expression "attribute_not_exists(service_id)" 2>/dev/null; then
            echo "Created new service entry on attempt $retry_count" >&2
            success=true
        else
            echo "Both update and create failed on attempt $retry_count, retrying..." >&2
            # Exponential backoff with jitter
            sleep_time=$((retry_count + RANDOM % 3))
            sleep $sleep_time
        fi
    fi
done

if [ "$success" = "false" ]; then
    echo "FATAL: Failed to register service $service_id after $MAX_RETRIES attempts" >&2
    exit 1
fi

echo "Done" >&2
"##
    );

    run_cmd! {
        echo $register_script_content > $ddb_register_script;
        chmod +x $ddb_register_script;

        echo $systemd_unit_content > ${ETC_PATH}ddb-register.service;
        systemctl enable --now ${ETC_PATH}ddb-register.service;
    }?;
    Ok(())
}

fn create_ddb_deregister_service(service_id: &str) -> CmdResult {
    let ddb_deregister_script = format!("{BIN_PATH}ddb-deregister.sh");
    let systemd_unit_content = format!(
        r##"[Unit]
Description=DynamoDB Service Deregistration
After=network-online.target
Before=reboot.target halt.target poweroff.target kexec.target

DefaultDependencies=no

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart={ddb_deregister_script}

[Install]
WantedBy=reboot.target halt.target poweroff.target kexec.target
"##
    );

    let deregister_script_content = format!(
        r##"#!/bin/bash
set -e
service_id={service_id}
instance_id=$(ec2-metadata -i | awk '{{print $2}}')
private_ip=$(ec2-metadata -o | awk '{{print $2}}')

echo "Deregistering itself ($instance_id, $private_ip) from ddb table {DDB_SERVICE_DISCOVERY_TABLE} with service_id $service_id" >&2
aws dynamodb update-item \
    --table-name {DDB_SERVICE_DISCOVERY_TABLE} \
    --key "{{\"service_id\": {{ \"S\": \"$service_id\"}}}} " \
    --update-expression "REMOVE instances.#instance_id" \
    --expression-attribute-names "{{\"#instance_id\": \"$instance_id\"}}"
echo "Done" >&2
"##
    );

    run_cmd! {
        echo $deregister_script_content > $ddb_deregister_script;
        chmod +x $ddb_deregister_script;

        echo $systemd_unit_content > ${ETC_PATH}ddb-deregister.service;
        systemctl enable ${ETC_PATH}ddb-deregister.service;
    }?;
    Ok(())
}

pub fn get_service_ips(service_id: &str, expected_min_count: usize) -> Vec<String> {
    info!("Waiting for {expected_min_count} {service_id} service(s)");
    let start_time = Instant::now();
    let timeout = Duration::from_secs(300);
    loop {
        if start_time.elapsed() > timeout {
            cmd_die!("Timeout waiting for ${service_id} service(s)");
        }
        let key = format!(r#"{{"service_id":{{"S":"{service_id}"}}}}"#);
        let res = run_fun! {
             aws dynamodb get-item
                 --table-name ${DDB_SERVICE_DISCOVERY_TABLE}
                 --key $key
                 --projection-expression "instances"
                 --query "Item.instances.M"
                 --output json
        };
        match res {
            Ok(output) if !output.is_empty() && output != "None" && output != "null" => {
                let instances: serde_json::Value =
                    serde_json::from_str(&output).unwrap_or(serde_json::json!({}));
                let mut ips = Vec::new();
                if let Some(obj) = instances.as_object() {
                    for (_instance_id, value) in obj {
                        if let Some(ip_obj) = value.get("S")
                            && let Some(ip) = ip_obj.as_str()
                        {
                            ips.push(ip.to_string());
                        }
                    }
                }
                if ips.len() >= expected_min_count {
                    info!("Found a list of {service_id} services: {ips:?}");
                    return ips;
                }
            }
            _ => std::thread::sleep(std::time::Duration::from_secs(1)),
        }
    }
}

pub fn get_etcd_endpoints(config: &BootstrapConfig) -> FunResult {
    // First try static endpoints from config (for on-prem/static etcd)
    if let Some(etcd_config) = &config.etcd
        && let Some(endpoints) = &etcd_config.endpoints
    {
        return Ok(endpoints.clone());
    }

    // Fall back to workflow barrier discovery (for dynamic BSS etcd cluster)
    let cluster_id = config
        .global
        .workflow_cluster_id
        .as_ref()
        .ok_or_else(|| Error::other("workflow_cluster_id not configured"))?;
    let bucket = &config.bootstrap_bucket;
    let instance_id = match config.global.deploy_target {
        DeployTarget::OnPrem => run_fun!(hostname)?,
        DeployTarget::Aws => get_instance_id()?,
    };

    let barrier =
        crate::workflow::WorkflowBarrier::new(bucket, cluster_id, &instance_id, "bss_server");
    let bss_nodes = barrier.get_etcd_nodes()?;

    if bss_nodes.is_empty() {
        return Err(Error::other(
            "No BSS nodes registered in workflow for etcd endpoints",
        ));
    }

    Ok(bss_nodes
        .iter()
        .map(|node| format!("http://{}:2379", node.ip))
        .collect::<Vec<_>>()
        .join(","))
}

pub fn create_etcd_register_and_deregister_service(
    config: &BootstrapConfig,
    service_id: &str,
) -> CmdResult {
    let endpoints = get_etcd_endpoints(config)?;
    create_etcd_register_service(config, &endpoints, service_id)?;
    create_etcd_deregister_service(config, &endpoints, service_id)?;
    Ok(())
}

fn create_etcd_register_service(
    config: &BootstrapConfig,
    endpoints: &str,
    service_id: &str,
) -> CmdResult {
    let etcd_register_script = format!("{BIN_PATH}etcd-register.sh");

    let get_instance_id_cmd = match config.global.deploy_target {
        DeployTarget::OnPrem => "hostname".to_string(),
        DeployTarget::Aws => "ec2-metadata -i | awk '{print $2}'".to_string(),
    };

    let get_private_ip_cmd = match config.global.deploy_target {
        DeployTarget::OnPrem => "hostname -I | awk '{print $1}'".to_string(),
        DeployTarget::Aws => "ec2-metadata -o | awk '{print $2}'".to_string(),
    };

    let systemd_unit_content = format!(
        r##"[Unit]
Description=etcd Service Registration
After=network-online.target etcd.service

[Service]
Type=oneshot
ExecStart={etcd_register_script}

[Install]
WantedBy=multi-user.target
"##
    );

    // Use individual keys per instance: /fractalbits-service-discovery/<service_id>/<instance_id> -> <ip>
    // This allows simple put/del operations and prefix scanning for discovery
    let register_script_content = format!(
        r###"#!/bin/bash
set -e
service_id={service_id}
endpoints="{endpoints}"
instance_id=$({get_instance_id_cmd})
private_ip=$({get_private_ip_cmd})
etcd_key="/fractalbits-service-discovery/$service_id/$instance_id"

echo "Registering itself ($instance_id,$private_ip) to etcd at $etcd_key" >&2

MAX_RETRIES=30
retry_count=0
success=false

while [ $retry_count -lt $MAX_RETRIES ] && [ "$success" = "false" ]; do
    retry_count=$((retry_count + 1))

    if {BIN_PATH}etcdctl --endpoints="$endpoints" put "$etcd_key" "$private_ip" 2>/dev/null; then
        echo "Registered service on attempt $retry_count" >&2
        success=true
    else
        echo "Registration failed on attempt $retry_count, retrying..." >&2
        sleep 2
    fi
done

if [ "$success" = "false" ]; then
    echo "FATAL: Failed to register service $service_id after $MAX_RETRIES attempts" >&2
    exit 1
fi

echo "Done" >&2
"###
    );

    run_cmd! {
        echo $register_script_content > $etcd_register_script;
        chmod +x $etcd_register_script;

        echo $systemd_unit_content > ${ETC_PATH}etcd-register.service;
        systemctl enable --now ${ETC_PATH}etcd-register.service;
    }?;
    Ok(())
}

fn create_etcd_deregister_service(
    config: &BootstrapConfig,
    endpoints: &str,
    service_id: &str,
) -> CmdResult {
    let etcd_deregister_script = format!("{BIN_PATH}etcd-deregister.sh");

    let get_instance_id_cmd = match config.global.deploy_target {
        DeployTarget::OnPrem => "hostname".to_string(),
        DeployTarget::Aws => "ec2-metadata -i | awk '{print $2}'".to_string(),
    };

    let systemd_unit_content = format!(
        r##"[Unit]
Description=etcd Service Deregistration
After=network-online.target
Before=reboot.target halt.target poweroff.target kexec.target

DefaultDependencies=no

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart={etcd_deregister_script}

[Install]
WantedBy=reboot.target halt.target poweroff.target kexec.target
"##
    );

    // Use individual keys per instance - just delete the key
    let deregister_script_content = format!(
        r###"#!/bin/bash
set -e
service_id={service_id}
endpoints="{endpoints}"
instance_id=$({get_instance_id_cmd})
etcd_key="/fractalbits-service-discovery/$service_id/$instance_id"

echo "Deregistering itself ($instance_id) from etcd at $etcd_key" >&2
{BIN_PATH}etcdctl --endpoints="$endpoints" del "$etcd_key" 2>/dev/null || true
echo "Done" >&2
"###
    );

    run_cmd! {
        echo $deregister_script_content > $etcd_deregister_script;
        chmod +x $etcd_deregister_script;

        echo $systemd_unit_content > ${ETC_PATH}etcd-deregister.service;
        systemctl enable ${ETC_PATH}etcd-deregister.service;
    }?;
    Ok(())
}

pub fn get_service_ips_etcd(
    endpoints: &str,
    service_id: &str,
    expected_min_count: usize,
) -> Vec<String> {
    info!("Waiting for {expected_min_count} {service_id} service(s) via etcd");
    let start_time = Instant::now();
    let timeout = Duration::from_secs(300);
    let etcdctl = format!("{BIN_PATH}etcdctl");
    // Use prefix scan: /fractalbits-service-discovery/<service_id>/<instance_id> -> <ip>
    let etcd_prefix = format!("/fractalbits-service-discovery/{service_id}/");
    loop {
        if start_time.elapsed() > timeout {
            cmd_die!("Timeout waiting for ${service_id} service(s) via etcd");
        }

        let res: Result<String, _> = run_fun! {
            $etcdctl --endpoints=$endpoints get $etcd_prefix --prefix --print-value-only 2>/dev/null
        };

        match res {
            Ok(ref output) if !output.trim().is_empty() => {
                // Each line is an IP address
                let ips: Vec<String> = output
                    .lines()
                    .filter(|line| !line.is_empty())
                    .map(|s| s.to_string())
                    .collect();

                if ips.len() >= expected_min_count {
                    info!("Found a list of {service_id} services via etcd: {ips:?}");
                    return ips;
                }
                info!(
                    "Found {} of {} {service_id} services, waiting...",
                    ips.len(),
                    expected_min_count
                );
            }
            _ => {}
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
}

pub fn register_service(config: &BootstrapConfig, service_id: &str) -> CmdResult {
    if config.is_etcd_backend() {
        create_etcd_register_and_deregister_service(config, service_id)
    } else {
        create_ddb_register_and_deregister_service(service_id)
    }
}

pub fn get_service_ips_with_backend(
    config: &BootstrapConfig,
    service_id: &str,
    expected_count: usize,
) -> Vec<String> {
    if config.is_etcd_backend() {
        let endpoints = get_etcd_endpoints(config).expect("etcd endpoints required");
        get_service_ips_etcd(&endpoints, service_id, expected_count)
    } else {
        get_service_ips(service_id, expected_count)
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

nvme_devices=$(nvme list | grep -v "Amazon Elastic Block Store" \
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

pub fn create_ena_irq_affinity_service() -> CmdResult {
    let script_path = format!("{BIN_PATH}configure-ena-irq-affinity.sh");
    let systemd_unit_content = format!(
        r##"[Unit]
Description=ENA IRQ Affinity Configuration
After=network-online.target
Before=api_server.service bss.service nss.service mirrord.service bench_client.service

[Service]
Type=oneshot
ExecStart={script_path}

[Install]
WantedBy=multi-user.target
"##
    );

    let script_content = r##"#!/bin/bash
set -e

echo "Configuring ENA interrupt affinity" >&2

echo "Disabling irqbalance" >&2
systemctl disable --now irqbalance 2>/dev/null || true

iface=$(grep -o "ens[0-9]*-Tx-Rx" /proc/interrupts | head -1 | sed "s/-Tx-Rx//")
if [ -z "$iface" ]; then
    echo "ERROR: Could not detect ENA interface" >&2
    exit 1
fi
echo "Detected ENA interface: $iface" >&2

num_queues=$(grep "$iface-Tx-Rx-" /proc/interrupts | wc -l)
if [ "$num_queues" -eq 0 ]; then
    echo "ERROR: Could not detect ENA queues" >&2
    exit 1
fi
echo "Found $num_queues queues" >&2

num_cpus=$(nproc)
echo "System has $num_cpus CPUs" >&2

echo "Attempting to configure RSS" >&2
if ethtool -X $iface equal $num_cpus 2>&1; then
    echo "RSS configured successfully" >&2
else
    echo "RSS configuration failed or not supported (using hardware default)" >&2
fi

cpus_per_queue=$((num_cpus / num_queues))
echo "Spreading $num_queues queues across $num_cpus CPUs ($cpus_per_queue CPUs per queue)" >&2

for queue in $(seq 0 $((num_queues - 1))); do
    irq=$(grep "$iface-Tx-Rx-$queue" /proc/interrupts | awk -F: '{print $1}' | tr -d ' ')

    if [ -n "$irq" ]; then
        start_cpu=$((queue * cpus_per_queue))
        end_cpu=$(((queue + 1) * cpus_per_queue))
        if [ $end_cpu -gt $num_cpus ]; then
            end_cpu=$num_cpus
        fi

        mask_low=0
        mask_high=0
        for cpu in $(seq $start_cpu $((end_cpu - 1))); do
            if [ $cpu -lt 32 ]; then
                mask_low=$((mask_low | (1 << cpu)))
            else
                mask_high=$((mask_high | (1 << (cpu - 32))))
            fi
        done

        if [ $mask_high -gt 0 ]; then
            mask_str=$(printf "%08x,%08x" $mask_high $mask_low)
        else
            mask_str=$(printf "%x" $mask_low)
        fi

        echo $mask_str > /proc/irq/$irq/smp_affinity
        echo "IRQ $irq ($iface-Tx-Rx-$queue) -> CPUs $start_cpu-$end_cpu (mask: $mask_str)" >&2

        xps_path="/sys/class/net/$iface/queues/tx-$queue/xps_cpus"
        echo $mask_str > $xps_path 2>/dev/null || true
    fi
done

echo 32768 > /proc/sys/net/core/rps_sock_flow_entries || true
for queue in $(seq 0 $((num_queues - 1))); do
    rps_path="/sys/class/net/$iface/queues/rx-$queue/rps_flow_cnt"
    echo 4096 > $rps_path 2>/dev/null || true
done

mgmt_irq=$(grep -E "ena-mgmnt|$iface-mgmnt" /proc/interrupts | awk -F: '{print $1}' | tr -d ' ' | head -1)
if [ -n "$mgmt_irq" ]; then
    echo 1 > /proc/irq/$mgmt_irq/smp_affinity
    echo "Management IRQ $mgmt_irq -> CPU 0" >&2
fi

echo "Done! Current ENA IRQ affinity:" >&2
for queue in $(seq 0 $((num_queues - 1))); do
    irq=$(grep "$iface-Tx-Rx-$queue" /proc/interrupts | awk -F: '{print $1}' | tr -d ' ')
    if [ -n "$irq" ]; then
        affinity=$(cat /proc/irq/$irq/smp_affinity)
        start_cpu=$((queue * cpus_per_queue))
        end_cpu=$(((queue + 1) * cpus_per_queue))
        if [ $end_cpu -gt $num_cpus ]; then
            end_cpu=$num_cpus
        fi
        echo "Queue $queue (IRQ $irq): CPUs $start_cpu-$end_cpu, mask = 0x$affinity" >&2
    fi
done

echo "RFS configured: 32768 global flow entries, 4096 per queue" >&2
echo "XPS configured for TX steering" >&2
"##;

    run_cmd! {
        echo $script_content > $script_path;
        chmod +x $script_path;

        mkdir -p $ETC_PATH;
        echo $systemd_unit_content > ${ETC_PATH}ena-irq-affinity.service;
        systemctl enable --now ${ETC_PATH}ena-irq-affinity.service;
    }?;
    Ok(())
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
