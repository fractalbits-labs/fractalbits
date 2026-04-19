use crate::common::{
    BIN_PATH, CLOUDWATCH_AGENT_CONFIG, DDB_SERVICE_DISCOVERY_TABLE, ETC_PATH, OsType,
    install_packages,
};
use cmd_lib::*;
use std::time::{Duration, Instant};

/// Install amazon-ec2-utils on Ubuntu so that ec2-metadata is available
/// for get_instance_id(). Only needed on AWS; on AL2023 it's pre-installed.
pub fn ensure_ec2_metadata() -> CmdResult {
    if OsType::detect() == OsType::Ubuntu {
        install_packages(&["amazon-ec2-utils"])?;
    }
    Ok(())
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
pub(crate) fn get_cpu_target() -> FunResult {
    let instance_type = get_ec2_instance_type()?;
    Ok(get_cpu_target_from_instance_type(&instance_type).to_string())
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

pub fn get_aws_instance_id() -> FunResult {
    run_fun!(ec2-metadata --instance-id | awk r"{print $2}")
}

pub fn get_aws_private_ip() -> FunResult {
    run_fun!(ec2-metadata --local-ipv4 | awk r"{print $2}")
}

pub fn get_s3_express_bucket_name(az: &str) -> FunResult {
    // Generate bucket name in format: fractalbits-data-{account}--{az}--x-s3
    let account = get_account_id()?;
    Ok(format!("fractalbits-data-{account}--{az}--x-s3"))
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

pub(crate) fn install_cloudwatch_agent(os: OsType) -> CmdResult {
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

pub(crate) fn get_service_ips(service_id: &str, expected_min_count: usize) -> Vec<String> {
    get_service_instances(service_id, expected_min_count)
        .into_iter()
        .map(|(_id, ip)| ip)
        .collect()
}

pub(crate) fn get_service_instances(
    service_id: &str,
    expected_min_count: usize,
) -> Vec<(String, String)> {
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
                let mut pairs = Vec::new();
                if let Some(obj) = instances.as_object() {
                    for (instance_id, value) in obj {
                        if let Some(ip_obj) = value.get("S")
                            && let Some(ip) = ip_obj.as_str()
                        {
                            pairs.push((instance_id.clone(), ip.to_string()));
                        }
                    }
                }
                if pairs.len() >= expected_min_count {
                    info!("Found a list of {service_id} services: {pairs:?}");
                    return pairs;
                }
            }
            _ => std::thread::sleep(std::time::Duration::from_secs(1)),
        }
    }
}

pub fn create_ena_irq_affinity_service() -> CmdResult {
    let script_path = format!("{BIN_PATH}configure-ena-irq-affinity.sh");
    let systemd_unit_content = format!(
        r##"[Unit]
Description=ENA IRQ Affinity Configuration
After=network-online.target
Before=api_server.service bss.service nss.service bench_client.service

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
