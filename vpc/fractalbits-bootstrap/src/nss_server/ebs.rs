use super::*;

// AWS EBS has 500 IOPS/GB limit, so we need to have 20GB
// space for 10K IOPS. but journal size is much smaller.
const EBS_SPACE_PERCENT: f64 = 0.2;

/// Calculate fa_journal_segment_size based on EBS volume size
pub(crate) fn calculate_fa_journal_segment_size(volume_dev: &str) -> Result<u64, Error> {
    // Get total size of volume_dev in bytes
    let ebs_blockdev_size_str = run_fun!(blockdev --getsize64 ${volume_dev})?;
    let ebs_blockdev_size = ebs_blockdev_size_str.trim().parse::<u64>().map_err(|_| {
        Error::other(format!(
            "invalid ebs blockdev size: {ebs_blockdev_size_str}"
        ))
    })?;
    let ebs_blockdev_mb = ebs_blockdev_size / 1024 / 1024;
    let fa_journal_segment_size = (ebs_blockdev_mb as f64 * EBS_SPACE_PERCENT) as u64 * 1024 * 1024;
    Ok(fa_journal_segment_size)
}

pub fn format_nss() -> CmdResult {
    let ebs_dev = discover_ebs_device()?;
    format_nss_internal(&ebs_dev)?;
    Ok(())
}

pub(crate) fn format_nss_internal(ebs_dev: &str) -> CmdResult {
    run_cmd! {
        info "Disabling udev rules for EBS";
        ln -sf /dev/null /etc/udev/rules.d/99-ebs.rules;

        info "Formatting $ebs_dev to ext4 file system";
        mkfs.ext4 -O bigalloc -C 16384 $ebs_dev &>/dev/null;

        info "Mounting $ebs_dev to /data/ebs";
        mkdir -p /data/ebs;
        mount $ebs_dev /data/ebs;
    }?;

    let mut wait_secs = 0;
    while run_cmd!(mountpoint -q "/data/local").is_err() {
        wait_secs += 1;
        info!("Waiting for /data/local to be mounted ({wait_secs}s)");
        std::thread::sleep(std::time::Duration::from_secs(1));
        if wait_secs >= 120 {
            cmd_die!("Timeout when waiting for /data/local to be mounted (120s)");
        }
    }

    run_cmd! {
        info "Creating directories for nss_server";
        mkdir -p /data/local/stats;
        mkdir -p /data/local/meta_cache/blobs;
    }?;

    info!(
        "Creating {} meta cache shard directories in parallel",
        NSS_META_CACHE_SHARDS
    );
    let shards: Vec<usize> = (0..NSS_META_CACHE_SHARDS).collect();
    shards.par_iter().try_for_each(|&i| {
        let shard_dir = format!("/data/local/meta_cache/blobs/{}", i);
        std::fs::create_dir(&shard_dir)
            .map_err(|e| Error::other(format!("Failed to create {}: {}", shard_dir, e)))
    })?;

    run_cmd! {
        info "Syncing file system changes";
        sync;
    }?;

    run_cmd! {
        info "Running format for nss_server";
        /opt/fractalbits/bin/nss_server format -c ${ETC_PATH}${NSS_SERVER_CONFIG};
    }?;

    run_cmd! {
        info "Enabling udev rules for EBS";
        ln -sf /opt/fractalbits/etc/99-ebs.rules /etc/udev/rules.d/99-ebs.rules;
        udevadm control --reload-rules;
        udevadm trigger;

        info "${ebs_dev} is formatted successfully.";
    }?;

    Ok(())
}

pub(crate) fn create_ebs_udev_rule(volume_id: &str, service_name: &str) -> CmdResult {
    let content = format!(
        r##"KERNEL=="nvme*n*", SUBSYSTEM=="block", ENV{{ID_SERIAL}}=="Amazon_Elastic_Block_Store_{}_1", TAG+="systemd", ENV{{SYSTEMD_WANTS}}="{service_name}.service""##,
        volume_id.replace("-", "")
    );
    run_cmd! {
        echo $content > $ETC_PATH/99-ebs.rules;
        ln -s $ETC_PATH/99-ebs.rules /etc/udev/rules.d/;
    }?;

    Ok(())
}

fn discover_ebs_device() -> Result<String, io::Error> {
    info!("Discovering EBS device from bootstrap config");

    let config = BootstrapConfig::download_and_parse()?;
    let instance_id = get_instance_id()?;

    let instance_config = config
        .instances
        .get(&instance_id)
        .ok_or_else(|| io::Error::other(format!("Instance {} not found in config", instance_id)))?;

    let volume_id = instance_config
        .volume_id
        .as_ref()
        .ok_or_else(|| io::Error::other("volume_id not set in instance config"))?;

    let ebs_dev = get_volume_dev(volume_id);
    info!("Discovered EBS device: {ebs_dev} for volume {volume_id}");
    Ok(ebs_dev)
}

pub fn get_volume_dev(volume_id: &str) -> String {
    // Sanitize: convert vol-07451bc901d5e1e09 → vol07451bc901d5e1e09
    let volume_id = &volume_id.replace("-", "");
    format!("/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{volume_id}")
}
