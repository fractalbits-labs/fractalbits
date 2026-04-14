use super::*;

// Fixed journal segment size for AWS EBS: 4GB
pub const FA_JOURNAL_SEGMENT_SIZE: u64 = 4 * 1024 * 1024 * 1024;

/// Format EBS journal with a specific volume ID and journal UUID
pub fn format_with_volume_id(
    volume_id: &str,
    journal_uuid: &str,
    metadata_vg_config: &str,
    journal_vg_config: &str,
) -> CmdResult {
    let ebs_dev = get_volume_dev(volume_id);
    info!("Formatting EBS device: {ebs_dev} for volume {volume_id} with UUID {journal_uuid}");
    format_internal(&ebs_dev, journal_uuid, metadata_vg_config, journal_vg_config)?;
    Ok(())
}

pub(crate) fn format_internal(
    ebs_dev: &str,
    journal_uuid: &str,
    metadata_vg_config: &str,
    journal_vg_config: &str,
) -> CmdResult {
    // Mount EBS at /data/ebs/{journal_uuid} (dynamic mount point per volume)
    let mount_point = format!("/data/ebs/{journal_uuid}");
    let journal_dir = mount_point.clone();

    // Unmount if already mounted (e.g., from a previous bootstrap attempt)
    let _ = cmd_lib::run_cmd!(umount $ebs_dev 2>/dev/null);

    run_cmd! {
        info "Disabling udev rules for EBS";
        ln -sf /dev/null /etc/udev/rules.d/99-ebs.rules;

        info "Formatting $ebs_dev to ext4 file system with UUID $journal_uuid";
        mkfs.ext4 -F -U $journal_uuid -O bigalloc -C 16384 $ebs_dev &>/dev/null;

        info "Mounting $ebs_dev to $mount_point";
        mkdir -p $mount_point;
        mount -o noatime,nodiratime,lazytime $ebs_dev $mount_point;

        info "Creating journal directory at $journal_dir";
        mkdir -p $journal_dir;
    }?;

    format_nss(false, metadata_vg_config, journal_vg_config)?;

    run_cmd! {
        info "Enabling udev rules for EBS";
        ln -sf /opt/fractalbits/etc/99-ebs.rules /etc/udev/rules.d/99-ebs.rules;
        udevadm control --reload-rules;
        udevadm trigger;

        info "${ebs_dev} is formatted successfully with UUID ${journal_uuid}.";
    }?;

    Ok(())
}

pub fn get_volume_dev(volume_id: &str) -> String {
    // GCP persistent disk: device name is set in terraform
    if let Some(device_name) = volume_id.strip_prefix("gcp:") {
        return format!("/dev/disk/by-id/google-{device_name}");
    }
    // AWS EBS: sanitize vol-07451bc901d5e1e09 → vol07451bc901d5e1e09
    let volume_id = &volume_id.replace("-", "");
    format!("/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{volume_id}")
}
