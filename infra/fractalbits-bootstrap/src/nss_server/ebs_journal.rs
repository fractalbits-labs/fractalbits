use super::*;

// Fixed journal segment size for AWS EBS: 4GB
pub const FA_JOURNAL_SEGMENT_SIZE: u64 = 4 * 1024 * 1024 * 1024;

/// Format EBS journal with a specific volume ID and journal UUID
pub fn format_with_volume_id(volume_id: &str, journal_uuid: &str) -> CmdResult {
    let ebs_dev = get_volume_dev(volume_id);
    info!("Formatting EBS device: {ebs_dev} for volume {volume_id} with UUID {journal_uuid}");
    format_internal(&ebs_dev, journal_uuid)?;
    Ok(())
}

pub(crate) fn format_internal(ebs_dev: &str, journal_uuid: &str) -> CmdResult {
    // Mount EBS at /data/ebs/{journal_uuid} (dynamic mount point per volume)
    let mount_point = format!("/data/ebs/{journal_uuid}");
    let journal_dir = mount_point.clone();

    run_cmd! {
        info "Disabling udev rules for EBS";
        ln -sf /dev/null /etc/udev/rules.d/99-ebs.rules;

        info "Formatting $ebs_dev to ext4 file system with UUID $journal_uuid";
        mkfs.ext4 -U $journal_uuid -O bigalloc -C 16384 $ebs_dev &>/dev/null;

        info "Mounting $ebs_dev to $mount_point";
        mkdir -p $mount_point;
        mount $ebs_dev $mount_point;

        info "Creating journal directory at $journal_dir";
        mkdir -p $journal_dir;
    }?;

    format_nss(false)?;

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
    // Sanitize: convert vol-07451bc901d5e1e09 → vol07451bc901d5e1e09
    let volume_id = &volume_id.replace("-", "");
    format!("/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{volume_id}")
}
