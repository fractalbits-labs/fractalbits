use super::*;

// Fixed journal segment size for AWS EBS: 4GB
pub const FA_JOURNAL_SEGMENT_SIZE: u64 = 4 * 1024 * 1024 * 1024;

/// Format EBS journal with a specific volume ID
pub fn format_with_volume_id(volume_id: &str) -> CmdResult {
    let ebs_dev = get_volume_dev(volume_id);
    info!("Formatting EBS device: {ebs_dev} for volume {volume_id}");
    format_internal(&ebs_dev)?;
    Ok(())
}

pub(crate) fn format_internal(ebs_dev: &str) -> CmdResult {
    run_cmd! {
        info "Disabling udev rules for EBS";
        ln -sf /dev/null /etc/udev/rules.d/99-ebs.rules;

        info "Formatting $ebs_dev to ext4 file system";
        mkfs.ext4 -O bigalloc -C 16384 $ebs_dev &>/dev/null;

        info "Mounting $ebs_dev to /data/ebs";
        mkdir -p /data/ebs;
        mount $ebs_dev /data/ebs;
    }?;

    format_nss(false)?;

    run_cmd! {
        info "Enabling udev rules for EBS";
        ln -sf /opt/fractalbits/etc/99-ebs.rules /etc/udev/rules.d/99-ebs.rules;
        udevadm control --reload-rules;
        udevadm trigger;

        info "${ebs_dev} is formatted successfully.";
    }?;

    Ok(())
}

pub fn get_volume_dev(volume_id: &str) -> String {
    // Sanitize: convert vol-07451bc901d5e1e09 → vol07451bc901d5e1e09
    let volume_id = &volume_id.replace("-", "");
    format!("/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{volume_id}")
}
