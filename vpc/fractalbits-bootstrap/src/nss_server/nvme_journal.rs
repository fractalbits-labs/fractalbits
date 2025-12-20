use super::*;

const NVME_JOURNAL_PERCENT: f64 = 0.2;
const MAX_JOURNAL_SEGMENT_SIZE: u64 = 4 * 1024 * 1024 * 1024; // 4GB max

pub(crate) fn calculate_fa_journal_segment_size() -> Result<u64, Error> {
    // Get the size of /data/local mount point
    let nvme_size_str = run_fun!(df -B1 /data/local | tail -1 | awk r"{print $2}")?;
    let nvme_size = nvme_size_str
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::other(format!("invalid nvme size: {nvme_size_str}")))?;
    // Convert to MB first then back to ensure alignment to 1MB boundary
    let nvme_size_mb = nvme_size / 1024 / 1024;
    let calculated_size = (nvme_size_mb as f64 * NVME_JOURNAL_PERCENT) as u64 * 1024 * 1024;
    // Cap at MAX_JOURNAL_SEGMENT_SIZE to prevent OOM during replay
    let fa_journal_segment_size = calculated_size.min(MAX_JOURNAL_SEGMENT_SIZE);
    Ok(fa_journal_segment_size)
}

pub fn format() -> CmdResult {
    wait_and_format_nss(true)?;
    info!("NSS server formatted successfully (nvme mode)");
    Ok(())
}
