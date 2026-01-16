use super::*;

// Fixed journal segment size for on-prem NVMe: 16GB
pub const FA_JOURNAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024 * 1024;

pub fn format() -> CmdResult {
    format_nss(true)?;
    info!("NSS server formatted successfully (nvme mode)");
    Ok(())
}
