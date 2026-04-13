# NSS journal disk (hyperdisk-balanced) - shared between NSS-0 and NSS-1 via multi-writer
# Note: pd-ssd does not support READ_WRITE_MANY; hyperdisk-balanced does.
resource "google_compute_disk" "nss_journal" {
  count                     = var.journal_type == "pd_ssd" ? 1 : 0
  name                      = "nss-journal-${var.cluster_id}"
  zone                      = var.zone_a
  type                      = "hyperdisk-balanced"
  size                      = var.journal_disk_size_gb
  physical_block_size_bytes = 4096
  access_mode               = "READ_WRITE_MANY"
}

# GCS bucket for hybrid blob storage (optional)
resource "google_storage_bucket" "data_blob" {
  count                       = var.data_blob_storage == "gcs_hybrid_single_az" ? 1 : 0
  name                        = "fractalbits-data-${var.cluster_id}"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}
