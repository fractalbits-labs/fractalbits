# NSS journal disk (PD mode) - zone A
resource "google_compute_disk" "nss_journal_a" {
  count                     = var.journal_type == "pd_ssd" ? 1 : 0
  name                      = "nss-journal-a-${var.cluster_id}"
  zone                      = var.zone_a
  type                      = "pd-ssd"
  size                      = var.journal_disk_size_gb
  physical_block_size_bytes = 4096
}

# NSS journal disk (PD mode) - zone B (HA only)
resource "google_compute_disk" "nss_journal_b" {
  count                     = var.journal_type == "pd_ssd" && local.is_ha ? 1 : 0
  name                      = "nss-journal-b-${var.cluster_id}"
  zone                      = var.zone_b
  type                      = "pd-ssd"
  size                      = var.journal_disk_size_gb
  physical_block_size_bytes = 4096
}

# Regional PD for multi-attach HA (shared journal between NSS-A and NSS-B)
resource "google_compute_region_disk" "nss_shared_journal" {
  count                     = var.journal_type == "pd_ssd" && local.is_ha ? 1 : 0
  name                      = "nss-shared-journal-${var.cluster_id}"
  type                      = "pd-ssd"
  region                    = var.region
  size                      = var.journal_disk_size_gb
  physical_block_size_bytes = 4096

  replica_zones = [
    "projects/${var.project_id}/zones/${var.zone_a}",
    "projects/${var.project_id}/zones/${var.zone_b}",
  ]
}

# GCS bucket for hybrid blob storage (optional)
resource "google_storage_bucket" "data_blob" {
  count                       = var.data_blob_storage == "gcs_hybrid_single_az" ? 1 : 0
  name                        = "fractalbits-data-${var.cluster_id}"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}
