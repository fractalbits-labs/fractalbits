# GCS bucket for hybrid blob storage (optional)
resource "google_storage_bucket" "data_blob" {
  count                       = var.data_blob_storage == "gcs_hybrid_single_az" ? 1 : 0
  name                        = "fractalbits-data-${var.cluster_id}"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}
