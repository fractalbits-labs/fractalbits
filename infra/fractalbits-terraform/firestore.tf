resource "google_firestore_database" "fractalbits" {
  count       = var.rss_backend == "firestore" ? 1 : 0
  project     = var.project_id
  name        = "(default)"
  location_id = var.region
  type        = "FIRESTORE_NATIVE"
}
