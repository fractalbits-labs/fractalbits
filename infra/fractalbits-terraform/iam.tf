resource "google_service_account" "fractalbits" {
  account_id   = "fractalbits-${var.cluster_id}"
  display_name = "Fractalbits Compute Engine SA (${var.cluster_id})"
}

resource "google_project_iam_member" "firestore" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.fractalbits.email}"
}

resource "google_project_iam_member" "storage" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.fractalbits.email}"
}

resource "google_project_iam_member" "compute" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${google_service_account.fractalbits.email}"
}

resource "google_project_iam_member" "logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.fractalbits.email}"
}

resource "google_project_iam_member" "monitoring" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.fractalbits.email}"
}
