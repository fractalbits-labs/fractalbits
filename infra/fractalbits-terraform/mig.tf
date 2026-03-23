# API Server instance template
resource "google_compute_instance_template" "api_server" {
  name_prefix  = "api-${var.cluster_id}-"
  machine_type = var.api_machine_type
  region       = var.region

  disk {
    source_image = var.os_image
    auto_delete  = true
    boot         = true
    disk_size_gb = var.boot_disk_size_gb
    disk_type    = "pd-ssd"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.private_a.id
  }

  metadata = {
    service-role  = "api_server"
    instance-role = "api"
    cluster-id    = var.cluster_id
    startup-script = templatefile("${path.module}/templates/startup-script.sh.tpl", {
      gcs_bucket = "${var.project_id}-deploy-staging"
      role_args  = "--role api_server"
    })
  }

  service_account {
    email  = google_service_account.fractalbits.email
    scopes = ["cloud-platform"]
  }

  tags = ["fractalbits-private", "fractalbits-api"]

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    google_project_iam_member.firestore,
    google_project_iam_member.storage,
    google_project_iam_member.compute,
    google_project_iam_member.logging,
    google_project_iam_member.monitoring,
  ]
}

# API Server managed instance group
resource "google_compute_instance_group_manager" "api_servers" {
  name               = "api-servers-${var.cluster_id}"
  base_instance_name = "api-${var.cluster_id}"
  zone               = var.zone_a
  target_size        = var.num_api_servers

  version {
    instance_template = google_compute_instance_template.api_server.id
  }

  named_port {
    name = "http"
    port = 80
  }
}

# BSS Server instance template
resource "google_compute_instance_template" "bss_server" {
  name_prefix  = "bss-${var.cluster_id}-"
  machine_type = var.bss_machine_type
  region       = var.region

  disk {
    source_image = var.os_image
    auto_delete  = true
    boot         = true
    disk_size_gb = var.boot_disk_size_gb
    disk_type    = "pd-ssd"
  }

  # Local SSDs for BSS data storage (io_uring direct I/O)
  disk {
    type         = "SCRATCH"
    disk_type    = "local-ssd"
    disk_size_gb = 375
    interface    = "NVME"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.private_a.id
  }

  metadata = {
    service-role  = "bss_server"
    instance-role = "bss"
    cluster-id    = var.cluster_id
    startup-script = templatefile("${path.module}/templates/startup-script.sh.tpl", {
      gcs_bucket = "${var.project_id}-deploy-staging"
      role_args  = "--role bss_server"
    })
  }

  service_account {
    email  = google_service_account.fractalbits.email
    scopes = ["cloud-platform"]
  }

  tags = ["fractalbits-private", "bss"]

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    google_project_iam_member.firestore,
    google_project_iam_member.storage,
    google_project_iam_member.compute,
    google_project_iam_member.logging,
    google_project_iam_member.monitoring,
  ]
}

# BSS Server managed instance group
resource "google_compute_instance_group_manager" "bss_servers" {
  name               = "bss-servers-${var.cluster_id}"
  base_instance_name = "bss-${var.cluster_id}"
  zone               = var.zone_a
  target_size        = var.num_bss_nodes

  version {
    instance_template = google_compute_instance_template.bss_server.id
  }
}
