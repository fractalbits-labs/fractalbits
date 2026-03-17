# Static internal IP for RSS-A (avoids self-referential metadata)
resource "google_compute_address" "rss_a" {
  name         = "rss-a-ip-${var.cluster_id}"
  subnetwork   = google_compute_subnetwork.private_a.id
  address_type = "INTERNAL"
  region       = var.region
}

# RSS-A (Root Storage Server - leader)
resource "google_compute_instance" "rss_a" {
  name         = "rss-a-${var.cluster_id}"
  machine_type = var.rss_machine_type
  zone         = var.zone_a

  boot_disk {
    initialize_params {
      image = var.os_image
      size  = var.boot_disk_size_gb
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.private_a.id
    network_ip = google_compute_address.rss_a.address
  }

  metadata = {
    service-role  = "root_server"
    instance-role = "leader"
    cluster-id    = var.cluster_id
    rss-backend   = var.rss_backend
    startup-script = templatefile("${path.module}/templates/startup-script.sh.tpl", {
      gcs_bucket = "${var.project_id}-deploy-staging"
    })
  }

  service_account {
    email  = google_service_account.fractalbits.email
    scopes = ["cloud-platform"]
  }

  tags = ["fractalbits-private", "rss"]

  allow_stopping_for_update = true

  depends_on = [
    google_project_iam_member.firestore,
    google_project_iam_member.storage,
    google_project_iam_member.compute,
    google_project_iam_member.logging,
    google_project_iam_member.monitoring,
  ]
}

# RSS-B (Root Storage Server - follower, HA only)
resource "google_compute_instance" "rss_b" {
  count        = local.is_ha ? 1 : 0
  name         = "rss-b-${var.cluster_id}"
  machine_type = var.rss_machine_type
  zone         = var.zone_b

  boot_disk {
    initialize_params {
      image = var.os_image
      size  = var.boot_disk_size_gb
    }
  }

  network_interface {
    subnetwork = local.is_ha ? google_compute_subnetwork.private_b[0].id : google_compute_subnetwork.private_a.id
  }

  metadata = {
    service-role  = "root_server"
    instance-role = "follower"
    cluster-id    = var.cluster_id
    rss-backend   = var.rss_backend
    startup-script = templatefile("${path.module}/templates/startup-script.sh.tpl", {
      gcs_bucket = "${var.project_id}-deploy-staging"
    })
  }

  service_account {
    email  = google_service_account.fractalbits.email
    scopes = ["cloud-platform"]
  }

  tags = ["fractalbits-private", "rss"]

  allow_stopping_for_update = true

  depends_on = [
    google_project_iam_member.firestore,
    google_project_iam_member.storage,
    google_project_iam_member.compute,
    google_project_iam_member.logging,
    google_project_iam_member.monitoring,
  ]
}

# NSS-A (Namespace Server)
resource "google_compute_instance" "nss_a" {
  name         = "nss-a-${var.cluster_id}"
  machine_type = var.nss_machine_type
  zone         = var.zone_a

  boot_disk {
    initialize_params {
      image = var.os_image
      size  = var.boot_disk_size_gb
    }
  }

  # Local SSDs for NVMe journal (if using local_ssd journal type)
  dynamic "scratch_disk" {
    for_each = var.journal_type == "local_ssd" ? [1] : []
    content {
      interface = "NVME"
    }
  }

  # Attach PD journal disk (if using pd_ssd journal type)
  dynamic "attached_disk" {
    for_each = var.journal_type == "pd_ssd" ? [1] : []
    content {
      source      = google_compute_disk.nss_journal_a[0].id
      device_name = "nss-journal"
      mode        = "READ_WRITE"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.private_a.id
  }

  metadata = {
    service-role  = "nss_server"
    instance-role = "active"
    cluster-id    = var.cluster_id
    startup-script = templatefile("${path.module}/templates/startup-script.sh.tpl", {
      gcs_bucket = "${var.project_id}-deploy-staging"
    })
  }

  service_account {
    email  = google_service_account.fractalbits.email
    scopes = ["cloud-platform"]
  }

  tags = ["fractalbits-private", "nss"]

  allow_stopping_for_update = true

  depends_on = [
    google_project_iam_member.firestore,
    google_project_iam_member.storage,
    google_project_iam_member.compute,
    google_project_iam_member.logging,
    google_project_iam_member.monitoring,
  ]
}

# NSS-B (Namespace Server - standby, HA only)
resource "google_compute_instance" "nss_b" {
  count        = local.is_ha ? 1 : 0
  name         = "nss-b-${var.cluster_id}"
  machine_type = var.nss_machine_type
  zone         = var.zone_b

  boot_disk {
    initialize_params {
      image = var.os_image
      size  = var.boot_disk_size_gb
    }
  }

  dynamic "scratch_disk" {
    for_each = var.journal_type == "local_ssd" ? [1] : []
    content {
      interface = "NVME"
    }
  }

  dynamic "attached_disk" {
    for_each = var.journal_type == "pd_ssd" ? [1] : []
    content {
      source      = google_compute_disk.nss_journal_b[0].id
      device_name = "nss-journal"
      mode        = "READ_WRITE"
    }
  }

  network_interface {
    subnetwork = local.is_ha ? google_compute_subnetwork.private_b[0].id : google_compute_subnetwork.private_a.id
  }

  metadata = {
    service-role  = "nss_server"
    instance-role = "standby"
    cluster-id    = var.cluster_id
    startup-script = templatefile("${path.module}/templates/startup-script.sh.tpl", {
      gcs_bucket = "${var.project_id}-deploy-staging"
    })
  }

  service_account {
    email  = google_service_account.fractalbits.email
    scopes = ["cloud-platform"]
  }

  tags = ["fractalbits-private", "nss"]

  allow_stopping_for_update = true

  depends_on = [
    google_project_iam_member.firestore,
    google_project_iam_member.storage,
    google_project_iam_member.compute,
    google_project_iam_member.logging,
    google_project_iam_member.monitoring,
  ]
}

# Bench server (optional)
resource "google_compute_instance" "bench" {
  count        = var.with_bench ? 1 : 0
  name         = "bench-${var.cluster_id}"
  machine_type = var.api_machine_type
  zone         = var.zone_a

  boot_disk {
    initialize_params {
      image = var.os_image
      size  = var.boot_disk_size_gb
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.private_a.id
  }

  metadata = {
    service-role  = "bench_server"
    instance-role = "bench"
    cluster-id    = var.cluster_id
    startup-script = templatefile("${path.module}/templates/startup-script.sh.tpl", {
      gcs_bucket = "${var.project_id}-deploy-staging"
    })
  }

  service_account {
    email  = google_service_account.fractalbits.email
    scopes = ["cloud-platform"]
  }

  tags = ["fractalbits-private", "fractalbits-bench"]

  allow_stopping_for_update = true

  depends_on = [
    google_project_iam_member.firestore,
    google_project_iam_member.storage,
    google_project_iam_member.compute,
    google_project_iam_member.logging,
    google_project_iam_member.monitoring,
  ]
}
