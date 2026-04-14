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
      role_args  = "--role root_server --rss-role leader --nss-a-ip ${google_compute_instance.nss_a.network_interface[0].network_ip} --nss-a-id nss-0-${var.cluster_id}"
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
  count        = local.rss_ha_enabled ? 1 : 0
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
    subnetwork = local.rss_ha_enabled ? google_compute_subnetwork.private_b[0].id : google_compute_subnetwork.private_a.id
  }

  metadata = {
    service-role  = "root_server"
    instance-role = "follower"
    cluster-id    = var.cluster_id
    rss-backend   = var.rss_backend
    startup-script = templatefile("${path.module}/templates/startup-script.sh.tpl", {
      gcs_bucket = "${var.project_id}-deploy-staging"
      role_args  = "--role root_server --rss-role follower --nss-a-ip ${google_compute_instance.nss_a.network_interface[0].network_ip}"
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

# NSS-0 (Namespace Server)
resource "google_compute_instance" "nss_a" {
  name         = "nss-0-${var.cluster_id}"
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

  # Attach shared PD journal disk (if using pd_ssd journal type)
  dynamic "attached_disk" {
    for_each = var.journal_type == "pd_ssd" ? [1] : []
    content {
      source      = google_compute_disk.nss_journal[0].id
      device_name = "nss-journal"
      # Multi-writer is set on the disk itself (access_mode=READ_WRITE_MANY).
      # The attached_disk mode must be READ_WRITE (not READ_WRITE_MANY).
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
      role_args  = "--role nss_server --nss-role primary"
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

# Bench clients (optional, one per index)
resource "google_compute_instance" "bench_client" {
  count        = var.with_bench ? var.num_bench_clients : 0
  name         = "bench-client-${count.index}-${var.cluster_id}"
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
    service-role  = "bench_client"
    instance-role = "bench_client"
    cluster-id    = var.cluster_id
    startup-script = templatefile("${path.module}/templates/startup-script.sh.tpl", {
      gcs_bucket = "${var.project_id}-deploy-staging"
      role_args  = "--role bench_client"
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
      role_args  = "--role bench_server --api-server-endpoint ${google_compute_forwarding_rule.api_lb.ip_address}"
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
    google_compute_forwarding_rule.api_lb,
  ]
}
