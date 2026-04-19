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
      role_args  = "--role root_server --rss-role leader"
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
      role_args  = "--role root_server --rss-role follower"
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

# NSS instance template — stateless, no journal disk (NSS uses BSS quorum
# journal VG). Provisioned via MIG below as a managed singleton (target_size=1)
# so a failed instance is replaced and re-registers in Firestore service
# discovery; RSS picks up the new endpoint on next leader-init poll.
resource "google_compute_instance_template" "nss_server" {
  name_prefix  = "nss-${var.cluster_id}-"
  machine_type = var.nss_machine_type
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

# NSS managed singleton — target_size=1 preserves the current single-NSS
# topology while using MIG so the instance is auto-replaced on failure.
resource "google_compute_instance_group_manager" "nss_server" {
  name               = "nss-server-${var.cluster_id}"
  base_instance_name = "nss-${var.cluster_id}"
  zone               = var.zone_a
  target_size        = 1

  version {
    instance_template = google_compute_instance_template.nss_server.id
  }
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
