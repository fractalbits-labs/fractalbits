resource "google_compute_network" "fractalbits" {
  name                    = "fractalbits-vpc-${var.cluster_id}"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "private_a" {
  name                     = "fractalbits-private-a-${var.cluster_id}"
  ip_cidr_range            = "10.0.1.0/24"
  region                   = var.region
  network                  = google_compute_network.fractalbits.id
  private_ip_google_access = true
}

resource "google_compute_subnetwork" "private_b" {
  count                    = local.rss_ha_enabled ? 1 : 0
  name                     = "fractalbits-private-b-${var.cluster_id}"
  ip_cidr_range            = "10.0.2.0/24"
  region                   = var.region
  network                  = google_compute_network.fractalbits.id
  private_ip_google_access = true
}

# Allow all internal traffic between fractalbits nodes
resource "google_compute_firewall" "internal" {
  name    = "fractalbits-internal-${var.cluster_id}"
  network = google_compute_network.fractalbits.id

  allow {
    protocol = "tcp"
    ports = [
      "80",    # API server HTTP
      "2379",  # etcd client
      "2380",  # etcd peer
      "8080",  # Docker S3 API
      "8086",  # RSS RPC
      "8087",  # NSS RPC
      "8088",  # BSS RPC
      "9999",  # reserved
      "18080", # Docker S3 mgmt / API mgmt
      "18086", # RSS health
      "18087", # RSS metrics
      "18088", # service management
    ]
  }

  allow {
    protocol = "icmp"
  }

  source_tags = ["fractalbits-private"]
  source_ranges = compact([
    google_compute_subnetwork.private_a.ip_cidr_range,
    local.rss_ha_enabled ? google_compute_subnetwork.private_b[0].ip_cidr_range : null,
  ])
  target_tags = ["fractalbits-private"]
}

# Allow IAP SSH tunneling for remote access
resource "google_compute_firewall" "ssh_iap" {
  name    = "fractalbits-ssh-iap-${var.cluster_id}"
  network = google_compute_network.fractalbits.id

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"] # GCP IAP range
  target_tags   = ["fractalbits-private"]
}

# Allow GCP health check probes to reach API servers
resource "google_compute_firewall" "health_check" {
  name    = "fractalbits-health-check-${var.cluster_id}"
  network = google_compute_network.fractalbits.id

  allow {
    protocol = "tcp"
    ports    = ["80"]
  }

  # GCP health check source ranges
  source_ranges = ["130.211.0.0/22", "35.191.0.0/16"]
  target_tags   = ["fractalbits-api"]
}

# Allow bench traffic if enabled
resource "google_compute_firewall" "bench" {
  count   = var.with_bench ? 1 : 0
  name    = "fractalbits-bench-${var.cluster_id}"
  network = google_compute_network.fractalbits.id

  allow {
    protocol = "tcp"
    ports    = ["7761"]
  }

  source_tags = ["fractalbits-bench"]
  target_tags = ["fractalbits-bench"]
}

# Cloud Router + NAT for outbound internet access (package installs, Docker pulls)
resource "google_compute_router" "nat_router" {
  name    = "fractalbits-router-${var.cluster_id}"
  region  = var.region
  network = google_compute_network.fractalbits.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "fractalbits-nat-${var.cluster_id}"
  router                             = google_compute_router.nat_router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}
