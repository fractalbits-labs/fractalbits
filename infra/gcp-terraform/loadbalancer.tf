# Skipped in bench mode: bench_server hits API IPs directly (no --use-nlb),
# so the LB would be unused. Kept for other deploys.

# Health check for API servers
resource "google_compute_region_health_check" "api" {
  count              = var.with_bench ? 0 : 1
  name               = "api-health-${var.cluster_id}"
  region             = var.region
  check_interval_sec = 10
  timeout_sec        = 5

  tcp_health_check {
    port = 80
  }
}

# Internal TCP Load Balancer backend service
resource "google_compute_region_backend_service" "api_lb" {
  count                 = var.with_bench ? 0 : 1
  name                  = "api-lb-${var.cluster_id}"
  region                = var.region
  protocol              = "TCP"
  load_balancing_scheme = "INTERNAL"

  backend {
    group = google_compute_instance_group_manager.api_servers.instance_group
  }

  health_checks = [google_compute_region_health_check.api[0].id]
}

# Internal forwarding rule
resource "google_compute_forwarding_rule" "api_lb" {
  count                 = var.with_bench ? 0 : 1
  name                  = "api-lb-rule-${var.cluster_id}"
  region                = var.region
  load_balancing_scheme = "INTERNAL"
  backend_service       = google_compute_region_backend_service.api_lb[0].id
  ports                 = ["80"]
  network               = google_compute_network.fractalbits.id
  subnetwork            = google_compute_subnetwork.private_a.id
}
