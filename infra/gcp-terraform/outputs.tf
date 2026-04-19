output "rss_a_ip" {
  value       = google_compute_instance.rss_a.network_interface[0].network_ip
  description = "RSS-A private IP"
}

output "rss_a_name" {
  value       = google_compute_instance.rss_a.name
  description = "RSS-A instance name"
}

output "rss_b_ip" {
  value       = local.rss_ha_enabled ? google_compute_instance.rss_b[0].network_interface[0].network_ip : ""
  description = "RSS-B private IP (HA only)"
}

output "nss_instance_group" {
  value       = google_compute_instance_group_manager.nss_server.instance_group
  description = "NSS server instance group URL (managed singleton)"
}

output "api_lb_ip" {
  value       = google_compute_forwarding_rule.api_lb.ip_address
  description = "API server load balancer IP"
}

output "api_instance_group" {
  value       = google_compute_instance_group_manager.api_servers.instance_group
  description = "API server instance group URL"
}

output "bss_instance_group" {
  value       = google_compute_instance_group_manager.bss_servers.instance_group
  description = "BSS server instance group URL"
}

output "service_account_email" {
  value       = google_service_account.fractalbits.email
  description = "Service account email"
}

output "cluster_id" {
  value       = var.cluster_id
  description = "Cluster identifier"
}

output "project_id" {
  value       = var.project_id
  description = "GCP project ID"
}

output "region" {
  value       = var.region
  description = "GCP region"
}

output "zone_a" {
  value       = var.zone_a
  description = "Primary zone"
}

output "zone_b" {
  value       = var.zone_b
  description = "Secondary zone"
}

output "rss_backend" {
  value       = var.rss_backend
  description = "RSS backend type"
}

output "journal_type" {
  value       = var.journal_type
  description = "NSS journal type"
}

output "bench_server_name" {
  value       = var.with_bench ? google_compute_instance.bench[0].name : ""
  description = "Bench server instance name"
}

output "bench_server_ip" {
  value       = var.with_bench ? google_compute_instance.bench[0].network_interface[0].network_ip : ""
  description = "Bench server private IP"
}

output "rss_b_name" {
  value       = local.rss_ha_enabled ? google_compute_instance.rss_b[0].name : ""
  description = "RSS-B instance name (HA only)"
}

output "network_name" {
  value       = google_compute_network.fractalbits.name
  description = "VPC network name"
}

output "subnetwork_name" {
  value       = google_compute_subnetwork.private_a.name
  description = "Subnet name"
}

output "firestore_database_id" {
  value       = "fractalbits"
  description = "Firestore database ID"
}
