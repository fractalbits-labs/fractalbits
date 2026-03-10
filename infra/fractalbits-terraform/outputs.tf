output "rss_a_ip" {
  value       = google_compute_instance.rss_a.network_interface[0].network_ip
  description = "RSS-A private IP"
}

output "rss_a_name" {
  value       = google_compute_instance.rss_a.name
  description = "RSS-A instance name"
}

output "rss_b_ip" {
  value       = local.is_ha ? google_compute_instance.rss_b[0].network_interface[0].network_ip : ""
  description = "RSS-B private IP (HA only)"
}

output "nss_a_ip" {
  value       = google_compute_instance.nss_a.network_interface[0].network_ip
  description = "NSS-A private IP"
}

output "nss_a_name" {
  value       = google_compute_instance.nss_a.name
  description = "NSS-A instance name"
}

output "nss_b_ip" {
  value       = local.is_ha ? google_compute_instance.nss_b[0].network_interface[0].network_ip : ""
  description = "NSS-B private IP (HA only)"
}

output "nss_b_name" {
  value       = local.is_ha ? google_compute_instance.nss_b[0].name : ""
  description = "NSS-B instance name (HA only)"
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

output "vpc_network" {
  value       = google_compute_network.fractalbits.name
  description = "VPC network name"
}

output "subnet_a" {
  value       = google_compute_subnetwork.private_a.name
  description = "Subnet A name"
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

output "bench_ip" {
  value       = var.with_bench ? google_compute_instance.bench[0].network_interface[0].network_ip : ""
  description = "Bench server private IP"
}
