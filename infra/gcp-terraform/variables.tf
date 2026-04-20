variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "zone_a" {
  type    = string
  default = "us-central1-a"
}

variable "zone_b" {
  type    = string
  default = "us-central1-b"
}

variable "cluster_id" {
  type        = string
  description = "Unique cluster identifier (e.g., timestamp-based)"
}

variable "vpc_template" {
  type    = string
  default = "mini"
  validation {
    condition     = contains(["mini", "standard"], var.vpc_template)
    error_message = "vpc_template must be 'mini' or 'standard'"
  }
}

variable "num_api_servers" {
  type    = number
  default = 1
}

variable "num_bss_nodes" {
  type    = number
  default = 1
}

variable "rss_machine_type" {
  type    = string
  default = "c3-standard-4"
}

variable "nss_machine_type" {
  type    = string
  default = "c3d-standard-8"
}

variable "bss_machine_type" {
  type    = string
  default = "c3d-standard-8-lssd"
}

variable "api_machine_type" {
  type    = string
  default = "c3-standard-4"
}

variable "rss_backend" {
  type    = string
  default = "firestore"
  validation {
    condition     = contains(["firestore", "etcd"], var.rss_backend)
    error_message = "rss_backend must be 'firestore' or 'etcd'"
  }
}

variable "data_blob_storage" {
  type    = string
  default = "all_in_bss_single_az"
}

variable "root_server_ha" {
  type    = bool
  default = false
}

variable "with_bench" {
  type    = bool
  default = false
}

variable "num_bench_clients" {
  type    = number
  default = 0
}

variable "use_generic_binaries" {
  type    = bool
  default = true
}

variable "os_image" {
  type    = string
  default = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
}

variable "boot_disk_size_gb" {
  type    = number
  default = 30
}
