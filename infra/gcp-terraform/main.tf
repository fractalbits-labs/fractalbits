terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    # Configure via: terraform init -backend-config="bucket=<state-bucket>" -backend-config="prefix=vpc"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  rss_a_name = "rss-a"
  rss_b_name = "rss-b"

  # Mini template: single NSS, single BSS, single API server
  # Standard template: single NSS, multiple BSS/API
  rss_ha_enabled = var.vpc_template == "standard" || var.root_server_ha
}
