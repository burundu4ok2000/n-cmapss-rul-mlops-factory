# ------------------------------------------------------------------------------
# LIVE INSTANCE: RUL TRAINING FACTORY
# ------------------------------------------------------------------------------

provider "google" {
  project = var.project_id
  region  = var.region
}

module "ephemeral_node" {
  source = "../../modules/ephemeral-hpc-worker"

  project_id    = var.project_id
  region        = var.region
  bucket_name   = var.bucket_name
  machine_type  = var.machine_type
  repository_id       = var.repository_id
  disk_size_gb        = var.disk_size_gb
  kms_key_id          = google_kms_crypto_key.data_encryption_key.id
  fast_forward_source = var.fast_forward_source
}

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.27.0"
    }
  }
}
