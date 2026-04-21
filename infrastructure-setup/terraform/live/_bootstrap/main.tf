variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

provider "google" {
  project = var.project_id
  region  = "us-central1"
}

resource "google_storage_bucket" "terraform_state" {
  name          = "ncmapss-terraform-state-${var.project_id}"
  location      = "us-central1"
  force_destroy = false # Protect against accidental deletion

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true
}
