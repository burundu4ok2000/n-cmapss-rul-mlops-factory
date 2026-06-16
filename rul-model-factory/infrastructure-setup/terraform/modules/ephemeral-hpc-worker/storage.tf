# ------------------------------------------------------------------------------
# STORAGE: DATA LAKE & ARTIFACTS
# ------------------------------------------------------------------------------

resource "google_storage_bucket" "training_assets" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    purpose = "ml-training"
  }

  encryption {
    default_kms_key_name = var.kms_key_id
  }
}

resource "google_artifact_registry_repository" "factory" {
  location      = var.region
  repository_id = var.repository_id
  description   = "Docker repository for ML models and training images"
  format        = "DOCKER"

  docker_config {
    immutable_tags = false
  }
}
