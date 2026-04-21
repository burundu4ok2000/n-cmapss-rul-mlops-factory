# ------------------------------------------------------------------------------
# IDENTITY: SERVICE ACCOUNT FOR ML TRAINING WORKERS
# ------------------------------------------------------------------------------

resource "google_service_account" "training_identity" {
  account_id   = "training-sa"
  display_name = "ML Training Pipeline Identity"
}

# ------------------------------------------------------------------------------
# RESOURCE-SCOPED POLICIES (LEAST PRIVILEGE)
# ------------------------------------------------------------------------------

# Access restricted to the specific training bucket
resource "google_storage_bucket_iam_member" "training_bucket_access" {
  bucket = var.bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.training_identity.email}"
}

# Access restricted to the model factory repository
resource "google_artifact_registry_repository_iam_member" "registry_reader" {
  location   = var.region
  repository = var.repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.training_identity.email}"
}

# ------------------------------------------------------------------------------
# PROJECT-WIDE UTILITIES
# ------------------------------------------------------------------------------

resource "google_project_iam_member" "logging_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.training_identity.email}"
}

resource "google_project_iam_member" "batch_agent" {
  project = var.project_id
  role    = "roles/batch.agentReporter"
  member  = "serviceAccount:${google_service_account.training_identity.email}"
}

# --- Lifecycle Management ---
# Allows the worker to delete itself after execution
resource "google_project_iam_member" "compute_admin" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${google_service_account.training_identity.email}"
}
