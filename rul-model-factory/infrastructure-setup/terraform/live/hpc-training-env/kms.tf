# ------------------------------------------------------------------------------
# KMS: KEY MANAGEMENT SERVICE (DORA COMPLIANCE)
# ------------------------------------------------------------------------------

resource "google_kms_key_ring" "factory_keyring" {
  name     = "hpc-factory-keyring"
  location = var.region
}

resource "google_kms_crypto_key" "data_encryption_key" {
  name            = "hpc-data-key"
  key_ring        = google_kms_key_ring.factory_keyring.id
  rotation_period = "7776000s" # 90 days

  lifecycle {
    prevent_destroy = true
  }
}

# Grant GCS service agent access to use the key
data "google_storage_project_service_account" "gcs_account" {
  project = var.project_id
}

# Grant Compute Engine service agent access to use the key for disks
data "google_project" "current" {}

resource "google_kms_crypto_key_iam_member" "gcs_kms_member" {
  crypto_key_id = google_kms_crypto_key.data_encryption_key.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
}

resource "google_kms_crypto_key_iam_member" "compute_kms_member" {
  crypto_key_id = google_kms_crypto_key.data_encryption_key.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.current.number}@compute-system.iam.gserviceaccount.com"
}

output "kms_key_id" {
  value = google_kms_crypto_key.data_encryption_key.id
}
