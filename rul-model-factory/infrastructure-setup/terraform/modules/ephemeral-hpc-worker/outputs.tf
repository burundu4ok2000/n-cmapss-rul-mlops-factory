# ------------------------------------------------------------------------------
# OUTPUTS: EXPORTING RELEVANT ENDPOINTS
# ------------------------------------------------------------------------------

output "bucket_url" {
  description = "The URL of the created GCS bucket"
  value       = google_storage_bucket.training_assets.url
}

output "artifact_registry_uri" {
  description = "The full URI for the Docker repository"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.factory.repository_id}"
}

output "service_account_email" {
  description = "The email of the pipeline service account"
  value       = google_service_account.training_identity.email
}

output "instance_template_link" {
  description = "The self-link of the instance template"
  value       = google_compute_instance_template.training_node.self_link
}
