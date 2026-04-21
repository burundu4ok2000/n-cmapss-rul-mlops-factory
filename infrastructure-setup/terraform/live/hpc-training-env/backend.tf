# ------------------------------------------------------------------------------
# BACKEND: WHERE THE MEMORY LIVES
# ------------------------------------------------------------------------------

terraform {
  backend "gcs" {
    # ARHITECTURAL GUARDRAIL:
    # The bucket name is intentionally set to a non-existent placeholder.
    # This acts as a FAIL-SAFE to prevent manual 'terraform init' bypass.
    # SUCCESSFUL INITIALIZATION REQUIRES the orchestrator:
    #   terraform init -backend-config="bucket=ncmapss-terraform-state-${GCP_PROJECT_ID}"
    #
    # SSOT: Environment identity is managed by the .env / Orchestration layer.
    bucket = "ncmapss-terraform-state-REPLACE_ME"
    prefix = "terraform/state/gcp-rul-training"
  }
}
