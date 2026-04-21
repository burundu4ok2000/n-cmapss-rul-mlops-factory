# ------------------------------------------------------------------------------
# VARIABLES: MODULE INPUTS
# ------------------------------------------------------------------------------

variable "project_id" {
  description = "The Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "The GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Globally unique name for the GCS bucket"
  type        = string
}

variable "machine_type" {
  description = "Machine type for the training node"
  type        = string
  default     = "c2d-standard-32"
}

variable "repository_id" {
  description = "ID for the Artifact Registry repository"
  type        = string
  default     = "ml-factory"
}

variable "disk_size_gb" {
  description = "Boot disk size in GB"
  type        = number
  default     = 500
}

variable "kms_key_id" {
  description = "Self-managed encryption key for data-at-rest protection (CMEK)"
  type        = string
  default     = null
}

variable "fast_forward_source" {
  description = "Optional Run ID to recycle artifacts from GCS"
  type        = string
  default     = ""
}
