variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
}

variable "bucket_name" {
  description = "GCS Bucket for training"
  type        = string
}

variable "machine_type" {
  description = "GCE Machine type"
  type        = string
}

variable "repository_id" {
  description = "Artifact Registry ID"
  type        = string
}

variable "disk_size_gb" {
  description = "Boot disk size in GB"
  type        = number
  default     = 500
}

variable "fast_forward_source" {
  description = "Optional Run ID to recycle artifacts from GCS"
  type        = string
  default     = ""
}
