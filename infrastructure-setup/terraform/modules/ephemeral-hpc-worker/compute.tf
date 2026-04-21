# ------------------------------------------------------------------------------
# COMPUTE: THE SELF-DESTROYING TRAINING NODE
# ------------------------------------------------------------------------------

resource "google_compute_instance_template" "training_node" {
  name_prefix  = "ephemeral-train-template-"
  machine_type = var.machine_type
  region       = var.region

  disk {
    source_image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
    auto_delete  = true
    boot         = true
    disk_size_gb = var.disk_size_gb
    disk_type    = "pd-ssd"

    disk_encryption_key {
      kms_key_self_link = var.kms_key_id
    }
  }

  scheduling {
    on_host_maintenance = "MIGRATE"
    automatic_restart   = false
  }

  network_interface {
    network = "default"
    access_config {
      network_tier = "PREMIUM"
    }
  }

  service_account {
    email  = google_service_account.training_identity.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata = {
    startup-script = templatefile("${path.module}/scripts/startup.sh.tftpl", {
      project_id          = var.project_id
      bucket_name         = google_storage_bucket.training_assets.name
      region              = var.region
      repo_id             = google_artifact_registry_repository.factory.repository_id
      fast_forward_source = var.fast_forward_source
    })
  }

  lifecycle {
    create_before_destroy = true
  }
}
