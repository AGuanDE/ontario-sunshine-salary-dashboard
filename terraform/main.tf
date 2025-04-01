# Enable required APIs
resource "google_project_service" "storage" {
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

# Storage bucket (matches existing configuration)
resource "google_storage_bucket" "data_bucket" {
  name          = "sunshine-list-bucket"
  location      = var.region
  storage_class = var.gsc_storage_class
  force_destroy = false # Keep versioning behavior as-is

  versioning {
    enabled = true # Soft delete behavior
  }

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365 # Optional: delete versions older than 1 year
    }
  }

  depends_on = [google_project_service.storage]
}

# BigQuery dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id    = "sunshine_clean"
  friendly_name = "Sunshine Clean Data"
  description   = "Cleaned and normalized Ontario Sunshine List data"
  location      = var.region

  depends_on = [google_project_service.bigquery]
}
