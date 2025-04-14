# Enable required APIs
resource "google_project_service" "storage" {
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

# Storage bucket
resource "google_storage_bucket" "data_bucket" {
  name          = "sunshine-list-bucket"
  location      = var.region
  storage_class = var.gsc_storage_class
  force_destroy = false

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365
    }
  }

  depends_on = [google_project_service.storage]
}

# Raw/Staging dataset (where your source CSVs or ingestion land)
resource "google_bigquery_dataset" "staging" {
  dataset_id    = "ontario_sunshine_dataset"     # matches schema.yml source.schema
  friendly_name = "Ontario Sunshine Raw Data"
  description   = "Raw staging data for Sunshine List"
  location      = var.region

  depends_on = [google_project_service.bigquery]
}

# DBT development dataset (where dbt writes models)
resource "google_bigquery_dataset" "dbt_dev" {
  dataset_id    = "sunshine_dbt_dev_dataset"     # matches your UI
  friendly_name = "Sunshine DBT Dev Dataset"
  description   = "DBT development dataset for Sunshine List"
  location      = var.region

  depends_on = [google_project_service.bigquery]
}

