output "bucket_url" {
  description = "The URL of the created GCS bucket"
  value       = google_storage_bucket.data_bucket.url
}

output "dataset_id" {
  description = "The ID of the created BigQuery dataset"
  value       = google_bigquery_dataset.dataset.id
}