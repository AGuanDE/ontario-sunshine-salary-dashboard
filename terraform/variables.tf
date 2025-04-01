variable "project_id" {
  description = "The ID of the GCP project"
  type        = string
}

variable "region" {
  description = "The default region for resources"
  type        = string
}

variable "gcp_credentials_file" {
  description = "Path to the GCP credentials JSON file"
  type        = string
}

variable "gsc_storage_class" {
  description = "Bucket storage class"
  type        = string
  default     = "STANDARD"
}
