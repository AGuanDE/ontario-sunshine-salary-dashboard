terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.17.0"
    }
  }
}

provider "google" {
  credentials = file(var.gcp_credentials_file)
  project     = var.project_id
  region      = var.region
}

terraform {
  backend "gcs" {
    bucket  = "sunshine-terraform-state"    # <- the bucket you created
    prefix  = "terraform/state"             # <- folder within the bucket
  }
}
