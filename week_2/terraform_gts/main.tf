// XXX: No idea let what this does...
data "google_storage_transfer_project_service_account" "default" {
  project = var.project
}

// Initializing the specific gcp service on my project I wanna use?
resource "google_project_service" "storagetransfer" {
  project = var.project
  service = "storagetransfer.googleapis.com"
}

// Create (or connect if exists?) to my google storage bucket
resource "google_storage_bucket" "my-transfer-service-terraform" {
  name          = "my-transfer-service-tf-bucket-name"
  storage_class = "STANDARD"
  project       = var.project
  location      = "EU"
}

// I guess this defines in what (IAM) way I which terraform to interact
// with my bucket, set by the `role`?
resource "google_storage_bucket_iam_member" "transfer-service-terraform-iam" {
  bucket     = google_storage_bucket.my-transfer-service-terraform.name
  role       = "roles/storage.admin"
  member     = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
  depends_on = [google_storage_bucket.my-transfer-service-terraform]
}

// Defining my transfer job
resource "google_storage_transfer_job" "s3-bucket-nightly-backup" {
  description = "My Google Transfer Job Run"
  project     = var.project

  transfer_spec {
    // Filter only files/object we want, i.e. 2019 and 2020 yellow taxi trip data
    object_conditions {
      includePrefixes = ["yellow_tripdata_2019", "yellow_tripdata_2020"]
    }
    transfer_options {
      delete_objects_unique_in_sink = false
    }
    // Source
    aws_s3_data_source {
      bucket_name = "nyc-tlc"
      aws_access_key {
        access_key_id     = var.access_key_id
        secret_access_key = var.aws_secret_key
      }
    }
    // Destination
    gcs_data_sink {
      bucket_name = google_storage_bucket.my-transfer-service-terraform.name
      path        = ""
    }
  }

  schedule {
    schedule_start_date {
      year  = 2022
      month = 01
      day   = 21
    }
    schedule_end_date {
      year  = 2022
      month = 01
      day   = 21
    }
  }

  depends_on = [google_storage_bucket_iam_member.transfer-service-terraform-iam]
}
