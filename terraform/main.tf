provider "google" {
  project = "waterwatch-406910"
  region  = "us-central1"
  zone    = "us-central1-c"
}

terraform {
 backend "gcs" {
   bucket  = "waterwatch-terraform"
   prefix  = "terraform/state"
 }
}