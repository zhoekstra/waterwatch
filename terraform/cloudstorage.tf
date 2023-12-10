resource "google_storage_bucket" "dagster_assets" {
  name          = "dagster_assets_${var.env}"
  location      = "US"
  force_destroy = true
}