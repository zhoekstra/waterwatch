resource "google_bigquery_dataset" "waterwatch" {
  dataset_id                  = "waterwatch_${var.env}"
  friendly_name               = "WaterWatch Dataset"
  description                 = ""
  location                    = "US"

  labels = {
    env = var.env
  }
}

resource "google_bigquery_table" "default" {
  dataset_id = google_bigquery_dataset.waterwatch.dataset_id
  friendly_name = "site_flow_information_${var.env}"
  table_id   = "site_flow_information_${var.env}"

  time_partitioning {
    field = "_runtime"
    type = "HOUR"
  }

  labels = {
    env = var.env
  }

  schema = <<-EOT
    [
      {
        "name": "site_no",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "The Site ID"
      },
      {
        "name": "station_nm",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Station Name"
      },
      {
        "name": "dec_lat_va",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Site Location latitude"
      },
      {
        "name": "dec_long_va",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Site Location Longitude"
      },
      {
        "name": "huc_cd",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": ""
      },
      {
        "name": "tz_cd",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": ""
      },
      {
        "name": "flow",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "The flow rating, or null if there is a status code preventing measurement"
      },
      {
        "name": "flow_status",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "The status of the flow meter. Nrm is normal operation"
      },
      {
        "name": "flow_unit",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": ""
      },
      {
        "name": "flow_dt",
        "type": "DATETIME",
        "mode": "NULLABLE",
        "description": ""
      },
      {
        "name": "stage",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": ""
      },
      {
        "name": "stage_status",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "The status of the stage meter. Nrm is normal operation"
      },
      {
        "name": "stage_unit",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": ""
      },
      {
        "name": "stage_dt",
        "type": "DATETIME",
        "mode": "NULLABLE",
        "description": ""
      },
      {
        "name": "class",
        "type": "INT64",
        "mode": "NULLABLE",
        "description": ""
      },
      {
        "name": "percentile",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": ""
      },
      {
        "name": "percent_median",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": ""
      },
      {
        "name": "percent_mean",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": ""
      },
      {
        "name": "_state",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": ""
      },
      {
        "name": "_runtime",
        "type": "DATETIME",
        "mode": "REQUIRED",
        "description": ""
      }
    ]
    EOT

}