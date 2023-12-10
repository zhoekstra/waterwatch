from dagster import EnvVar
from dagster_gcp import GCSPickleIOManager, GCSResource
from dagster_gcp_pandas import BigQueryPandasIOManager

bq_io_manager_key = "BQ_IO_MANAGER"
bq_io_manager = BigQueryPandasIOManager(
    project="waterwatch-406910",
    dataset="waterwatch_dev",
    timeout=15.0
)

gcs_io_manager_key = "GCS_IO_MANAGER"
gcs_io_manager = GCSPickleIOManager(
    gcs=GCSResource(project="waterwatch-406910"),
    gcs_bucket="dagster_assets",
    gcs_prefix="water_watch"
)
