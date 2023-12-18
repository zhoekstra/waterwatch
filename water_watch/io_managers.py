import os

from dagster import EnvVar, FilesystemIOManager
from dagster_gcp import GCSPickleIOManager, GCSResource
from dagster_gcp_pandas import BigQueryPandasIOManager
ENV = os.getenv('DAGSTER_CLOUD_DEPLOYMENT_NAME', default='unknown')

filesystem_io_manager_key = "io_manager"
filesystem_io_manager = FilesystemIOManager(base_dir=f"data_{ENV}")

bq_io_manager_key = "BQ_IO_MANAGER"
bq_io_manager = BigQueryPandasIOManager(
    project="waterwatch-406910",
    dataset=f"waterwatch_{ENV}",
    timeout=15.0
)

gcs_io_manager_key = "GCS_IO_MANAGER"
gcs_io_manager = GCSPickleIOManager(
    gcs=GCSResource(project="waterwatch-406910"),
    gcs_bucket="dagster_assets",
    gcs_prefix=f"water_watch/{ENV}"
)
