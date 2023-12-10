import os

from dagster import Definitions, load_assets_from_modules, FilesystemIOManager, AutoMaterializePolicy, \
    AutoMaterializeRule
from dagster_gcp import GCSResource
from dagster_gcp_pandas import BigQueryPandasIOManager

from . import assets
from .io_managers import gcs_io_manager_key, bq_io_manager_key, gcs_io_manager, bq_io_manager
from dagster_gcp.gcs import gcs_pickle_io_manager, gcs_resource

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/zach/PycharmProjects/waterwatch/credentials.json'

all_assets = load_assets_from_modules(
    modules=[assets],
    auto_materialize_policy=
    AutoMaterializePolicy.eager().with_rules(
        AutoMaterializeRule.materialize_on_missing())
)

defs = Definitions(
    assets=all_assets,
    resources={
        "gcs": GCSResource(project="waterwatch-406910"),
        "io_manager": FilesystemIOManager(base_dir="local_data"),
        gcs_io_manager_key: gcs_io_manager,
        bq_io_manager_key: bq_io_manager,
    },
)
