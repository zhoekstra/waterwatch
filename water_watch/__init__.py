import base64
import json
import os

from dagster import Definitions, load_assets_from_modules, FilesystemIOManager, AutoMaterializePolicy, \
    AutoMaterializeRule
from dagster_gcp import GCSResource
from dagster_gcp_pandas import BigQueryPandasIOManager

from . import assets
from .io_managers import gcs_io_manager_key, bq_io_manager_key, gcs_io_manager, bq_io_manager
from dagster_gcp.gcs import gcs_pickle_io_manager, gcs_resource


AUTH_FILE = "/tmp/gcp_creds.json"
with open(AUTH_FILE, "w") as f:
    json.dump(json.loads(base64.b64decode(os.getenv("GCP_CREDS_JSON_CREDS_BASE64"))), f)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = AUTH_FILE

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
