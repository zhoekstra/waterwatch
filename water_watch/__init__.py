import base64
import json
import os

from dagster import Definitions, load_assets_from_modules, AutoMaterializePolicy, \
    AutoMaterializeRule
from dagster_gcp import GCSResource

from . import assets
from .io_managers import gcs_io_manager_key, bq_io_manager_key, gcs_io_manager, bq_io_manager, filesystem_io_manager

AUTH_FILE = "./gcp_creds.json"
with open(AUTH_FILE, "w") as f:
    base64creds = os.getenv("GCP_CREDS_JSON_CREDS_BASE64")
    creds_str = json.loads(base64.b64decode(base64creds)) if base64creds else ''
    json.dump(creds_str, f)
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
        "io_manager": filesystem_io_manager,
        gcs_io_manager_key: gcs_io_manager,
        bq_io_manager_key: bq_io_manager,
    },
)
