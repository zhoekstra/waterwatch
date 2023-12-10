from datetime import timezone

import dateutil.parser
import pandas
import requests
from dagster import asset, MultiPartitionsDefinition, StaticPartitionsDefinition, HourlyPartitionsDefinition, \
    AssetExecutionContext, build_asset_context, MultiPartitionKey

from water_watch.stateflow_schema import SiteFlowInformation, SiteFlowFile, NULL_DATETIME_STRING
from water_watch.io_managers import gcs_io_manager_key, bq_io_manager_key

HourlyStatePartititonDefenition = MultiPartitionsDefinition({
    "state": StaticPartitionsDefinition(["co"]),
    "date": HourlyPartitionsDefinition(start_date="2023-12-10-07:00"),
})


@asset(
    io_manager_key=gcs_io_manager_key,
    partitions_def=HourlyStatePartititonDefenition)
def current_flow_data_raw(context: AssetExecutionContext) -> str:
    partition: dict[str, str] = context.partition_key.keys_by_dimension
    r = requests.get(f"https://waterwatch.usgs.gov/webservices/realtime?region={partition['state']}&format=json")
    return r.text


@asset(
    partitions_def=HourlyStatePartititonDefenition)
def current_flow_data_parsed(current_flow_data_raw: str) -> list[SiteFlowInformation]:
    return SiteFlowFile.from_json(current_flow_data_raw).sites


@asset(
    io_manager_key=bq_io_manager_key,
    partitions_def=HourlyStatePartititonDefenition,
    metadata={"partition_expr": {'date': '_runtime', 'state': '_state'}})
def site_flow_information(context: AssetExecutionContext,
                          current_flow_data_parsed: list[SiteFlowInformation]) -> pandas.DataFrame:
    partition: dict[str, str] = context.partition_key.keys_by_dimension
    result = pandas.DataFrame(current_flow_data_parsed)
    result.rename(columns={'class_': 'class'})
    # pull flow/stage status codes out into a separate column
    result['flow_status'] = result['flow'].apply(lambda f: f if isinstance(f, str) else 'Nrm')
    result['flow'] = result['flow'].apply(lambda f: None if isinstance(f, str) else f)
    result['stage_status'] = result['stage'].apply(lambda s: s if isinstance(s, str) else 'Nrm')
    result['stage'] = result['stage'].apply(lambda s: None if isinstance(s, str) else s)
    # filter out null datetimes and parse the rest
    result['flow_dt'] = result['flow_dt'].apply(
        lambda t: None if t == NULL_DATETIME_STRING else dateutil.parser.isoparse(t).astimezone(timezone.utc))
    result['stage_dt'] = result['stage_dt'].apply(
        lambda t: None if t == NULL_DATETIME_STRING else dateutil.parser.isoparse(t).astimezone(timezone.utc))
    # parse non-null percent_*
    result['percent_median'] = result['percent_median'].apply(lambda f: float(f) if f else None)
    result['percent_mean'] = result['percent_mean'].apply(lambda f: float(f) if f else None)
    # add partition columns
    result['_state'] = partition['state']
    result['_runtime'] = dateutil.parser.isoparse(partition['date']).astimezone(timezone.utc)
    return result
