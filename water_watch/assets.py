from datetime import timezone

import dateutil.parser
import pandas
import requests
from dagster import asset, MultiPartitionsDefinition, StaticPartitionsDefinition, HourlyPartitionsDefinition, \
    AssetExecutionContext, WeeklyPartitionsDefinition, AssetIn, MultiToSingleDimensionPartitionMapping

from water_watch.stateflow_schema import SiteFlowInformation, SiteFlowFile, NULL_DATETIME_STRING, \
    SiteFlowAverageInformation, SiteFlowAverageFile
from water_watch.io_managers import gcs_io_manager_key, bq_io_manager_key

StatePartitionDefenition = StaticPartitionsDefinition(["co"])

HourlyStatePartititonDefenition = MultiPartitionsDefinition({
    "state": StaticPartitionsDefinition(["co"]),
    "date": HourlyPartitionsDefinition(start_date="2023-12-16-07:00"),
})

WeeklyStatePartititonDefenition = MultiPartitionsDefinition({
    "state": StaticPartitionsDefinition(["co"]),
    "date": WeeklyPartitionsDefinition(start_date="2023-12-01-00:00"),
})


@asset(
    io_manager_key=gcs_io_manager_key,
    partitions_def=HourlyStatePartititonDefenition)
def current_flow_data_raw(context: AssetExecutionContext) -> str:
    partition: dict[str, str] = context.partition_key.keys_by_dimension
    r = requests.get(f"https://waterwatch.usgs.gov/webservices/realtime?region={partition['state']}&format=json")
    return r.text


@asset(
    io_manager_key=gcs_io_manager_key,
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


@asset(
    io_manager_key=gcs_io_manager_key,
    partitions_def=WeeklyStatePartititonDefenition)
def flow_data_7d_raw(context: AssetExecutionContext) -> str:
    partition: dict[str, str] = context.partition_key.keys_by_dimension
    r = requests.get(f"https://waterwatch.usgs.gov/webservices/flows7d?region={partition['state']}&format=json")
    return r.text


@asset(
    io_manager_key=gcs_io_manager_key,
    partitions_def=WeeklyStatePartititonDefenition)
def flow_data_7d_parsed(flow_data_7d_raw: str) -> list[SiteFlowAverageInformation]:
    return SiteFlowAverageFile.from_json(flow_data_7d_raw).sites


@asset(
    io_manager_key=bq_io_manager_key,
    partitions_def=WeeklyStatePartititonDefenition,
    metadata={"partition_expr": {'date': '_runtime', 'state': '_state'}})
def site_flow_7d_information(context: AssetExecutionContext,
                             flow_data_7d_parsed: list[SiteFlowAverageInformation]) -> pandas.DataFrame:

    context.log.warning("flow_data_7d_parsed: %s", flow_data_7d_parsed)
    partition: dict[str, str] = context.partition_key.keys_by_dimension
    result = pandas.DataFrame(flow_data_7d_parsed)
    result.rename(columns={'class_': 'class'})
    # pull flow/stage status codes out into a separate column
    result['flow_status'] = result['flow'].apply(lambda f: f if isinstance(f, str) else 'Nrm')
    result['flow'] = result['flow'].apply(lambda f: None if isinstance(f, str) else f)
    # filter out null datetimes and parse the rest
    result['flow_dt'] = result['flow_dt'].apply(
        lambda t: None if t == NULL_DATETIME_STRING or not t else dateutil.parser.isoparse(t).astimezone(timezone.utc))
    # parse non-null percent_*
    result['percent_median'] = result['percent_median'].apply(lambda f: float(f) if f else None)
    result['percent_mean'] = result['percent_mean'].apply(lambda f: float(f) if f else None)
    # add partition columns
    result['_state'] = partition['state']
    result['_runtime'] = dateutil.parser.isoparse(partition['date']).astimezone(timezone.utc)
    return result


@asset(
    io_manager_key=bq_io_manager_key,
    metadata={"partition_expr": {'date': '_runtime', 'state': '_state'}},
    partitions_def=StaticPartitionsDefinition(["co"]),
    ins={"site_flow_7d_information":
             AssetIn(["site_flow_7d_information"],
                     partition_mapping=MultiToSingleDimensionPartitionMapping(partition_dimension_name="state")),
         "site_flow_information":
             AssetIn(["site_flow_information"],
                     partition_mapping=MultiToSingleDimensionPartitionMapping(partition_dimension_name="state"))
         })
def sites(site_flow_7d_information: pandas.DataFrame, site_flow_information: pandas.DataFrame) -> pandas.DataFrame:
    columns_to_retain = ['site_no', 'station_nm',''
                         'dec_lat_va', 'dec_long_va',
                         'huc_cd', 'class_']
    reduced_7d = site_flow_7d_information[columns_to_retain]
    reduced_flow = site_flow_information[columns_to_retain]
    return pandas.concat([reduced_7d, reduced_flow], ignore_index=True).drop_duplicates()
