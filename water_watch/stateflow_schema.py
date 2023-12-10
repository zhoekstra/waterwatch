from dataclasses import dataclass, field
from datetime import datetime
from typing import Union, Optional

from dataclasses_json import config, DataClassJsonMixin
from marshmallow import fields

NULL_DATETIME_STRING = "0000-00-00 00:00:00"


@dataclass(frozen=True)
class SiteFlowInformation(DataClassJsonMixin):
    site_no: str
    station_nm: str
    dec_lat_va: float
    dec_long_va: float
    huc_cd: str
    tz_cd: str
    flow: Union[str, float]
    flow_unit: str
    flow_dt: Optional[str]
    stage: Union[str, float]
    stage_unit: str
    stage_dt: Optional[str]
    class_: int = field(metadata=config(field_name="class"))
    percentile: float
    percent_median: Optional[str]
    percent_mean: Optional[str]
    url: str


@dataclass(frozen=True)
class SiteFlowFile(DataClassJsonMixin):
    sites: list[SiteFlowInformation]


@dataclass(frozen=True)
class SiteFlowAverageInformation(DataClassJsonMixin):
    site_no: str
    station_nm: str
    dec_lat_va: float
    dec_long_va: float
    huc_cd: str
    tz_cd: str
    flow: Union[str, float]
    flow_dt: Optional[str]
    flow_unit: str
    flow_nday: int
    class_: int = field(metadata=config(field_name="class"))
    percent_median: Optional[str]
    percent_mean: Optional[str]


@dataclass(frozen=True)
class SiteFlowAverageFile(DataClassJsonMixin):
    sites: list[SiteFlowAverageInformation]
