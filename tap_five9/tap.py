import typing as t

from singer_sdk import Tap
from singer_sdk.typing import PropertiesList, Property, StringType, DateTimeType

from tap_five9 import client
from tap_five9.streams import CallLog, AgentLoginLogout, AgentOccupancy, AgentInformation, Five9ApiStream

STREAMS = [
    CallLog,
    AgentLoginLogout,
    AgentOccupancy,
    AgentInformation
]


class TapFive9(Tap):
    name: str = 'tap-five9'
    config_jsonschema = PropertiesList(
        Property("username", StringType(), required=True, description="Username for Five9", secret=True),
        Property("password", StringType(), required=True, description="Password for Five9", secret=True),
        Property("start_date", DateTimeType(), required=True, description="Starting date to sync data from Five9"),
        Property("region", StringType(), allowed_values=list(client.HOSTS), default="US", description="Five9 region"),
    ).to_dict()

    def discover_streams(self) -> t.Sequence[Five9ApiStream]:
        return [stream_class(tap=self) for stream_class in STREAMS]
