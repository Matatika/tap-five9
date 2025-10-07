import typing as t

from singer_sdk import Tap
from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)
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
        Property("region", StringType(), allowed_values=list(client.REGIONS), default="US", description="Five9 region"),
        Property("custom_reports", ArrayType(
            ObjectType(
                Property("name", StringType(), required=True, description="Stream name"),
                Property("schema", ObjectType(additional_properties=True), default={"properties": {}}, description="Stream schema"),
                Property("primary_keys", ArrayType(StringType()), default=[], description="Stream primary keys"),
                Property("replication_key", StringType(), description="Stream replication key (implies incremental sync)"),
                Property("folder_name", StringType(), required=True, description="Custom report folder name"),
                Property("report_name", StringType(), required=True, description="Custom report name"),
                Property("datetime_fields", ArrayType(StringType()), default=[], description="Date-time schema fields"),
                Property("int_fields", ArrayType(StringType()), default=[], description="Integer schema fields"),
            )
        ))
    ).to_dict()

    def discover_streams(self) -> t.Sequence[Five9ApiStream]:
        streams = [stream_class(tap=self) for stream_class in STREAMS]

        for report_config in self.config["custom_reports"]:
            report_stream = Five9ApiStream(
                tap=self,
                name=report_config["name"],
                schema=report_config["schema"],
            )

            report_stream.primary_keys = report_config["primary_keys"]
            report_stream.replication_key = report_config.get("replication_key")
            report_stream.folder_name = report_config["folder_name"]
            report_stream.report_name = report_config["report_name"]
            report_stream.datetime_fields = report_config["datetime_fields"]
            report_stream.datetime_fields = report_config["int_fields"]

            streams.append(report_stream)

        return streams
