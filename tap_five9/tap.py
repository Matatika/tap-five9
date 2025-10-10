import contextlib
import typing as t
from datetime import datetime

from singer_sdk import Tap
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    DateType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
    TimeType,
)

from tap_five9 import client
from tap_five9.streams import (
    DATE_FORMAT,
    DATETIME_FORMAT,
    TIME_FORMAT,
    AgentInformation,
    AgentLoginLogout,
    AgentOccupancy,
    CallLog,
    Five9ApiStream,
)

STREAMS = [
    CallLog,
    AgentLoginLogout,
    AgentOccupancy,
    AgentInformation,
]

NULLABLE_STRING_TYPE = StringType(nullable=True).type_dict

class TapFive9(Tap):
    name: str = "tap-five9"
    config_jsonschema = PropertiesList(
        Property(
            "username",
            StringType(),
            required=True,
            description="Username for Five9",
            secret=True,
        ),
        Property(
            "password",
            StringType(),
            required=True,
            description="Password for Five9",
            secret=True,
        ),
        Property(
            "start_date",
            DateTimeType(),
            required=True,
            description="Starting date to sync data from Five9",
        ),
        Property(
            "region",
            StringType(),
            allowed_values=list(client.REGIONS),
            default="US",
            description="Five9 region",
        ),
        Property(
            "custom_reports",
            ArrayType(
                ObjectType(
                    Property(
                        "folder_name",
                        StringType(),
                        required=True,
                        description="Custom report folder name",
                    ),
                    Property(
                        "report_name",
                        StringType(),
                        required=True,
                        description="Custom report name",
                    ),
                    Property(
                        "stream_name",
                        StringType(),
                        description="Stream name",
                    ),
                    Property(
                        "primary_keys",
                        ArrayType(StringType()),
                        default=[],
                        description="Stream primary keys",
                    ),
                    Property(
                        "replication_key",
                        StringType(),
                        description="Stream replication key (implies incremental sync)",
                    ),
                )
            ),
            default=[],
            description="Custom report stream definitions"
        ),
    ).to_dict()

    def discover_streams(self) -> t.Sequence[Five9ApiStream]:
        streams = [stream_class(tap=self) for stream_class in STREAMS]

        for report_config in self.config["custom_reports"]:
            report_stream = Five9ApiStream(
                tap=self,
                name=(
                    report_config.get("stream_name")
                    or report_config["report_name"].lower().replace(" ", "_")
                ),
                schema={"properties": {}},
            )

            report_stream.primary_keys = report_config["primary_keys"]
            report_stream.replication_key = report_config.get("replication_key")
            report_stream.folder_name = report_config["folder_name"]
            report_stream.report_name = report_config["report_name"]

            properties = {}

            for i, record in enumerate(report_stream.get_records(None)):
                if i >= 10_000:
                    break

                for k, v in record.items():
                    schema = properties.setdefault(k, {})

                    # skip if null; we can't resolve a schema from this
                    if v is None:
                        continue

                    properties[k] = _infer_schema_from_value(v, schema)

            for k in properties.copy():
                if not properties[k]:
                    properties[k] = NULLABLE_STRING_TYPE

            report_stream.schema["properties"] = properties

            # clear cached properties
            del report_stream.int_fields
            del report_stream.boolean_fields
            del report_stream.date_fields
            del report_stream.datetime_fields

            streams.append(report_stream)

        return streams

# https://community.five9.com/s/document-item?language=en_US&bundleId=reports-dashboards&topicId=reports-dashboards/data-sources/_ch-data-sources.htm&_LANG=enus
def _infer_schema_from_value(value: str, schema: dict):
    # nothing to do if already resolved as a string
    if schema == NULLABLE_STRING_TYPE:
        return schema

    # boolean
    if value in ["1", "0", "-"]:
        return BooleanType(nullable=True).type_dict

    # integer
    if value.isdigit():
        return IntegerType(nullable=True).type_dict

    # date
    with contextlib.suppress(ValueError):
        datetime.strptime(value, DATE_FORMAT)
        return DateType(nullable=True).type_dict

    # time
    with contextlib.suppress(ValueError):
        datetime.strptime(value, TIME_FORMAT)
        return TimeType(nullable=True).type_dict

    # date-time
    with contextlib.suppress(ValueError):
        datetime.strptime(value, DATETIME_FORMAT)
        return DateTimeType(nullable=True).type_dict

    return NULLABLE_STRING_TYPE