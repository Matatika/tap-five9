from __future__ import annotations

import datetime
import typing as t
from functools import cached_property
from importlib import resources as importlib_resources

import pendulum
import singer
from singer.utils import strftime as singer_strftime
from singer_sdk import Stream, Tap

from tap_five9 import client

LOGGER = singer.get_logger()
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"
DATE_FORMAT = r"%a, %d %b %Y"
TIME_FORMAT = r"%H:%M:%S"
DATETIME_FORMAT = r"%a, %d %b %Y %H:%M:%S"


class Five9ApiStream(Stream):
    name = None
    stream = None
    folder_name = None
    report_name = None
    results_key = 'records'

    def __init__(self, tap: Tap, schema=None,
                 name: str | None = None) -> None:
        super().__init__(tap, schema, name)

        self.client = client.Five9API(self.config)
        self.timezone = client.REGIONS[self.config["region"]].timezone

    @cached_property
    def date_fields(self):
        return {k for k, v in self.schema["properties"].items() if v.get("format") == "date"}

    @cached_property
    def datetime_fields(self):
        return {k for k, v in self.schema["properties"].items() if v.get("format") == "date-time"}

    @cached_property
    def int_fields(self):
        return {k for k, v in self.schema["properties"].items() if "integer" in v["type"]}

    @cached_property
    def boolean_fields(self):
        return {k for k, v in self.schema["properties"].items() if "boolean" in v["type"]}

    # https://community.five9.com/s/document-item?language=en_US&bundleId=reports-dashboards&topicId=reports-dashboards/data-sources/_ch-data-sources.htm&_LANG=enus
    def transform_value(self, key, value):
        if value == "":
            return None

        if key in self.date_fields and value:
            return datetime.datetime.strptime(DATE_FORMAT, value).isoformat()

        if key in self.datetime_fields and value:
            value = datetime.datetime.strptime(value, DATETIME_FORMAT)
            # convert timezone-naive to timezone-aware, based on region
            value = value.astimezone(self.timezone)
            # convert to utc
            value = value.astimezone(datetime.timezone.utc)
            # reformat to use RFC3339 format
            return singer_strftime(value)

        if key in self.int_fields and value:
            return int(value)

        if key in self.boolean_fields and value:
            if value == "1":
                return True
            if value == "0":
                return False
            if value == "-":
                return None

        return value

    def get_starting_timestamp(self, context: dict | None) -> datetime.datetime | None:
        state = self.get_context_state(context)
        value = self.config['start_date']
        if state:
            if state['starting_replication_value'] is not None:
                value = state['starting_replication_value']

        if value is None:
            return None

        return pendulum.parse(value)

    def get_records(
            self,
            context: dict | None,
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:

        params = {
            "folder_name": self.folder_name,
            "report_name": self.report_name,
        }

        start_date = self.get_starting_timestamp(context)

        while True:
            # period of one day seems to be generally within CSV report result limit
            end_date = start_date + datetime.timedelta(days=1)

            if end_date > datetime.datetime.now(tz=datetime.timezone.utc):
                end_date = None

            data = self.client.return_report_results(
                {
                    **params,
                    "start": start_date,
                    "end": end_date,
                }
            )

            i = 0
            for i, row in enumerate(data, start=1):
                record = {k: self.transform_value(k, v) for (k, v) in row.items()}
                yield record

            if i >= 50000:
                LOGGER.warning("CSV report result limit of 50,000 reached - data may be missing")

            self._finalize_state(self.stream_state)

            if end_date is None:
                break

            start_date = end_date


class CallLog(Five9ApiStream):
    name = 'call_log'
    stream = 'call_log'
    replication_method = 'INCREMENTAL'
    replication_key = 'timestamp'
    primary_keys = ('call_id',)
    folder_name = 'Call Log Reports'
    report_name = "Call Log"
    schema_filepath = SCHEMAS_DIR / "call_log.json"


class AgentLoginLogout(Five9ApiStream):
    name = 'agent_login_logout'
    stream = 'agent_login_logout'
    replication_method = 'INCREMENTAL'
    replication_key = 'date'
    primary_keys = ('agent', 'date',)
    folder_name = 'Agent Reports'
    report_name = "Agent Login-Logout"
    schema_filepath = SCHEMAS_DIR / "agent_login_logout.json"


class AgentOccupancy(Five9ApiStream):
    name = 'agent_occupancy'
    stream = 'agent_occupancy'
    replication_method = 'INCREMENTAL'
    replication_key = 'date'
    primary_keys = ('agent', 'date',)
    folder_name = 'Agent Reports'
    report_name = "Agent Occupancy"
    schema_filepath = SCHEMAS_DIR / "agent_occupancy.json"


class AgentInformation(Five9ApiStream):
    name = 'agent_information'
    stream = 'agent_information'
    replication_method = "FULL_TABLE"
    primary_keys = ('agent_id',)
    folder_name = 'Agent Reports'
    report_name = 'Agents Information'
    schema_filepath = SCHEMAS_DIR / "agent_information.json"
