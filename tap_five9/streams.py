from __future__ import annotations

import datetime
import typing as t
from importlib import resources as importlib_resources

import dateutil.parser as parser
import pendulum
import pytz
import singer
from singer.utils import strftime as singer_strftime
from singer_sdk import Stream, Tap

from tap_five9.client import Five9API

LOGGER = singer.get_logger()
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"


class Five9ApiStream(Stream):
    name = None
    stream = None
    replication_method = None
    replication_key = None
    key_properties = None
    folder_name = None
    report_name = None
    results_key = 'records'
    datetime_fields = []
    int_fields = []

    def __init__(self, tap: Tap, schema=None,
                 name: str | None = None) -> None:
        super().__init__(tap, schema, name)

        self.client = Five9API(self.config)

    def transform_value(self, key, value):
        if key in self.datetime_fields and value:
            value = parser.parse(value)
            value = value.replace(tzinfo=pytz.utc)
            # reformat to use RFC3339 format
            value = singer_strftime(value)

        if key in self.int_fields and value:
            value = int(value)

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

        start_date = self.get_starting_timestamp(context)

        params = {
            'folder_name': self.folder_name,
            'report_name': self.report_name,
            'start': start_date.strftime('%Y-%m-%dT%H:%M:%S'),
            'end': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        }

        data = self.client.return_report_results(params)
        for row in data:
            record = {k: self.transform_value(k, v) for (k, v) in row.items()}
            yield record


class CallLog(Five9ApiStream):
    name = 'call_log'
    stream = 'call_log'
    replication_method = 'INCREMENTAL'
    replication_key = 'timestamp'
    primary_keys = ('call_id',)
    folder_name = 'Call Log Reports'
    report_name = 'Call Log'
    datetime_fields = {'timestamp'}
    int_fields = {'transfers', 'conferences', 'holds', 'abandoned'}
    schema_filepath = SCHEMAS_DIR / "call_log.json"


class AgentLoginLogout(Five9ApiStream):
    name = 'agent_login_logout'
    stream = 'agent_login_logout'
    replication_method = 'INCREMENTAL'
    replication_key = 'date'
    primary_keys = ('agent', 'date',)
    folder_name = 'Agent Reports'
    report_name = 'Agent Login-Logout'
    datetime_fields = {'date', 'login_timestamp', 'logout_timestamp'}
    schema_filepath = SCHEMAS_DIR / "agent_login_logout.json"


class AgentOccupancy(Five9ApiStream):
    name = 'agent_occupancy'
    stream = 'agent_occupancy'
    replication_method = 'INCREMENTAL'
    replication_key = 'date'
    primary_keys = ('agent', 'date',)
    folder_name = 'Agent Reports'
    report_name = 'Agent Occupancy'
    datetime_fields = {'date'}
    schema_filepath = SCHEMAS_DIR / "agent_occupancy.json"


class AgentInformation(Five9ApiStream):
    name = 'agent_information'
    stream = 'agent_information'
    replication_method = 'FULL_TABLE'
    int_fields = {'agent_id', 'agent_start_year'}
    primary_keys = ('agent_id',)
    folder_name = 'Agent Reports'
    report_name = 'Agents Information'
    schema_filepath = SCHEMAS_DIR / "agent_information.json"
