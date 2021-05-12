from typing import Any, Iterator

import singer
from singer import Transformer, metrics, metadata
from singer.catalog import Schema

from tap_dynamics.client import DynamicsClient
from tap_dynamics.transform import flatten_entity_attributes


LOGGER = singer.get_logger()

STRING_TYPES = set([
    'Customer', # TODO: not present in demo data
    'LookupType',
    'MemoType',
    'Owner', # TODO: not present in demo data
    'PartyList',
    'PicklistType',
    'StateType',
    'StatusType',
    'StringType',
    'UniqueidentifierType',
    'CalendarRules', # TODO: need to confirm; not present in demo data
    'ManagedPropertyType',
    'EntityNameType',
    ])

INTEGER_TYPES = set([
    'IntegerType',
    'BigIntType',
])

NUMBER_TYPES = set([
    'DecimalType',
    'DoubleType',
    'MoneyType',
    ])

DATE_TYPES = set(['DateTimeType'])

BOOL_TYPES = set(['BooleanType'])

COMPLEX_TYPES = set([
    'ImageType', # From Virtual `AttributeType`
    'MultiSelectPicklistType', # From Virtual `AttributeType`
    ])

class BaseStream:
    """
    A base class representing singer streams.

    :param client: The API client used extract records from the external source
    """
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    params = {}
    parent = None

    def __init__(self, client: DynamicsClient):
        self.client = client

    def get_records(self, config: dict = None, is_parent: bool = False) -> list:
        """
        Returns a list of records for that stream.

        :param config: The tap config file
        :param is_parent: If true, may change the type of data
            that is returned for a child stream to consume
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require implementation")

    def set_parameters(self, params: dict) -> None:
        """
        Sets or updates the `params` attribute of a class.

        :param params: Dictionary of parameters to set or update the class with
        """
        self.params = params

    def get_parent_data(self, config: dict = None) -> list:
        """
        Returns a list of records from the parent stream.

        :param config: The tap config file
        :return: A list of records
        """
        # pylint: disable=not-callable
        parent = self.parent(self.client) 
        return parent.get_records(config, is_parent=True)


class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'
    batched = False

    def __init__(self, client):
        super().__init__(client)

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict, config: dict, transformer: Transformer) -> dict:
        """
        The sync logic for an incremental stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        start_time = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        max_record_value = start_time

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(config):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                record_replication_value = singer.utils.strptime_to_utc(transformed_record[self.replication_key])
                if record_replication_value >= singer.utils.strptime_to_utc(max_record_value):
                    singer.write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_record_value = record_replication_value.isoformat()

        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_record_value)
        singer.write_state(state)
        return state


class FullTableStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    FULL_TABLE replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'FULL_TABLE'

    def __init__(self, client):
        super().__init__(client)

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict, config: dict, transformer: Transformer) -> dict:
        """
        The sync logic for an full table stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(config):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()

        singer.write_state(state)
        return state


class SampleStream(FullTableStream):
    """
    Gets records for a sample stream.
    """
    tap_stream_id = 'sample_stream'
    key_properties = ['id']

    def get_records(self, config=None, is_parent=False):
        sample_data = [{
            'string_field': 'some string',
            'datetime_field': '2021-04-23T17:05:41.762537+00:00',
            'integer_field': 3,
            'double_field': 22.78,
        }]

        yield from sample_data

REPLICATION_TO_STREAM_MAP = {
    'INCREMENTAL': IncrementalStream,
    'FULL_TABLE': FullTableStream
}

STREAMS = {}

def get_streams(config):
    global STREAMS

    client = DynamicsClient(**config)

    # dynamically build streams by iterating over entities and calling build_schema()
    for stream in client.build_entity_metadata():
        stream_name = stream.get('LogicalName')

        attributes = flatten_entity_attributes(stream.get('Attributes'))

        if 'modifiedon' in attributes.keys() or 'createdon' in attributes.keys():
            replication_method = 'INCREMENTAL'
            if 'modifiedon' in attributes.keys():
                replication_key = 'modifiedon'
            elif 'createdon' in attributes.keys():
                replication_key = 'createdon'
        else: replication_method = 'FULL_TABLE'
       
        # Instantiate an object for each stream Class with requisite metadata
        stream_class = REPLICATION_TO_STREAM_MAP.get(replication_method)
        stream_obj = stream_class(client)

        # set class attributes for each stream
        stream_obj.tap_stream_id = stream_name
        stream_obj.key_properties = [stream_name + 'id']        
                
        if replication_method == 'INCREMENTAL':
            stream_obj.replication_key = replication_key
            stream_obj.valid_replication_keys = ['modifiedon', 'createdon']

        # 
        stream_obj.schema = build_schema(attributes)

        STREAMS.update({stream_name: stream_obj})

    return STREAMS

def build_schema(attributes: dict):
    json_props = {}

    for attr_name, attr_props in attributes.items():
        if not attr_props.get('is_readable'):
            continue

        dyn_type = attr_props.get('type')
        json_type = 'string'
        json_format = None

        if dyn_type in DATE_TYPES:
            json_format = 'date-time'
        elif dyn_type in INTEGER_TYPES:
            json_type = 'integer'
        elif dyn_type in NUMBER_TYPES:
            json_type = 'number'
        elif dyn_type in BOOL_TYPES:
            json_type = 'boolean'
        elif dyn_type in COMPLEX_TYPES:
            continue

        prop_json_schema = {
            'type': ['null', json_type]
        }

        if json_format:
            prop_json_schema['format'] = json_format

        json_props[attr_name] = prop_json_schema

    schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': json_props
    }

    return schema