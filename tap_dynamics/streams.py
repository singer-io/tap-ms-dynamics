import singer
from singer import Transformer, metrics

from tap_dynamics.client import DynamicsClient
from tap_dynamics.transform import (flatten_entity_attributes, get_client_args)

LOGGER = singer.get_logger()

MAX_PAGESIZE = 5000

EXCLUDED_ENTITIES = set([
    'MetadataBase',
    'calendarrule',
    'complexcontrol',
    'dataperformance',
    'knowledgearticlescategories',
    'languageprovisioningstate',
    'managedproperty',
    'msdyn_nonrelationalds',
    'officegraphdocument',
    'optionset',
    'organizationdatasyncsubscription',
    'postregarding',
    'ribbonmetadatatoprocess',
    'runtimedependency',
    'similarityrule',
    'subscriptionmanuallytrackedobject',
    'subscriptionstatisticsoutlook',
    'subscriptionsyncentryoffline',
    'subscriptionsyncentryoutlook',
    'systemusersyncmappingprofiles',
    'teamsyncattributemappingprofiles',
    'timestampdatemapping',
    'lookupmapping',
    'msdyn_casesuggestion',
    'msdyn_knowledgearticlesuggestion'
])

STRING_TYPES = set([
    'Edm.String',
    'Edm.Guid',
    ])

INTEGER_TYPES = set([
    'Edm.Int32',
    'Edm.Int64',
])

NUMBER_TYPES = set([
    'Edm.Decimal',
    'Edm.Double',
    ])

DATE_TYPES = set([
    'Edm.DateTimeOffset',
    'Edm.Date',
    ])

BOOL_TYPES = set(['Edm.Boolean'])

COMPLEX_TYPES = set([
    'Edm.Binary',
    'mscrm.BooleanManagedProperty',
    ])

class BaseStream:
    """
    A base class representing singer streams.

    :param client: The API client used extract records from the external source
    """
    tap_stream_id = None
    stream_endpoint = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    params = {}

    def __init__(self, client: DynamicsClient):
        self.client = client

    def get_records(self, max_pagesize: int = 100, bookmark_datetime: str = None) -> list:
        """
        Returns a list of records for that stream.

        :param max_pagesize: The odata.maxpagesize to use in the request header
        :param bookmark_datetime: The datetime value to use in the $filter
            query param for the request
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require implementation")

    def set_parameters(self, params: dict) -> None:
        """
        Sets or updates the `params` attribute of a class.

        :param params: Dictionary of parameters to set or update the class with
        """
        self.params = params


class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'
    batched = False

    def get_records(self, max_pagesize: int = 100, bookmark_datetime: str = None):
        endpoint = self.stream_endpoint

        max_pagesize = MAX_PAGESIZE if max_pagesize is None else max_pagesize # tap-tester was failing otherwise
        pagesize = max_pagesize if max_pagesize <= MAX_PAGESIZE else MAX_PAGESIZE
        header = {'Prefer': f'odata.maxpagesize={pagesize}'}

        params = self.client.build_params(filter_value=bookmark_datetime)
        self.set_parameters(params)

        next_page = True
        paging = False

        while next_page:
            response = self.client.get(endpoint, paging, headers=header, params=self.params)

            if '@odata.nextLink' in response:
                paging = True
                endpoint = response.get('@odata.nextLink')
                self.set_parameters({})
            else:
                next_page = False

            yield from response.get('value')


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
        start_time = singer.get_bookmark(state,
                                        self.tap_stream_id,
                                        self.replication_key,
                                        config['start_date'])
        max_record_value = start_time

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(config.get('max_pagesize'), max_record_value):
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

    :param client: The API client used to extract records from the external source
    """
    replication_method = 'FULL_TABLE'

    # pylint: disable=arguments-differ
    def get_records(self, max_pagesize: int = 100):
        endpoint = self.stream_endpoint

        max_pagesize = MAX_PAGESIZE if max_pagesize is None else max_pagesize # tap-tester was failing otherwise
        pagesize = max_pagesize if max_pagesize <= MAX_PAGESIZE else MAX_PAGESIZE
        header = {'Prefer': f'odata.maxpagesize={pagesize}'}

        next_page = True
        paging = False

        while next_page:
            response = self.client.get(endpoint, paging, headers=header, params=self.params)

            if not response.get('value'):
                LOGGER.warning('response is empty for {}'.format(self.stream_endpoint))

            if '@odata.nextLink' in response:
                paging = True
                endpoint = response.get('@odata.nextLink')
                self.set_parameters({})
            else:
                next_page = False

            yield from response.get('value')


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
            for record in self.get_records(config.get('max_pagesize')):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()

        singer.write_state(state)
        return state


REPLICATION_TO_STREAM_MAP = {
    'INCREMENTAL': IncrementalStream,
    'FULL_TABLE': FullTableStream
}

def get_streams(config: dict, config_path: str) -> dict:
    STREAMS = {} # pylint: disable=invalid-name

    config["config_path"] = config_path
    client_config = get_client_args(config)

    client = DynamicsClient(**client_config)

    # dynamically build streams by iterating over entities and calling build_schema()
    for stream in client.build_entity_metadata():
        stream_name = stream.get('LogicalName')
        stream_endpoint = stream.get('EntitySetName')
        stream_key = stream.get('Key')

        # skip over any streams that don't have a name or are in EXCLUDED_ENTITIES
        if not stream_name or stream_name in EXCLUDED_ENTITIES:
            continue

        attributes = flatten_entity_attributes(stream.get('Properties'))

        if 'modifiedon' in attributes.keys():
            replication_method = 'INCREMENTAL'
            replication_key = 'modifiedon'
        else: replication_method = 'FULL_TABLE'

        # Instantiate an object for each stream Class with requisite metadata
        stream_class = REPLICATION_TO_STREAM_MAP.get(replication_method)
        stream_obj = stream_class(client)

        # set class attributes for each stream
        stream_obj.tap_stream_id = stream_name
        stream_obj.key_properties = [stream_key]
        stream_obj.stream_endpoint = stream_endpoint

        if replication_method == 'INCREMENTAL':
            stream_obj.replication_key = replication_key
            stream_obj.valid_replication_keys = ['modifiedon']

        # build schema and skip over any streams with no valid fields
        stream_obj.schema = build_schema(attributes)
        if not stream_obj.schema.get('properties'):
            continue

        if stream_obj.key_properties[0] not in stream_obj.schema.get('properties'):
            continue

        STREAMS.update({stream_name: stream_obj})

    return STREAMS

def build_schema(attributes: dict):
    json_props = {}

    for attr_name, attr_props in attributes.items():
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
            # TODO: mark as "inclusion": "unsupported"
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
