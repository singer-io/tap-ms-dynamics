import os
import sys
import math
import json
from datetime import datetime, timedelta

import backoff
import requests
import singer
from simplejson import JSONDecodeError

from tap_dynamics.transform import transform_metadata_xml


LOGGER = singer.get_logger()

API_VERSION = '9.2'
MAX_PAGESIZE = 5000
MAX_RETRIES = 5

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def log_backoff_attempt(details):
    LOGGER.info("ConnectionError detected, triggering backoff: %d try", details.get("tries"))

def retry_after_wait_gen():
    while True:
        # This is called in an except block so we can retrieve the exception
        # and check it.
        exc_info = sys.exc_info()
        resp = exc_info[1].response
        sleep_time_str = resp.headers.get('Retry-After')
        LOGGER.info(f'API rate limit exceeded -- sleeping for '
                    f'{sleep_time_str} seconds')
        yield math.floor(float(sleep_time_str))

# pylint: disable=missing-class-docstring
class DynamicsException(Exception):
    pass

# pylint: disable=missing-class-docstring
class DynamicsQuotaExceededException(DynamicsException):
    pass

# pylint: disable=missing-class-docstring
class Dynamics5xxException(DynamicsException):
    pass

# pylint: disable=missing-class-docstring
class Dynamics4xxException(DynamicsException):
    pass

# pylint: disable=missing-class-docstring
class Dynamics429Exception(DynamicsException):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response

# pylint: disable=too-many-instance-attributes
class DynamicsClient:
    def __init__(self,
                organization_uri,
                config_path,
                max_pagesize,
                api_version=None,
                client_id=None,
                client_secret=None,
                user_agent=None,
                redirect_uri=None,
                refresh_token=None,
                start_date=None):
        self.organization_uri = organization_uri
        self.api_version = api_version if api_version else API_VERSION
        max_pagesize = MAX_PAGESIZE if max_pagesize is None else max_pagesize # tap-tester was failing otherwise
        self.max_pagesize = max_pagesize if max_pagesize <= MAX_PAGESIZE else MAX_PAGESIZE
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.user_agent = user_agent
        self.refresh_token = refresh_token

        self.session = requests.Session()
        self.access_token = None
        self.expires_at = None

        self.start_date = start_date
        self.config_path = config_path

    def _write_config(self, refresh_token):
        LOGGER.info("Credentials Refreshed")
        self.refresh_token = refresh_token

        # Update config at config_path
        with open(self.config_path) as file:
            config = json.load(file)

        config['refresh_token'] = refresh_token

        with open(self.config_path, 'w') as file:
            json.dump(config, file, indent=2)

    def _ensure_access_token(self):
        if self.access_token is None or self.expires_at <= datetime.utcnow():
            response = self.session.post(
                'https://login.microsoftonline.com/common/oauth2/token',
                data={
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'redirect_uri': self.redirect_uri,
                    'refresh_token': self.refresh_token,
                    'grant_type': 'refresh_token',
                    'resource': self.organization_uri
                })

            if response.status_code != 200:
                raise DynamicsException('Non-200 response fetching Dynamics access token')

            data = response.json()

            self.access_token = data.get('access_token')
            if self.refresh_token != data.get('refresh_token'):
                self._write_config(data.get('refresh_token'))

            # pad by 10 seconds for clock drift
            self.expires_at = datetime.utcnow() + \
                timedelta(seconds=int(data.get('expires_in')) - 10)

    def _get_standard_headers(self):
        return {
            "Authorization": "Bearer {}".format(self.access_token),
            "User-Agent": self.user_agent,
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "If-None-Match": "null"
            }

    @backoff.on_exception(retry_after_wait_gen,
                          Dynamics429Exception,
                          max_tries=MAX_RETRIES,
                          on_backoff=log_backoff_attempt)
    @backoff.on_exception(backoff.expo,
                          (Dynamics5xxException, Dynamics4xxException, requests.ConnectionError),
                          max_tries=MAX_RETRIES,
                          factor=2,
                          on_backoff=log_backoff_attempt)
    def _make_request(self, method, endpoint, paging=False, headers=None, params=None, data=None):
        if not paging:
            full_url = f'{self.organization_uri}/api/data/v{self.api_version}/{endpoint}'
        else: full_url = endpoint

        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            endpoint if not paging else '@odata.nextLink',
            params,
        )

        self._ensure_access_token()

        default_headers = self._get_standard_headers()

        if headers:
            headers = {**default_headers, **headers}
        else:
            headers = {**default_headers}

        response = self.session.request(method, full_url, headers=headers, params=params, data=data)

        # pylint: disable=no-else-raise
        if response.status_code >= 500:
            raise Dynamics5xxException(response.text)
        elif response.status_code == 429:
            raise Dynamics429Exception("rate limit exceeded", response)
        elif response.status_code >= 400:
            raise Dynamics4xxException(response.text)

        try:
            results = response.json()
        except JSONDecodeError:
            results = response.text

        return results

    def get(self, endpoint, paging=False, headers=None, params=None):
        return self._make_request("GET", endpoint, paging, headers=headers, params=params)

    def call_entity_definitions(self):
        '''
        Calls the `EntityDefinitions` endpoint to get all entities.
        '''

        params = {
            "$select": "MetadataId,LogicalName,EntitySetName",
            "$count": "true",
        }

        results = self.get('EntityDefinitions', params=params)

        LOGGER.info('MS Dynamics returned {} entities'.format(results.get("@odata.count")))

        # return results
        yield from results.get('value')

    def call_metadata(self) -> dict:
        '''
        Calls the `$metadata` endpoint to get entities, key field,
            properties, and corresponding datatypes.
        '''
        metadata = self.get('$metadata')

        return transform_metadata_xml(metadata)

    def build_entity_metadata(self):
        '''
        Builds entity metadata from the `EntityDefinitions` and `$metadata` endpoints.
        '''
        entity_definitions = self.call_entity_definitions()

        entity_metadata = self.call_metadata()

        for entity in entity_definitions:
            entity_name = entity.get("LogicalName")
            if entity_name in entity_metadata:
                # checks that entity is in $metadata response
                entity_metadata[entity_name]["LogicalName"] = entity_name
                entity_metadata[entity_name]["EntitySetName"] = entity.get("EntitySetName")

        yield from entity_metadata.values()

    @staticmethod
    def build_params(orderby_key: str = 'modifiedon',
                    replication_key: str = 'modifiedon',
                    filter_value: str = None) -> dict:
        orderby_param = f'{orderby_key} asc'

        if filter_value:
            filter_param = f'{replication_key} ge {filter_value}'
            return {"$orderby": orderby_param, "$filter": filter_param}
        return {"$orderby": orderby_param}
