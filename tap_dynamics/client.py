from logging import config
import os
import time
import json
import backoff
import requests
from datetime import datetime, timedelta
from requests.exceptions import RequestException
import singer
import singer.utils as singer_utils
from singer import metadata, metrics

LOGGER = singer.get_logger()

API_VERSION = '9.2'

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

class DynamicsException(Exception):
    pass

class DynamicsQuotaExceededException(DynamicsException):
    pass

class Dynamics5xxException(DynamicsException):
    pass

class Dynamics4xxException(DynamicsException):
    pass

class Dynamics429Exception(DynamicsException):
    pass

STRING_TYPES = set([
    'Customer',
    'Lookup',
    'Memo',
    'Owner',
    'PartyList',
    'Picklist',
    'State',
    'Status',
    'String',
    'Uniqueidentifier',
    'CalendarRules', # TODO: need to confirm
    'Virtual', # TODO: need to confirm
    'ManagedProperty',
    'EntityName'
])

NUMBER_TYPES = set([
    'Decimal',
    'Double',
    'Integer',
    'Money',
    'BigInt'
])

DATE_TYPES = set(['DateTime'])

BOOL_TYPES = set(['Boolean'])

def log_backoff_attempt(details):
    LOGGER.info("ConnectionError detected, triggering backoff: %d try", details.get("tries"))



class DynamicsClient:
    def __init__(self,
                organization_uri,
                config_path,
                tenant_id=None,
                api_version=None,
                client_id=None,
                client_secret=None,
                user_agent=None,
                redirect_uri=None,
                refresh_token=None,
                select_fields_by_default=None,
                start_date=None):
        self.organization_uri = organization_uri
        self.api_version = api_version if api_version else API_VERSION
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.redirect_uri = redirect_uri
        self.user_agent = user_agent
        self.refresh_token = refresh_token

        self.session = requests.Session()
        self.access_token = None
        self.expires_at = None     

        self.select_fields_by_default = select_fields_by_default is True or (isinstance(select_fields_by_default, str) and select_fields_by_default.lower() == 'true')
        self.start_date = start_date
        self.config_path = config_path # TODO: check how this is implimented in tap-quickbooks w/ OUT needing self-reference in the config file

        # validate start_date
        singer_utils.strptime(start_date)

    def _write_config(self, refresh_token):
        LOGGER.info("Credentials Refreshed")
        self.refresh_token = refresh_token

        # Update config at config_path
        with open(self.config_path) as file:
            config = json.load(file)

        config['refresh_token'] = refresh_token

        with open(self.config_path, 'w') as file:
            json.dump(config, file, indent=2)

    def login(self):
        # TODO: create login method for OAth2.0 authorization_code flow using 'offline_access' and 'org_uri/.default' scopes
        pass

    def ensure_access_token(self):
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

            self.expires_at = datetime.utcnow() + \
                timedelta(seconds=int(data.get('expires_in')) - 10) # pad by 10 seconds for clock drift

    def _get_standard_headers(self):
        return {
            "Authorization": "Bearer {}".format(self.access_token),
            "User-Agent": self.user_agent,
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0"
            }

    @backoff.on_exception(backoff.expo,
                          (Dynamics5xxException, Dynamics429Exception, requests.ConnectionError),
                          max_tries=10,
                          factor=2,
                          on_backoff=log_backoff_attempt)
    @singer.utils.ratelimit(500, 60)
    def _make_request(self, method, endpoint, headers=None, params=None, data=None):
        full_url = self.organization_uri + '/api/data/v' + self.api_version + '/' + endpoint
        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            endpoint,
            params,
        )

        self.ensure_access_token()

        default_headers = self._get_standard_headers()

        if headers:
            headers = {**default_headers, **headers}
        else:
            headers = {**default_headers}

        response = self.session.request(method, full_url, headers=headers, params=params, data=data)

        # TODO: Check error status, rate limit, etc.
        if response.status_code >= 500:
            raise Dynamics5xxException(response.text)
        elif response.status_code == 429:
            raise Dynamics429Exception(response.text)
        elif response.status_code >= 400:
            raise Dynamics4xxException(response.text)

        return response.json()

    def get(self, url, headers=None, params=None):
        return self._make_request("GET", url, headers=headers, params=params)

    def get_entity_definitions(self):
        # TODO: this would call the `EntityDefinitions` endpoint and get all entities and their respective fields (Attributes) and corresponding metadata

        params = {
            "$select": "MetadataId,LogicalName,EntitySetName",
            "$expand": "Attributes($select=MetadataId,IsValidForRead,IsRetrievable,AttributeType,AttributeTypeName,LogicalName)",    
            "$count": "true",
        }

        return self.get('EntityDefinitions', params=params)

    def build_entity_metadata(self):
        # TODO: this should take the output of get_entity_definitions() and parse the results

        results = self.get_entity_definitions()

        LOGGER.info(f'MS Dynamics 365 returned {results.get("@odata.count")} entities')

        # parse results
        for result in results.get('value'):

            result['name'] = result.get('LogicalName')
            attributes = [attr.get('LogicalName') for attr in result.get('Attributes')]

            if 'modifiedon' in attributes or 'createdon' in attributes:
                result['replication_method'] = 'INCREMENTAL'
            else: result['replication_method'] = 'FULL_TABLE'

            # TODO: add logic or a helper method to determine data-types mapping

            yield result
