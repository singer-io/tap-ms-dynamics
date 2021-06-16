# tap-dynamics

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls data from the [Microsoft Dataverse Web API](https://docs.microsoft.com/en-us/powerapps/developer/data-platform/webapi/overview)
- Discovers all available entities and their fields.
    - Some entities are excluded as a result of testing. See `EXCLUDED_ENTITIES` in [streams.py](tap_dynamics/streams.py)
-  Includes a schema for each resource reflecting most recent tested data retrieved using the API. See the [EntityType](https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/entitytypes?view=dynamics-ce-odata-9) page for details.
- Some streams incrementally pull data based on the previously saved state. See the [bookmarking strategy](README.md#bookmarking-strategy) section for more details.

## Bookmarking Strategy
Some endpoints in the Microsoft Dataverse Web API support a `modifiedon` field that allows for `INCREMENTAL` replication. However, in some cases there is no such field available so the endpoints require `FULL_TABLE` replication.

The API supports pagination using the `@odata.nextLink` in the response. The page size for each request can be specified in the config using `max_pagesize` otherwise it defaults to 100 and the API max is 5,000.

## Authentication
The API uses OAuth2.0 for authorization and authentication. See the Microsoft [docs](https://docs.microsoft.com/en-us/powerapps/developer/data-platform/authenticate-oauth) page for more details. The [guide](https://docs.microsoft.com/en-us/powerapps/developer/data-platform/walkthrough-register-app-azure-active-directory) for adding an Azure AD app has all the required steps for setting it up.

The high level steps are as follows:

1. Create an Azure AD app
2. Add a `client_secret` and `redirect_uri` to the Azure AD app
3. Obtain a `refresh_token` for the app using *authorization code* grant type and both *offline_access* and *default* scope. Example: offline_access <organization_uri>/.default>
4. Add `refresh_token` to config and the tap will retrieve the access token when run

## Quick Start
1. Install

Clone this repository, and then install using setup.py. We recommend using a virtualenv:

```bash
$ virtualenv -p python3 venv
$ source venv/bin/activate
$ pip install -e .
```

2. Create your tap's config.json file. The tap config file for this tap should include these entries:

    - `start_date` - (rfc3339 date string) the default value to use if no bookmark exists for an endpoint
    - `user_agent` (string, required): Process and email for API logging purposes. Example: tap-dynamics <api_user_email@your_company.com>
    - `organization_uri` (string, required): the MS Dynamics 365 domain URI for environment. Example: `https://<org-name>.<region>.dynamics.com`
    - `client_id` (string, required): The Azure AD app client id
    - `client_secret` (string, required): The Azure AD app client secret
    - `redirect_uri` (string, required): The Azure AD app redirect URI
    - `refresh_token` (string, required): The OAuth2.0 refresh token (see [Authentication](README.md#Authentication) section above)
    - `api_version` (string, optional): The API version. Example: "9.2"
    - `max_pagesize` (integer, optional): The maximum number of records per page to request for pagination

And the other values mentioned in the authentication section above.

```json
{
  "start_date": "2021-04-01T00:00:00Z",
  "user_agent": "Stitch Tap (+support@stitchdata.com)",
  "organization_uri": "<https://YOUR_ORG_NAME.YOUR_INSTANCE_REGION.dynamics.com>",
  "client_id": "<YOUR_AZURE_APP_CLIENT_ID>",
  "client_secret": "<YOUR_AZURE_APP_CLIENT_SECRET>",
  "redirect_uri": "<YOUR_AZURE_APP_REDIRECT_URI>",
  "refresh_token": "<YOUR_OAUTH2.0_REFRESH_TOKEN>",
  "api_version": "<DATAVERSE_WEB_API_VERSION>",
  "max_pagesize": <MAX_RECORDS_PER_PAGE>
}
```

3. Run the Tap in Discovery Mode This creates a catalog.json for selecting objects/fields to integrate:

```bash
tap-dynamics --config config.json --discover > catalog.json
```

See the Singer docs on discovery mode [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

4. Run the Tap in Sync Mode (with catalog) and write out to state file

For Sync mode:

```bash
$ tap-dynamics --config tap_config.json --catalog catalog.json >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

To load to json files to verify outputs:

```bash
$ tap-dynamics --config tap_config.json --catalog catalog.json | target-json >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

To pseudo-load to Stitch Import API with dry run:

```bash
$ tap-dynamics --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```


---

Copyright &copy; 2018 Stitch
