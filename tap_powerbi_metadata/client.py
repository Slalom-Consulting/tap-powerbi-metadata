"""REST client handling, including PowerBIStream base class."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta

from typing import Any, Dict, Generator, Iterable, Optional, Union
from urllib import parse
from urllib.parse import parse_qsl, urljoin

import requests
from memoization import cached
from singer_sdk.streams import RESTStream

from tap_powerbi_metadata.auth import PowerBIMetadataAuthenticator
# from tap_powerbi_metadata.schema import get_schema, get_type_schema

API_URL = "https://api.powerbi.com/v1.0/myorg"
API_DATE_FORMAT = "'%Y-%m-%dT%H:%M:%SZ'"

class TapPowerBIMetadataStream(RESTStream):
    """Base class for PowerBIMetadata streams."""

    url_base = "https://api.powerbi.com/v1.0/myorg"

    #: Variables to indicate if the endpoint requires a $top and $skip value. Defaults to False
    top_required = False
    skip_required = False

    #: Empty dictionary that can be used if the endpoint requires additional URI parameters.
    #: Example: uri_parameters = {'$expand':'users,reports,dashboards,datasets,dataflows,workbooks'}
    uri_parameters = {}

    #: Integer that determines the $top and $skip values for the streams that use them
    #: 5000 is the maximum amount. Shouldn't need to be changed unless a smaller size is required.
    top_value = 5000

    def get_stream_config(self) -> dict:
        """Get config for stream."""
        stream_configs = self.config.get("stream_config", {})
        if not stream_configs:
            string_configs = self.config.get("stream_config_string","")
            if string_configs:
                stream_configs = json.loads(string_configs)
        return stream_configs.get(self.name, {})

    def get_stream_params(self) -> dict:
        """Get parameters set in config for stream."""
        stream_params = self.get_stream_config().get("parameters","")
        return {qry[0]: qry[1] for qry in parse_qsl(stream_params.lstrip("?"))}
    

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.
        
        Args:
            response: A `requests.Response` object. 

        Yields:
            One item for every item found in the response.
        
        """
        resp_json = response.json()
        for row in resp_json.get("value"):
            yield row
    
    @property
    def authenticator(self) -> PowerBIMetadataAuthenticator:
        return PowerBIMetadataAuthenticator(self)
    
    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.
        

        """
        params = self.uri_parameters
        #Set pagination parameters
        if self.top_required:
            params["$top"] = self.top_value
        if next_page_token:
            params["$skip"] = next_page_token
        
        #Set custom config parameters
        stream_params = self.get_stream_params()

        # Ensure that $count is True when used in $filter parameter
        filter_params = stream_params.get("$filter", "")
        if filter_params:
            if "$count" in filter_params:
                params["$count"] = True

        # Ensure that primary keys are included in $select parameter
        select_param = stream_params.get("$select", "")
        if select_param:
            select_params = select_param.split(",")

            missing_primary_keys = [
                k for k in self.primary_keys if k not in select_params
            ]

            if missing_primary_keys:
                select_params.extend(missing_primary_keys)

            params["$select"] = ",".join(select_params)
        params.update(stream_params)
        self.logger.info(f'PARAMS: {params}')
        return params

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any] = None) -> Optional[Any]:
        """Return token for identifying next page or None if not applicable.
        
        Args:
            response: A `requests.Response` object.
            previous_token: An integer that is the $skip value that was used in the previous request.

        Returns:
            Either a new next_page_token that is the sum of the previous_token and top_value
            Or None if a $skip value is not required, or if the 'value' of the Response object is empty
        """

        if not self.skip_required:
            return None
        if not previous_token:
            next_page_token = self.top_value
        #if there is a previous_token, but there is not content in the Response object
        elif not response.json().get('value'):
            return None
        else:
            previous_token += self.top_value
            next_page_token = previous_token
        return next_page_token

class TapPowerBIUsageStream(RESTStream):
    """PowerBIUsage stream class."""

    url_base = "https://api.powerbi.com/v1.0/myorg"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.
        
        API only supports a single UTC day, or continuationToken-based pagination.
        """
        params = {}
        if next_page_token:
            starting_datetime = next_page_token["urlStartDate"]
            continuationToken = next_page_token.get("continuationToken")
        else:
            starting_datetime = self.get_starting_timestamp(partition)
            continuationToken = None
        if continuationToken:
            params["continuationToken"] = "'" + continuationToken + "'"
        else:
            params.update({"startDateTime": starting_datetime.strftime(API_DATE_FORMAT)})
            ending_datetime = starting_datetime.replace(hour=0, minute=0, second=0) + timedelta(days=1) + timedelta(microseconds=-1)
            params.update({"endDateTime": ending_datetime.strftime(API_DATE_FORMAT)})
        self.logger.debug(params)
        return params

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return PowerBIMetadataAuthenticator(
            stream=self,
            auth_endpoint=f"https://login.microsoftonline.com/{self.config['tenant_id']}/oauth2/token",
            oauth_scopes="https://analysis.windows.net/powerbi/api",
        )

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any] = None) -> Optional[Any]:
        """Return token for identifying next page or None if not applicable."""
        resp_json = response.json()
        continuationToken = resp_json.get("continuationToken")
        next_page_token = {}
        if not previous_token:
            # First time creating a pagination token so we need to record the initial start date.
            req_url = response.request.url
            req_params = parse.parse_qs(parse.urlparse(req_url).query)
            self.logger.debug("Params: {}".format(req_params))
            latest_url_start_date_param = req_params["startDateTime"][0]
            next_page_token["urlStartDate"] = datetime.strptime(latest_url_start_date_param, API_DATE_FORMAT)
        else: 
            next_page_token["urlStartDate"] = previous_token.get("urlStartDate")
        if continuationToken:
            next_page_token["continuationToken"] = requests.utils.unquote(continuationToken)
        else:
            next_page_token["continuationToken"] = None
            # Now check if we should repeat API call for next day
            latestUrlStartDate = next_page_token["urlStartDate"]
            nextUrlStartDate = latestUrlStartDate.replace(hour=0, minute=0, second=0) + timedelta(days=1)
            self.logger.info("No next page token found, checking if {} is greater than now".format(nextUrlStartDate))
            if nextUrlStartDate < datetime.utcnow():
                self.logger.info("{} is less than now, incrementing date by 1 and continuing".format(nextUrlStartDate))
                next_page_token["urlStartDate"] = nextUrlStartDate
                self.logger.debug(next_page_token)
            else:
                self.logger.info("No continuationToken, and nextUrlStartDate after today, calling it quits")
                return None
        return next_page_token

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        for row in resp_json.get("activityEventEntities"):
            yield row
