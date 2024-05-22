"""Authentication classes for tap-powerbi-metadata."""

from urllib.parse import urljoin

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta

AUTH_URL = "TODO"
    
class PowerBIMetadataAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    # https://pivotalbi.com/automate-your-power-bi-dataset-refresh-with-python

    @property
    def oauth_request_body(self) -> dict:
        return {
            'grant_type': 'password',
            'scope': 'https://api.powerbi.com',
            'resource': 'https://analysis.windows.net/powerbi/api',
            'client_id': self.config["client_id"],
            'username': self.config["username"],
            'password': self.config["password"],
        }
    
    @property
    def auth_endpoint(self) -> str:
        base= "https://login.microsoftonline.com/"
        tenant = self.config['tenant_id']
        endpoint = f"/{tenant}/oauth2/token"
        # oauth_scopes="https://analysis.windows.net/powerbi/api",
        return urljoin(base, endpoint)