"""API configuration."""
import os
from typing import Any, Dict, Set

from stac_fastapi.types.config import ApiSettings
from pyle38 import Tile38

DOMAIN = os.getenv("38_HOST")
PORT = os.getenv("38_PORT")

# def _tile38_config() -> Dict[str, Any]:
#     config = {
#         "domain": os.getenv("38_HOST"),
#         "port": os.getenv("38_PORT"),
#     }
#     return config


# def _es_config() -> Dict[str, Any]:
#     config = {
#         "hosts": [{"host": os.getenv("ES_HOST"), "port": os.getenv("ES_PORT")}],
#         "headers": {"accept": "application/vnd.elasticsearch+json; compatible-with=7"},
#         "use_ssl": True,
#         "verify_certs": True,
#     }

#     if (u := os.getenv("ES_USER")) and (p := os.getenv("ES_PASS")):
#         config["http_auth"] = (u, p)

#     if (v := os.getenv("ES_USE_SSL")) and v == "false":
#         config["use_ssl"] = False

#     if (v := os.getenv("ES_VERIFY_CERTS")) and v == "false":
#         config["verify_certs"] = False

#     if v := os.getenv("CURL_CA_BUNDLE"):
#         config["ca_certs"] = v

#     return config


_forbidden_fields: Set[str] = {"type"}


class Tile38Settings(ApiSettings):
    """API settings."""

    # Fields which are defined by STAC but not included in the database model
    forbidden_fields: Set[str] = _forbidden_fields

    @property
    def create_client(self):
        """Create tile38 client."""
        client = Tile38(url=f"redis://{str(DOMAIN)}:{str(PORT)}", follower_url="redis://{str(DOMAIN)}:{str(PORT)}")
        return client


class AsyncElasticsearchSettings(ApiSettings):
    """API settings."""

    # Fields which are defined by STAC but not included in the database model
    forbidden_fields: Set[str] = _forbidden_fields

    @property
    def create_client(self):
        """Create async tile38 client."""
        client = Tile38(url=f"redis://{str(DOMAIN)}:{str(PORT)}", follower_url="redis://{str(DOMAIN)}:{str(PORT)}")
        return client
