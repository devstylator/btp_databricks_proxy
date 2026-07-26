"""
Reads a Databricks Delta Sharing table into a pandas DataFrame using the
official open-source `delta-sharing` Python client - no Spark/JVM needed.

Authentication uses the Databricks-specific "oauth_client_credentials"
Delta Sharing profile type (Azure AD / Entra ID service principal), the
same mechanism already validated in Test.groovy. The profile is built
in-memory (never written to disk).

Required environment variables (see README.md for how to supply them
securely via a bound user-provided service / Credential Store):
  DELTA_SHARING_ENDPOINT   e.g. https://<workspace-host>/api/2.0/delta-sharing/metastores/<metastore-id>/recipients/<recipient-id>
  AZURE_TOKEN_ENDPOINT     e.g. https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token
  AZURE_CLIENT_ID          Entra app (service principal) client id
  AZURE_CLIENT_SECRET      Entra app client secret
  AZURE_SCOPE              e.g. <client-id>/.default
"""

import os
import re
from functools import lru_cache
from typing import Optional

import pandas as pd
from cfenv import AppEnv
from delta_sharing.protocol import DeltaSharingProfile, Table
from delta_sharing.reader import DeltaSharingReader
from delta_sharing.rest_client import DataSharingRestClient

# Delta Sharing share/schema/table names are inlined into the REST URL path
# by the underlying client without escaping, so we validate them ourselves
# to prevent path-injection via a malicious share/schema/table value.
NAME = re.compile(r"^[A-Za-z0-9_\-]{1,255}$")

# Upper bound on rows fetched per request, independent of the caller's
# "top" - filters are applied client-side (see query_builder.py), so we
# must fetch a bounded superset before filtering rather than relying on
# the Delta Sharing server to filter for us.
MAX_FETCH_ROWS = 50000

# Name of the bound user-provided service holding the Delta Sharing /
# Azure AD credentials (see manifest.yml / mta.yaml).
_CREDENTIALS_SERVICE_NAME = "databricks-connector-credentials"


@lru_cache(maxsize=1)
def _service_credentials() -> dict:
    """Credentials from the bound user-provided service, if any (VCAP_SERVICES)."""
    service = AppEnv().get_service(name=_CREDENTIALS_SERVICE_NAME)
    return dict(service.credentials) if service else {}


def _require_env(name: str) -> str:
    # Prefer the bound user-provided service (how it's supplied in Cloud
    # Foundry); fall back to a plain OS env var for local development.
    value = _service_credentials().get(name) or os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@lru_cache(maxsize=1)
def _get_rest_client() -> DataSharingRestClient:
    profile = DeltaSharingProfile(
        share_credentials_version=2,
        type="oauth_client_credentials",
        endpoint=_require_env("DELTA_SHARING_ENDPOINT"),
        token_endpoint=_require_env("AZURE_TOKEN_ENDPOINT"),
        client_id=_require_env("AZURE_CLIENT_ID"),
        client_secret=_require_env("AZURE_CLIENT_SECRET"),
        scope=_require_env("AZURE_SCOPE"),
    )
    return DataSharingRestClient(profile)


class ReadError(ValueError):
    """Raised for invalid share/schema/table coordinates - mapped to HTTP 400."""


def read_table(share: str, schema: str, table: str, limit: Optional[int] = MAX_FETCH_ROWS) -> pd.DataFrame:
    for name, value in (("share", share), ("schema", schema), ("table", table)):
        if not value or not NAME.match(value):
            raise ReadError(f'Invalid {name}: "{value}"')

    rest_client = _get_rest_client()
    delta_table = Table(name=table, share=share, schema=schema)
    reader = DeltaSharingReader(delta_table, rest_client, limit=limit)
    return reader.to_pandas()
