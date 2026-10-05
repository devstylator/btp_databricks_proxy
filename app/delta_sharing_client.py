"""
Reads a Databricks Delta Sharing table into a pandas DataFrame using the
official open-source `delta-sharing` Python client - no Spark/JVM needed.

Authentication: SAP Cloud Integration authenticates to this proxy with an
XSUAA JWT in the Authorization header. Separately, CPI passes the Databricks
access token in the `Databricks-Bearer` header; this token is used here,
unmodified, as a Delta Sharing "bearer_token" (v1) profile.

Required configuration (see README.md for how to supply it securely via
a bound user-provided service):
  DELTA_SHARING_ENDPOINT   e.g. https://<workspace-host>/api/2.0/delta-sharing/metastores/<metastore-id>/recipients/<recipient-id>
"""

import os
import re
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
# must fetch a bounded superset before filtering rather than relying on the
# Delta Sharing server to filter for us.
MAX_FETCH_ROWS = 50000

# Name of the bound user-provided service holding the (non-secret) Delta
# Sharing endpoint URL (see manifest.yml / mta.yaml).
_CREDENTIALS_SERVICE_NAME = "databricks-connector-credentials"


def _delta_sharing_endpoint() -> str:
    # Prefer the bound user-provided service (how it's supplied in Cloud
    # Foundry); fall back to a plain OS env var for local development.
    service = AppEnv().get_service(name=_CREDENTIALS_SERVICE_NAME)
    value = (service.credentials.get("DELTA_SHARING_ENDPOINT") if service else None) or os.environ.get(
        "DELTA_SHARING_ENDPOINT"
    )
    if not value:
        raise RuntimeError("Missing required configuration: DELTA_SHARING_ENDPOINT")
    return value


class ReadError(ValueError):
    """Raised for invalid share/schema/table coordinates - mapped to HTTP 400."""


def read_table(
    share: str,
    schema: str,
    table: str,
    bearer_token: str,
    limit: Optional[int] = MAX_FETCH_ROWS,
) -> pd.DataFrame:
    for name, value in (("share", share), ("schema", schema), ("table", table)):
        if not value or not NAME.match(value):
            raise ReadError(f'Invalid {name}: "{value}"')

    # Built fresh per request (never cached): the Databricks bearer token is
    # supplied by CPI on every call and may rotate/expire independently.
    profile = DeltaSharingProfile(
        share_credentials_version=1,
        endpoint=_delta_sharing_endpoint(),
        bearer_token=bearer_token,
    )
    rest_client = DataSharingRestClient(profile)
    delta_table = Table(name=table, share=share, schema=schema)
    reader = DeltaSharingReader(delta_table, rest_client, limit=limit)
    return reader.to_pandas()
