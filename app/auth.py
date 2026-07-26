"""
Validates the XSUAA JWT that SAP Cloud Integration sends (OAuth2 Client
Credentials token) and enforces the required scope, using SAP's official
`sap-xssec` library.

For local development only, set AUTH_DISABLED=true to skip validation -
never do this in a deployed environment.
"""

import os

from cfenv import AppEnv
from fastapi import Header, HTTPException
from sap import xssec

_env = AppEnv()


def _uaa_credentials() -> dict:
    service = _env.get_service(label="xsuaa")
    if not service:
        raise RuntimeError("No bound xsuaa service instance found (VCAP_SERVICES)")
    return service.credentials


def require_scope(scope_name: str):
    async def dependency(authorization: str = Header(default=None)):
        if os.environ.get("AUTH_DISABLED") == "true":
            return None

        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing bearer token")

        token = authorization[len("Bearer ") :]
        try:
            security_context = xssec.create_security_context(token, _uaa_credentials())
        except Exception as exc:  # noqa: BLE001 - any validation failure -> 401
            raise HTTPException(status_code=401, detail=f"Invalid token: {exc}") from exc

        if not security_context.check_local_scope(scope_name):
            raise HTTPException(status_code=403, detail=f'Missing required scope "{scope_name}"')

        return security_context

    return dependency
