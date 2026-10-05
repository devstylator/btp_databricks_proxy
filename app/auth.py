"""XSUAA authentication and Databricks token extraction for incoming requests."""

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import urlsplit, urlunsplit

import jwt
from cfenv import AppEnv
from fastapi import Header, HTTPException
from jwt import PyJWKClient
from jwt.exceptions import PyJWKClientConnectionError, PyJWTError

_XSUAA_SERVICE_NAME = "databricks-connector-xsuaa"
_REQUIRED_SCOPE_SUFFIX = ".Proxy"
_JWT_ALGORITHM = "RS256"
_logger = logging.getLogger(__name__)


class XSUAAConfigurationError(RuntimeError):
    """Raised when the bound XSUAA service or required settings are missing."""


@dataclass(frozen=True)
class _XSUAAConfig:
    issuer: str
    jwks_url: str
    audience: str
    required_scope: str
    allowed_client_ids: frozenset[str]


def _xsuaa_endpoints(url: str) -> tuple[str, str]:
    parsed = urlsplit(url.strip())
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise XSUAAConfigurationError("XSUAA URL must be an HTTPS URL without query or fragment")

    path = parsed.path.rstrip("/")
    for suffix in ("/oauth/token", "/token_keys"):
        if path.endswith(suffix):
            path = path[: -len(suffix)]
            break

    base_url = urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
    return f"{base_url}/oauth/token", f"{base_url}/token_keys"


def _load_xsuaa_config() -> _XSUAAConfig:
    service = AppEnv().get_service(name=_XSUAA_SERVICE_NAME)
    credentials = service.credentials if service else {}
    if not isinstance(credentials, Mapping):
        raise XSUAAConfigurationError("Invalid XSUAA service credentials")

    xsuaa_url = os.environ.get("XSUAA_URL") or credentials.get("url")
    xsappname = os.environ.get("XSUAA_XSAPPNAME") or credentials.get("xsappname")
    allowed_client_ids_value = os.environ.get("XSUAA_ALLOWED_CLIENT_IDS", "")
    allowed_client_ids = frozenset(
        client_id.strip() for client_id in allowed_client_ids_value.split(",") if client_id.strip()
    )

    if not isinstance(xsuaa_url, str) or not xsuaa_url.strip():
        raise XSUAAConfigurationError("Missing required configuration: XSUAA_URL")
    if not isinstance(xsappname, str) or not xsappname.strip():
        raise XSUAAConfigurationError("Missing required configuration: XSUAA_XSAPPNAME")
    if not allowed_client_ids:
        raise XSUAAConfigurationError("Missing required configuration: XSUAA_ALLOWED_CLIENT_IDS")

    issuer, jwks_url = _xsuaa_endpoints(xsuaa_url)
    audience = xsappname.strip()
    return _XSUAAConfig(
        issuer=issuer,
        jwks_url=jwks_url,
        audience=audience,
        required_scope=f"{audience}{_REQUIRED_SCOPE_SUFFIX}",
        allowed_client_ids=allowed_client_ids,
    )


@lru_cache(maxsize=4)
def _get_jwks_client(jwks_url: str) -> PyJWKClient:
    return PyJWKClient(jwks_url, cache_jwk_set=True, lifespan=300, timeout=5)


def _extract_authorization_token(authorization: str | None) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing XSUAA bearer token")

    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Malformed XSUAA bearer token")

    return parts[1]


def require_xsuaa_token(authorization: str | None = Header(default=None)) -> None:
    token = _extract_authorization_token(authorization)

    try:
        config = _load_xsuaa_config()
    except XSUAAConfigurationError as exc:
        _logger.error("XSUAA authentication is not configured: %s", exc)
        raise HTTPException(status_code=503, detail="XSUAA authentication is not configured") from exc

    try:
        signing_key = _get_jwks_client(config.jwks_url).get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=[_JWT_ALGORITHM],
            audience=config.audience,
            issuer=config.issuer,
            options={"require": ["exp", "iss", "aud", "client_id"]},
        )
    except PyJWKClientConnectionError as exc:
        _logger.error("Unable to retrieve XSUAA signing keys: %s", exc)
        raise HTTPException(status_code=503, detail="XSUAA token verification is unavailable") from exc
    except PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid XSUAA bearer token") from exc

    client_id = claims.get("client_id")
    if not isinstance(client_id, str) or client_id not in config.allowed_client_ids:
        raise HTTPException(status_code=403, detail="XSUAA client is not allowed")

    scopes = claims.get("scope", [])
    if isinstance(scopes, str):
        scopes = scopes.split()
    if not isinstance(scopes, list) or config.required_scope not in scopes:
        raise HTTPException(status_code=403, detail="XSUAA token is missing the required scope")


def require_databricks_bearer(
    databricks_bearer: str | None = Header(default=None, alias="Databricks-Bearer"),
) -> str:
    token = (databricks_bearer or "").strip()
    if not token or any(character.isspace() for character in token):
        raise HTTPException(status_code=401, detail="Missing or malformed Databricks-Bearer header")

    return token
