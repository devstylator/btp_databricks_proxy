"""
Extracts the bearer token that SAP Cloud Integration forwards on each
request.

This app no longer stores or requests any Databricks/Azure AD credentials
itself. Instead, SAP CI's HTTP receiver adapter is configured with an
OAuth2 Client Credentials artifact (client id/secret/token endpoint/scope
for the Databricks Delta Sharing recipient's Entra ID service principal).
CPI mints the access token itself and sends it as a normal
`Authorization: Bearer <token>` header on its call to this app.

This module only extracts that token - it does not (and cannot, since it
never sees the client secret) validate its signature locally. That's fine
because the token is used, unmodified, as the Delta Sharing bearer token
when this app calls Databricks (see delta_sharing_client.py): Databricks
itself validates the token's signature, audience, scope and expiry, so an
invalid/forged/expired token simply fails upstream (surfaced as a 502)
rather than being trusted by this app.
"""

from fastapi import Header, HTTPException


async def require_bearer_token(authorization: str = Header(default=None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token = authorization[len("Bearer ") :].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Empty bearer token")

    return token
