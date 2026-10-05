from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import Mock, patch

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException
from jwt.exceptions import PyJWKClientConnectionError

from app.auth import (
    XSUAAConfigurationError,
    _XSUAAConfig,
    _xsuaa_endpoints,
    require_databricks_bearer,
    require_xsuaa_token,
)


class AuthTests(unittest.TestCase):
    def setUp(self):
        self.config = _XSUAAConfig(
            issuer="https://uaa.example.com/oauth/token",
            jwks_url="https://uaa.example.com/token_keys",
            audience="databricks-connector",
            required_scope="databricks-connector.Proxy",
            allowed_client_ids=frozenset({"cpi-client"}),
        )
        self.claims = {
            "client_id": "cpi-client",
            "scope": ["databricks-connector.Proxy"],
        }
        self.jwks_client = Mock()
        self.jwks_client.get_signing_key_from_jwt.return_value = Mock(key="public-key")

    def test_builds_xsuaa_issuer_and_signing_key_urls(self):
        self.assertEqual(
            _xsuaa_endpoints("https://uaa.example.com/oauth/token"),
            ("https://uaa.example.com/oauth/token", "https://uaa.example.com/token_keys"),
        )

    def test_requires_authorization_bearer_token(self):
        for authorization in (None, "", "Basic abc", "Bearer"):
            with self.subTest(authorization=authorization):
                with self.assertRaises(HTTPException) as raised:
                    require_xsuaa_token(authorization)
                self.assertEqual(raised.exception.status_code, 401)

    def test_accepts_valid_xsuaa_token_for_allowed_client_and_scope(self):
        with (
            patch("app.auth._load_xsuaa_config", return_value=self.config),
            patch("app.auth._get_jwks_client", return_value=self.jwks_client),
            patch("app.auth.jwt.decode", return_value=self.claims) as decode,
        ):
            require_xsuaa_token("Bearer signed-token")

        decode.assert_called_once_with(
            "signed-token",
            "public-key",
            algorithms=["RS256"],
            audience="databricks-connector",
            issuer="https://uaa.example.com/oauth/token",
            options={"require": ["exp", "iss", "aud", "client_id"]},
        )

    def test_verifies_a_real_xsuaa_signature_and_claims(self):
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()
        now = datetime.now(timezone.utc)
        token = jwt.encode(
            {
                **self.claims,
                "iss": self.config.issuer,
                "aud": self.config.audience,
                "iat": int(now.timestamp()),
                "exp": int((now + timedelta(minutes=5)).timestamp()),
            },
            private_key,
            algorithm="RS256",
            headers={"kid": "test-key"},
        )
        self.jwks_client.get_signing_key_from_jwt.return_value = Mock(key=public_key)

        with (
            patch("app.auth._load_xsuaa_config", return_value=self.config),
            patch("app.auth._get_jwks_client", return_value=self.jwks_client),
        ):
            require_xsuaa_token(f"Bearer {token}")

    def test_rejects_expired_xsuaa_token(self):
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        now = datetime.now(timezone.utc)
        token = jwt.encode(
            {
                **self.claims,
                "iss": self.config.issuer,
                "aud": self.config.audience,
                "iat": int((now - timedelta(minutes=10)).timestamp()),
                "exp": int((now - timedelta(minutes=5)).timestamp()),
            },
            private_key,
            algorithm="RS256",
            headers={"kid": "test-key"},
        )
        self.jwks_client.get_signing_key_from_jwt.return_value = Mock(key=private_key.public_key())

        with (
            patch("app.auth._load_xsuaa_config", return_value=self.config),
            patch("app.auth._get_jwks_client", return_value=self.jwks_client),
        ):
            with self.assertRaises(HTTPException) as raised:
                require_xsuaa_token(f"Bearer {token}")

        self.assertEqual(raised.exception.status_code, 401)

    def test_rejects_invalid_xsuaa_jwt(self):
        with (
            patch("app.auth._load_xsuaa_config", return_value=self.config),
            patch("app.auth._get_jwks_client", return_value=self.jwks_client),
            patch("app.auth.jwt.decode", side_effect=jwt.InvalidTokenError("invalid")),
        ):
            with self.assertRaises(HTTPException) as raised:
                require_xsuaa_token("Bearer invalid-token")

        self.assertEqual(raised.exception.status_code, 401)

    def test_reports_unavailable_signing_keys(self):
        self.jwks_client.get_signing_key_from_jwt.side_effect = PyJWKClientConnectionError("unavailable")
        with (
            patch("app.auth._load_xsuaa_config", return_value=self.config),
            patch("app.auth._get_jwks_client", return_value=self.jwks_client),
        ):
            with self.assertRaises(HTTPException) as raised:
                require_xsuaa_token("Bearer signed-token")

        self.assertEqual(raised.exception.status_code, 503)

    def test_rejects_unapproved_client(self):
        claims = {**self.claims, "client_id": "another-client"}
        with (
            patch("app.auth._load_xsuaa_config", return_value=self.config),
            patch("app.auth._get_jwks_client", return_value=self.jwks_client),
            patch("app.auth.jwt.decode", return_value=claims),
        ):
            with self.assertRaises(HTTPException) as raised:
                require_xsuaa_token("Bearer signed-token")

        self.assertEqual(raised.exception.status_code, 403)

    def test_rejects_token_without_required_scope(self):
        claims = {**self.claims, "scope": ["openid"]}
        with (
            patch("app.auth._load_xsuaa_config", return_value=self.config),
            patch("app.auth._get_jwks_client", return_value=self.jwks_client),
            patch("app.auth.jwt.decode", return_value=claims),
        ):
            with self.assertRaises(HTTPException) as raised:
                require_xsuaa_token("Bearer signed-token")

        self.assertEqual(raised.exception.status_code, 403)

    def test_requires_raw_databricks_bearer_header(self):
        self.assertEqual(require_databricks_bearer(" databricks-token "), "databricks-token")
        for token in (None, "", "Bearer databricks-token", "token with spaces"):
            with self.subTest(token=token):
                with self.assertRaises(HTTPException) as raised:
                    require_databricks_bearer(token)
                self.assertEqual(raised.exception.status_code, 401)

    def test_reports_missing_xsuaa_configuration(self):
        with patch(
            "app.auth._load_xsuaa_config",
            side_effect=XSUAAConfigurationError("missing"),
        ):
            with self.assertRaises(HTTPException) as raised:
                require_xsuaa_token("Bearer signed-token")

        self.assertEqual(raised.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
