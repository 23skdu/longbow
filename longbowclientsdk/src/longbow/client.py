import logging
from typing import Any, Dict, Optional

from ._mixins import (
    AdminMixin,
    GraphMixin,
    RestSearchMixin,
    SearchMixin,
    SecurityMixin,
    TransportMixin,
)

__all__ = ["LongbowClient"]

logger = logging.getLogger(__name__)


class LongbowClient(
    TransportMixin,
    SearchMixin,
    RestSearchMixin,
    GraphMixin,
    SecurityMixin,
    AdminMixin,
):
    """Client for interacting with the Longbow Vector Database."""

    def __init__(
        self,
        uri: str = "grpc://localhost:3000",
        meta_uri: Optional[str] = None,
        api_key: Optional[str] = None,
        oidc_token: Optional[str] = None,
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        tls_root_certs: Optional[bytes] = None,
        tls_cert_chain: Optional[bytes] = None,
        tls_private_key: Optional[bytes] = None,
    ):
        """
        Initialize the Longbow Client.

        Args:
            uri: gRPC URI for the Data Plane (DoPut/DoGet).
            meta_uri: gRPC URI for the Control Plane (DoAction/Info). Defaults to uri.
            api_key: Optional Longbow API key or Bearer token for authentication.
            oidc_token: Optional federated OIDC JWT (used as Bearer token when api_key is not set).
            tenant_id: Optional tenant identifier injected as x-tenant-id header.
            namespace: Optional namespace identifier injected as x-namespace header.
            tls_root_certs: Optional PEM root CA certificates for TLS/mTLS.
            tls_cert_chain: Optional client certificate chain PEM for mTLS.
            tls_private_key: Optional client private key PEM for mTLS.
        """
        self.uri = uri
        self.meta_uri = meta_uri or uri
        self.api_key = api_key or oidc_token
        self.tenant_id = tenant_id
        self.namespace = namespace
        self.headers = headers or {}
        self.tls_root_certs = tls_root_certs
        self.tls_cert_chain = tls_cert_chain
        self.tls_private_key = tls_private_key

        self._data_client: Any = None
        self._meta_client: Any = None
