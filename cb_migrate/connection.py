"""Couchbase cluster connection management."""

import os
from typing import Optional
from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions


def get_connection(config: Optional[dict] = None) -> Cluster:
    """
    Create and return an authenticated Couchbase cluster connection.

    Reads from the provided config dict first, then falls back to
    environment variables: CB_HOST, CB_USERNAME, CB_PASSWORD.

    Capella clusters:
        Set `tls: true` in your config YAML (or CB_TLS=true) — this switches
        the connection string to `couchbases://` which enables TLS.
        Capella hostnames look like: cb.xxxxxx.cloud.couchbase.com
    """
    cfg = config or {}

    host = cfg.get("host") or os.environ.get("CB_HOST", "localhost")
    username = cfg.get("username") or os.environ.get("CB_USERNAME")
    password = cfg.get("password") or os.environ.get("CB_PASSWORD")

    # Auto-enable TLS for Capella hosts or when explicitly configured
    tls = cfg.get("tls", False) or os.environ.get("CB_TLS", "").lower() in ("true", "1", "yes")
    if not tls and "cloud.couchbase.com" in host:
        tls = True

    if not username or not password:
        raise EnvironmentError(
            "Couchbase credentials not set. "
            "Provide CB_USERNAME and CB_PASSWORD environment variables "
            "or set them in your config file."
        )

    timeout_opts = ClusterTimeoutOptions(
        connect_timeout=timedelta(seconds=30),
        kv_timeout=timedelta(seconds=10),
        query_timeout=timedelta(seconds=75),
        management_timeout=timedelta(seconds=75),
    )

    auth = PasswordAuthenticator(username, password)
    options = ClusterOptions(auth, timeout_options=timeout_opts)

    scheme = "couchbases" if tls else "couchbase"
    connection_string = f"{scheme}://{host}"
    cluster = Cluster(connection_string, options)

    cluster.wait_until_ready(timedelta(seconds=30))
    return cluster
