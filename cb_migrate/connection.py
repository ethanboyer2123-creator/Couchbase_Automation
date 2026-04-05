"""Couchbase cluster connection management."""

import os
from typing import Optional

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
from datetime import timedelta


def get_connection(config: Optional[dict] = None) -> Cluster:
    """
    Create and return an authenticated Couchbase cluster connection.

    Reads from the provided config dict first, then falls back to
    environment variables: CB_HOST, CB_USERNAME, CB_PASSWORD.
    """
    cfg = config or {}

    host = cfg.get("host") or os.environ.get("CB_HOST", "localhost")
    username = cfg.get("username") or os.environ.get("CB_USERNAME")
    password = cfg.get("password") or os.environ.get("CB_PASSWORD")

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

    connection_string = f"couchbase://{host}"
    cluster = Cluster(connection_string, options)

    # Wait until the cluster is ready
    cluster.wait_until_ready(timedelta(seconds=30))
    return cluster
