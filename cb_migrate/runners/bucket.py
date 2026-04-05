"""Runner for bucket create/update/drop operations."""

from couchbase.exceptions import BucketAlreadyExistsException, BucketNotFoundException
from couchbase.management.buckets import (
    BucketSettings,
    BucketType,
    ConflictResolutionType,
    CreateBucketSettings,
    StorageBackend,
)

from .base import BaseRunner

_BUCKET_TYPES = {
    "couchbase": BucketType.Couchbase,
    "ephemeral": BucketType.Ephemeral,
    "memcached": BucketType.Memcached,
}

_STORAGE_BACKENDS = {
    "couchstore": StorageBackend.Couchstore,
    "magma": StorageBackend.Magma,
}

_CONFLICT_RESOLUTION = {
    "timestamp": ConflictResolutionType.Timestamp,
    "sequence_number": ConflictResolutionType.SequenceNumber,
}


class BucketRunner(BaseRunner):
    def apply(self, migration: dict) -> None:
        operation = migration.get("operation", "create").lower()
        if operation == "create":
            self._create(migration)
        elif operation == "update":
            self._update(migration)
        elif operation == "drop":
            self._drop(migration)
        else:
            raise ValueError(f"Unknown bucket operation: {operation!r}")

    def _create(self, migration: dict) -> None:
        name = migration["name"]
        settings = migration.get("settings", {})

        bucket_type_str = settings.get("bucket_type", "couchbase").lower()
        bucket_type = _BUCKET_TYPES.get(bucket_type_str, BucketType.Couchbase)

        kwargs = {
            "name": name,
            "ram_quota_mb": settings.get("ram_quota_mb", 256),
            "bucket_type": bucket_type,
            "num_replicas": settings.get("num_replicas", 1),
            "flush_enabled": settings.get("flush_enabled", False),
            "max_expiry": settings.get("max_expiry", 0),
        }

        if "storage_backend" in settings:
            kwargs["storage_backend"] = _STORAGE_BACKENDS[settings["storage_backend"]]

        if "conflict_resolution_type" in settings:
            kwargs["conflict_resolution_type"] = _CONFLICT_RESOLUTION[settings["conflict_resolution_type"]]

        self._log(f"CREATE BUCKET `{name}` (quota={kwargs['ram_quota_mb']}MB, replicas={kwargs['num_replicas']})")
        if not self.dry_run:
            try:
                self.cluster.buckets().create_bucket(CreateBucketSettings(**kwargs))
            except BucketAlreadyExistsException:
                self._log(f"Bucket `{name}` already exists — skipping")

    def _update(self, migration: dict) -> None:
        name = migration["name"]
        settings = migration.get("settings", {})
        self._log(f"UPDATE BUCKET `{name}` settings={settings}")
        if not self.dry_run:
            mgr = self.cluster.buckets()
            existing = mgr.get_bucket(name)
            if "ram_quota_mb" in settings:
                existing.ram_quota_mb = settings["ram_quota_mb"]
            if "num_replicas" in settings:
                existing.num_replicas = settings["num_replicas"]
            if "flush_enabled" in settings:
                existing.flush_enabled = settings["flush_enabled"]
            mgr.update_bucket(existing)

    def _drop(self, migration: dict) -> None:
        name = migration["name"]
        self._log(f"DROP BUCKET `{name}`")
        if not self.dry_run:
            try:
                self.cluster.buckets().drop_bucket(name)
            except BucketNotFoundException:
                self._log(f"Bucket `{name}` does not exist — skipping")
