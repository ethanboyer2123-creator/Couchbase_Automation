"""Runner for scope and collection create/drop operations."""

from datetime import timedelta

from couchbase.exceptions import (
    CollectionAlreadyExistsException,
    CollectionNotFoundException,
    ScopeAlreadyExistsException,
    ScopeNotFoundException,
)
from couchbase.management.collections import CollectionSpec

from .base import BaseRunner


class CollectionRunner(BaseRunner):
    def apply(self, migration: dict) -> None:
        operation = migration.get("operation", "create").lower()
        resource_type = migration.get("type", "").lower()

        if resource_type == "scope":
            if operation == "create":
                self._create_scope(migration)
            elif operation == "drop":
                self._drop_scope(migration)
            else:
                raise ValueError(f"Unknown scope operation: {operation!r}")
        elif resource_type == "collection":
            if operation == "create":
                self._create_collection(migration)
            elif operation == "drop":
                self._drop_collection(migration)
            else:
                raise ValueError(f"Unknown collection operation: {operation!r}")
        else:
            raise ValueError(f"CollectionRunner requires type 'scope' or 'collection', got: {resource_type!r}")

    def _get_collection_manager(self, bucket_name: str):
        bucket = self.cluster.bucket(bucket_name)
        return bucket.collections()

    def _create_scope(self, migration: dict) -> None:
        bucket = migration["bucket"]
        scope = migration["name"]
        self._log(f"CREATE SCOPE `{bucket}`.`{scope}`")
        if not self.dry_run:
            try:
                self._get_collection_manager(bucket).create_scope(scope)
            except ScopeAlreadyExistsException:
                self._log(f"Scope `{scope}` already exists — skipping")

    def _drop_scope(self, migration: dict) -> None:
        bucket = migration["bucket"]
        scope = migration["name"]
        self._log(f"DROP SCOPE `{bucket}`.`{scope}`")
        if not self.dry_run:
            try:
                self._get_collection_manager(bucket).drop_scope(scope)
            except ScopeNotFoundException:
                self._log(f"Scope `{scope}` does not exist — skipping")

    def _create_collection(self, migration: dict) -> None:
        bucket = migration["bucket"]
        scope = migration.get("scope", "_default")
        name = migration["name"]
        max_expiry_seconds = migration.get("max_expiry_seconds", 0)
        max_expiry = timedelta(seconds=max_expiry_seconds)

        self._log(f"CREATE COLLECTION `{bucket}`.`{scope}`.`{name}`")
        if not self.dry_run:
            spec = CollectionSpec(name, scope_name=scope, max_expiry=max_expiry)
            try:
                self._get_collection_manager(bucket).create_collection(spec)
            except CollectionAlreadyExistsException:
                self._log(f"Collection `{name}` already exists in `{scope}` — skipping")

    def _drop_collection(self, migration: dict) -> None:
        bucket = migration["bucket"]
        scope = migration.get("scope", "_default")
        name = migration["name"]

        self._log(f"DROP COLLECTION `{bucket}`.`{scope}`.`{name}`")
        if not self.dry_run:
            spec = CollectionSpec(name, scope_name=scope)
            try:
                self._get_collection_manager(bucket).drop_collection(spec)
            except CollectionNotFoundException:
                self._log(f"Collection `{name}` does not exist — skipping")
