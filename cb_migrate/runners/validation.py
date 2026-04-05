"""
Runner for document validation schema migrations.

Stores JSON Schema definitions as documents in a dedicated
`_cb_migrate_schemas` collection within each bucket's `_default` scope.
Applications can retrieve these at startup to validate documents before writes.
"""

import json
from datetime import datetime, timezone

from .base import BaseRunner

SCHEMA_COLLECTION = "_cb_migrate_schemas"
SCHEMA_SCOPE = "_default"


class ValidationRunner(BaseRunner):
    def apply(self, migration: dict) -> None:
        operation = migration.get("operation", "upsert").lower()
        if operation in ("create", "upsert", "update"):
            self._upsert_schema(migration)
        elif operation == "drop":
            self._drop_schema(migration)
        else:
            raise ValueError(f"Unknown validation operation: {operation!r}")

    def _ensure_schema_collection(self, bucket_name: str) -> None:
        """Create the schema metadata collection if it doesn't exist."""
        from couchbase.exceptions import CollectionAlreadyExistsException
        from couchbase.management.collections import CollectionSpec

        mgr = self.cluster.bucket(bucket_name).collections()
        spec = CollectionSpec(SCHEMA_COLLECTION, scope_name=SCHEMA_SCOPE)
        try:
            mgr.create_collection(spec)
            self._log(f"Created metadata collection `{SCHEMA_COLLECTION}` in `{bucket_name}`")
        except CollectionAlreadyExistsException:
            pass  # Already exists, that's fine

    def _upsert_schema(self, migration: dict) -> None:
        bucket_name = migration["bucket"]
        scope = migration.get("scope", "_default")
        collection = migration["collection"]
        schema = migration["schema"]

        doc_key = f"{bucket_name}.{scope}.{collection}"
        doc = {
            "bucket": bucket_name,
            "scope": scope,
            "collection": collection,
            "schema": schema,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "description": migration.get("description", ""),
        }

        self._log(
            f"UPSERT VALIDATION SCHEMA for `{bucket_name}`.`{scope}`.`{collection}` "
            f"(key={doc_key!r})"
        )

        if not self.dry_run:
            self._ensure_schema_collection(bucket_name)
            cb = (
                self.cluster
                .bucket(bucket_name)
                .scope(SCHEMA_SCOPE)
                .collection(SCHEMA_COLLECTION)
            )
            cb.upsert(doc_key, doc)

    def _drop_schema(self, migration: dict) -> None:
        bucket_name = migration["bucket"]
        scope = migration.get("scope", "_default")
        collection = migration["collection"]

        doc_key = f"{bucket_name}.{scope}.{collection}"
        self._log(f"DROP VALIDATION SCHEMA for `{bucket_name}`.`{scope}`.`{collection}`")

        if not self.dry_run:
            from couchbase.exceptions import DocumentNotFoundException
            try:
                cb = (
                    self.cluster
                    .bucket(bucket_name)
                    .scope(SCHEMA_SCOPE)
                    .collection(SCHEMA_COLLECTION)
                )
                cb.remove(doc_key)
            except DocumentNotFoundException:
                self._log(f"Schema document {doc_key!r} not found — skipping")
