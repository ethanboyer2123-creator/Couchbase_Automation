"""Runner for N1QL DDL index migrations (raw SQL++ statements)."""

import re
from couchbase.exceptions import IndexAlreadyExistsException, IndexNotFoundException
from couchbase.options import QueryOptions

from .base import BaseRunner

# Statements that are read-only / non-mutating — disallow in migrations
_DISALLOWED_PREFIXES = {"SELECT", "EXPLAIN", "ADVISE", "INFER"}


def _split_statements(sql: str) -> list[str]:
    """Split a .n1ql file on semicolons, stripping comments and blanks."""
    # Remove line comments
    sql = re.sub(r"--[^\n]*", "", sql)
    # Remove block comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    statements = [s.strip() for s in sql.split(";")]
    return [s for s in statements if s]


class IndexRunner(BaseRunner):
    """
    Applies raw N1QL DDL statements from a .n1ql migration file.

    The migration dict for N1QL files has the shape:
        {"_raw_sql": "<file contents>"}
    """

    def apply(self, migration: dict) -> None:
        raw_sql = migration.get("_raw_sql", "")
        statements = _split_statements(raw_sql)

        if not statements:
            self._log("No statements found in migration — skipping")
            return

        for stmt in statements:
            first_word = stmt.split()[0].upper() if stmt.split() else ""
            if first_word in _DISALLOWED_PREFIXES:
                raise ValueError(
                    f"Statement starting with {first_word!r} is not allowed in index migrations. "
                    "Only DDL statements (CREATE/DROP/ALTER INDEX) are permitted."
                )

            self._log(f"EXECUTE: {stmt[:120]}{'…' if len(stmt) > 120 else ''}")
            if not self.dry_run:
                try:
                    result = self.cluster.query(stmt, QueryOptions(metrics=False))
                    # Consume the result to surface any errors
                    for _ in result.rows():
                        pass
                except IndexAlreadyExistsException:
                    self._log("Index already exists — skipping")
                except IndexNotFoundException:
                    self._log("Index not found for DROP — skipping")
