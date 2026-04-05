"""Base runner interface."""

from abc import ABC, abstractmethod
from couchbase.cluster import Cluster


class BaseRunner(ABC):
    def __init__(self, cluster: Cluster, dry_run: bool = False):
        self.cluster = cluster
        self.dry_run = dry_run

    @abstractmethod
    def apply(self, migration: dict) -> None:
        """Apply a single migration. Raise on failure."""
        ...

    def _log(self, msg: str) -> None:
        prefix = "[DRY-RUN] " if self.dry_run else ""
        print(f"  {prefix}{msg}")
