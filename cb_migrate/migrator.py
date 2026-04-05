"""Core migration orchestration — discover, diff, and apply migrations."""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import yaml

from . import history
from .runners import BucketRunner, CollectionRunner, IndexRunner, ValidationRunner

MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"

# Maps top-level YAML `type` values to runner classes
_YAML_RUNNERS = {
    "bucket": BucketRunner,
    "scope": CollectionRunner,
    "collection": CollectionRunner,
    "index": IndexRunner,
    "validation": ValidationRunner,
}

_VERSION_RE = re.compile(r"^(V\d{3,})__(.+)\.(yml|yaml|n1ql)$", re.IGNORECASE)


@dataclass
class MigrationFile:
    version: str       # e.g. "V001"
    description: str   # e.g. "create_initial_buckets"
    filename: str      # e.g. "V001__create_initial_buckets.yml"
    path: Path
    ext: str           # "yml" or "n1ql"


def discover(migrations_dir: Path = MIGRATIONS_DIR) -> List[MigrationFile]:
    """Return all migration files sorted by version."""
    files = []
    for f in migrations_dir.iterdir():
        m = _VERSION_RE.match(f.name)
        if m:
            files.append(MigrationFile(
                version=m.group(1).upper(),
                description=m.group(2),
                filename=f.name,
                path=f,
                ext=m.group(3).lower(),
            ))
    files.sort(key=lambda x: x.version)
    return files


def pending(migrations_dir: Path = MIGRATIONS_DIR) -> List[MigrationFile]:
    """Return migrations not yet recorded in history."""
    applied = set(history.applied_versions())
    return [f for f in discover(migrations_dir) if f.version not in applied]


def _load_migration(mf: MigrationFile) -> dict:
    """Parse a migration file into a dict."""
    if mf.ext in ("yml", "yaml"):
        with open(mf.path) as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise ValueError(f"Migration {mf.filename} must be a YAML mapping at the top level.")
        return data
    elif mf.ext == "n1ql":
        return {"_raw_sql": mf.path.read_text()}
    raise ValueError(f"Unsupported migration extension: .{mf.ext}")


def _get_runner_class(mf: MigrationFile, data: dict):
    if mf.ext == "n1ql":
        return IndexRunner
    resource_type = data.get("type", "").lower()
    runner_cls = _YAML_RUNNERS.get(resource_type)
    if runner_cls is None:
        raise ValueError(
            f"Unknown migration type {resource_type!r} in {mf.filename}. "
            f"Valid types: {', '.join(_YAML_RUNNERS)}"
        )
    return runner_cls


def apply_all(
    cluster,
    migrations_dir: Path = MIGRATIONS_DIR,
    dry_run: bool = False,
    target: Optional[str] = None,
) -> int:
    """
    Apply all pending migrations up to (and including) `target` version.
    Returns the count of migrations applied.
    """
    violations = history.verify_checksums(migrations_dir)
    if violations:
        lines = "\n".join(f"  - {v['filename']}: {v['issue']}" for v in violations)
        raise RuntimeError(
            f"Checksum verification failed for previously applied migrations:\n{lines}\n"
            "Do not modify applied migration files. Create a new migration instead."
        )

    to_apply = pending(migrations_dir)
    if target:
        target_upper = target.upper()
        to_apply = [m for m in to_apply if m.version <= target_upper]

    if not to_apply:
        print("Nothing to apply — database is up to date.")
        return 0

    applied_count = 0
    for mf in to_apply:
        print(f"\n[{mf.version}] {mf.description}")
        data = _load_migration(mf)
        runner_cls = _get_runner_class(mf, data)
        runner = runner_cls(cluster, dry_run=dry_run)
        runner.apply(data)

        if not dry_run:
            history.record(mf.version, mf.description, mf.filename, mf.path)
            print(f"  ✓ Applied")
        else:
            print(f"  ✓ Dry-run complete")

        applied_count += 1

    return applied_count
