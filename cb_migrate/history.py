"""Migration history tracking via migration_history.json in the repo root."""

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


HISTORY_FILE = Path(__file__).parent.parent / "migration_history.json"


def _load() -> dict:
    if not HISTORY_FILE.exists():
        return {"applied": []}
    with open(HISTORY_FILE) as f:
        return json.load(f)


def _save(data: dict) -> None:
    with open(HISTORY_FILE, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def checksum(file_path: Path) -> str:
    """SHA-256 checksum of a migration file."""
    h = hashlib.sha256()
    h.update(file_path.read_bytes())
    return h.hexdigest()


def applied_versions() -> List[str]:
    return [entry["version"] for entry in _load()["applied"]]


def get_entry(version: str) -> Optional[dict]:
    for entry in _load()["applied"]:
        if entry["version"] == version:
            return entry
    return None


def record(version: str, description: str, filename: str, file_path: Path) -> None:
    """Record a successfully applied migration."""
    data = _load()
    data["applied"].append({
        "version": version,
        "description": description,
        "filename": filename,
        "checksum": checksum(file_path),
        "applied_at": datetime.now(timezone.utc).isoformat(),
    })
    _save(data)


def verify_checksums(migrations_dir: Path) -> List[dict]:
    """
    Verify that previously applied migrations have not been altered.
    Returns a list of violations (empty list = all clean).
    """
    violations = []
    for entry in _load()["applied"]:
        file_path = migrations_dir / entry["filename"]
        if not file_path.exists():
            violations.append({
                "filename": entry["filename"],
                "issue": "File missing from migrations directory",
            })
            continue
        current = checksum(file_path)
        if current != entry["checksum"]:
            violations.append({
                "filename": entry["filename"],
                "issue": f"Checksum mismatch (expected {entry['checksum'][:8]}…, got {current[:8]}…)",
            })
    return violations


def get_history() -> List[dict]:
    return _load()["applied"]
