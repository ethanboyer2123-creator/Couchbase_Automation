"""
cb-migrate CLI entry point.

Commands:
    status      Show pending and applied migrations
    apply       Apply pending migrations to the database
    dry-run     Preview what would be applied without making changes
    history     Show full migration history from migration_history.json
    validate    Validate all migration files without connecting to Couchbase
"""

import sys
from pathlib import Path

import click
import yaml

from . import migrator
from . import history as hist
from .connection import get_connection

MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"


def _load_config(config_file: str) -> dict:
    if not config_file:
        return {}
    path = Path(config_file)
    if not path.exists():
        raise click.ClickException(f"Config file not found: {config_file}")
    with open(path) as f:
        return yaml.safe_load(f) or {}


@click.group()
@click.version_option()
def cli():
    """cb-migrate: Couchbase schema migration tool."""
    pass


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
@cli.command()
@click.option("--config", "-c", default=None, envvar="CB_CONFIG", help="Path to environment config YAML.")
def status(config):
    """Show applied and pending migrations."""
    cfg = _load_config(config)
    all_migrations = migrator.discover(MIGRATIONS_DIR)
    applied = set(hist.applied_versions())

    if not all_migrations:
        click.echo("No migration files found in migrations/")
        return

    click.echo(f"\n{'VERSION':<10} {'STATUS':<10} DESCRIPTION")
    click.echo("-" * 55)
    for mf in all_migrations:
        status_label = click.style("applied", fg="green") if mf.version in applied else click.style("pending", fg="yellow")
        click.echo(f"{mf.version:<10} {status_label:<19} {mf.description}")

    pending_count = len([m for m in all_migrations if m.version not in applied])
    click.echo(f"\n{len(applied)} applied, {pending_count} pending")


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------
@cli.command()
@click.option("--config", "-c", default=None, envvar="CB_CONFIG", help="Path to environment config YAML.")
@click.option("--target", "-t", default=None, help="Apply only up to this version (e.g. V005).")
@click.option("--yes", "-y", is_flag=True, default=False, help="Skip confirmation prompt.")
def apply(config, target, yes):
    """Apply all pending migrations."""
    cfg = _load_config(config)

    pending = migrator.pending(MIGRATIONS_DIR)
    if target:
        pending = [m for m in pending if m.version <= target.upper()]

    if not pending:
        click.echo("Nothing to apply — database is up to date.")
        return

    click.echo(f"\nPending migrations to apply ({len(pending)}):")
    for mf in pending:
        click.echo(f"  [{mf.version}] {mf.description}")

    if not yes:
        click.confirm("\nProceed?", abort=True)

    cluster = get_connection(cfg)
    count = migrator.apply_all(cluster, MIGRATIONS_DIR, dry_run=False, target=target)
    click.echo(f"\n{click.style(f'Done. {count} migration(s) applied.', fg='green')}")


# ---------------------------------------------------------------------------
# dry-run
# ---------------------------------------------------------------------------
@cli.command("dry-run")
@click.option("--config", "-c", default=None, envvar="CB_CONFIG", help="Path to environment config YAML.")
@click.option("--target", "-t", default=None, help="Preview up to this version.")
def dry_run(config, target):
    """Preview pending migrations without applying them."""
    cfg = _load_config(config)
    cluster = get_connection(cfg)
    count = migrator.apply_all(cluster, MIGRATIONS_DIR, dry_run=True, target=target)
    if count:
        click.echo(f"\n{click.style(f'Dry-run complete. {count} migration(s) would be applied.', fg='cyan')}")


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------
@cli.command()
def history():
    """Show full migration history."""
    entries = hist.get_history()
    if not entries:
        click.echo("No migrations have been applied yet.")
        return

    click.echo(f"\n{'VERSION':<10} {'APPLIED AT':<30} DESCRIPTION")
    click.echo("-" * 65)
    for e in entries:
        click.echo(f"{e['version']:<10} {e['applied_at']:<30} {e['description']}")
    click.echo(f"\nTotal: {len(entries)} migration(s)")


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------
@cli.command()
def validate():
    """Validate all migration files (no database connection required)."""
    import re

    all_files = migrator.discover(MIGRATIONS_DIR)
    if not all_files:
        click.echo("No migration files found.")
        return

    errors = []
    for mf in all_files:
        try:
            data = migrator._load_migration(mf)
            if mf.ext in ("yml", "yaml"):
                if "type" not in data:
                    errors.append(f"{mf.filename}: missing required field 'type'")
        except Exception as exc:
            errors.append(f"{mf.filename}: {exc}")

    violations = hist.verify_checksums(MIGRATIONS_DIR)
    for v in violations:
        errors.append(f"{v['filename']}: {v['issue']}")

    if errors:
        click.echo(click.style(f"\nValidation FAILED ({len(errors)} error(s)):", fg="red"))
        for err in errors:
            click.echo(f"  - {err}")
        sys.exit(1)
    else:
        click.echo(click.style(f"\nAll {len(all_files)} migration file(s) are valid.", fg="green"))


def main():
    cli()
