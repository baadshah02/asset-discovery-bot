"""Apply every ``.sql`` file in this directory in lexicographic order.

The script is intentionally minimal: it does not track a migration version,
because every statement inside each ``.sql`` file uses ``IF NOT EXISTS``
semantics (Req 7.1). Re-running the script leaves the schema unchanged.

Invoke once per deploy (and during local development) via::

    docker compose run --rm bot python -m bot.migrations.run_migrations

The database URL is read from the ``Secrets`` loader (the ``db_url`` field
resolved from ``/run/secrets/db_url``). No argparse surface — a single
environment (the ``Secrets`` object) owns credentials and connection info.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from bot.config import load_config

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent


def discover_migrations(migrations_dir: Path = MIGRATIONS_DIR) -> list[Path]:
    """Return every ``.sql`` file in ``migrations_dir`` sorted lexicographically.

    The lexicographic ordering is what gives the ``NNN_`` prefix its meaning.
    ``001_init.sql`` always runs before ``002_*.sql``, etc.
    """
    return sorted(migrations_dir.glob("*.sql"))


def apply_migration(engine: Engine, path: Path) -> None:
    """Execute a single ``.sql`` file as one transaction.

    psycopg splits the file on ``;`` boundaries and executes statements in
    order. Any failure rolls back the entire file — partial application of
    a migration is never persisted.
    """
    sql = path.read_text(encoding="utf-8")
    logger.info("Applying migration: %s", path.name)
    with engine.begin() as conn:
        conn.execute(text(sql))


def main() -> int:
    """Apply all migrations. Returns 0 on success, non-zero on failure."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        _cfg, secrets = load_config()
    except Exception as exc:  # pragma: no cover - fail-fast on bad config
        logger.error("Config load failed: %s", exc)
        return 2

    engine: Engine = create_engine(secrets.db_url, future=True)
    try:
        migrations = discover_migrations()
        if not migrations:
            logger.warning("No migrations found in %s", MIGRATIONS_DIR)
            return 0
        logger.info("Discovered %d migration(s)", len(migrations))
        for path in migrations:
            apply_migration(engine, path)
        logger.info("All migrations applied successfully.")
        return 0
    except Exception as exc:  # pragma: no cover - surface via exit code
        logger.exception("Migration failed: %s", exc)
        return 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
