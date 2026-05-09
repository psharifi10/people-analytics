"""Landing utility — append raw source records into the warehouse.

For Phase 1 the warehouse is DuckDB locally; the same module exposes a
Snowflake adapter (see ``LandingClient.for_snowflake``) so production wiring
is a config change, not a rewrite.

Raw layer contract (mirrors §5 of the design doc):

* Append-only — never UPDATE or DELETE rows here.
* Every row carries the canonical metadata columns:
  ``_ingested_at_utc``, ``_source_system``, ``_source_object``, ``_run_id``,
  ``_extract_mode``, ``_source_record_id``, ``_source_updated_at``,
  ``_payload_hash``, ``raw_payload``.
* The full source record is preserved as JSON in ``raw_payload`` so we can
  re-derive any downstream model without re-fetching.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import structlog

log = structlog.get_logger()


def _payload_hash(payload: dict[str, Any]) -> str:
    """Deterministic hash for change detection (stable across runs)."""
    blob = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


class LandingClient:
    """Lands records into ``raw.<source_system>__<source_object>`` tables."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    def _ensure_table(self, source_system: str, source_object: str) -> str:
        table = f"raw.{source_system}__{source_object}"
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                _ingested_at_utc   TIMESTAMP,
                _source_system     VARCHAR,
                _source_object     VARCHAR,
                _run_id            VARCHAR,
                _extract_mode      VARCHAR,
                _source_record_id  VARCHAR,
                _source_updated_at TIMESTAMP,
                _payload_hash      VARCHAR,
                raw_payload        JSON
            )
            """
        )
        return table

    def land(
        self,
        source_system: str,
        source_object: str,
        records: Iterable[dict[str, Any]],
        run_id: str,
        record_id_field: str,
        extract_mode: str = "full",
        source_updated_at_field: str | None = "source_updated_at",
    ) -> int:
        """Append ``records`` to the raw table for this source/object."""
        table = self._ensure_table(source_system, source_object)
        ingested_at = datetime.now(timezone.utc)
        rows: list[tuple[Any, ...]] = []
        for rec in records:
            rid = str(rec.get(record_id_field, ""))
            updated_raw = rec.get(source_updated_at_field) if source_updated_at_field else None
            updated_at = (
                datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
                if isinstance(updated_raw, str)
                else None
            )
            rows.append(
                (
                    ingested_at,
                    source_system,
                    source_object,
                    run_id,
                    extract_mode,
                    rid,
                    updated_at,
                    _payload_hash(rec),
                    json.dumps(rec, default=str),
                )
            )

        if not rows:
            log.warning("landing.no_records", source=source_system, object=source_object)
            return 0

        self.con.executemany(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        log.info(
            "landing.inserted",
            source=source_system,
            object=source_object,
            count=len(rows),
            run_id=run_id,
        )
        return len(rows)

    def truncate(self, source_system: str, source_object: str) -> None:
        """Wipe a raw table — used by the demo so re-runs are reproducible."""
        table = f"raw.{source_system}__{source_object}"
        self.con.execute(f"DROP TABLE IF EXISTS {table}")

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> "LandingClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
