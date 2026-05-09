"""Workday-shaped HRIS extractor.

Two operating modes:

* ``file`` — reads JSON payloads produced by ``sample_data/generate.py``.
  Used for the local demo and CI; needs no credentials.
* ``api`` — placeholder for a real Workday RaaS / REST integration.
  Implementing it is a configuration change, not a redesign: the landing
  contract is identical.

The extractor unfurls the worker payload's nested ``profile_history`` array
into its own raw table so downstream dbt models can stay flat.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import click
import structlog

from extractors.common.landing import LandingClient

log = structlog.get_logger()

SOURCE_SYSTEM = "hris_workday"

DEFAULT_SAMPLE_DIR = Path(__file__).resolve().parents[2] / "sample_data" / "generated"
DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "warehouse" / "people.duckdb"


def run_file_mode(
    sample_dir: Path,
    db_path: Path,
    truncate_first: bool = True,
) -> dict[str, int]:
    """Read JSON files from ``sample_dir`` and land them into ``db_path``."""
    run_id = str(uuid4())
    log.info("hris.extract.start", mode="file", run_id=run_id, sample_dir=str(sample_dir))

    workers_payload = json.loads((sample_dir / "workday_workers.json").read_text())
    persons_payload = json.loads((sample_dir / "workday_persons.json").read_text())
    events_payload = json.loads((sample_dir / "workday_employment_events.json").read_text())

    # Derive flat profile-versions table from the worker payload's nested array.
    versions: list[dict[str, object]] = []
    workers_flat: list[dict[str, object]] = []
    extract_ts = datetime.now(timezone.utc).isoformat()
    for w in workers_payload:
        history = w.pop("profile_history", []) or []
        for v in history:
            versions.append(
                {
                    "profile_version_id": f"{w['worker_id']}|{v['effective_date']}",
                    "worker_id": w["worker_id"],
                    "person_external_id": w["person_external_id"],
                    **v,
                    "source_updated_at": extract_ts,
                }
            )
        workers_flat.append(w)

    counts: dict[str, int] = {}
    with LandingClient(db_path) as land:
        if truncate_first:
            for obj in ("workers", "profile_versions", "persons", "employment_events"):
                land.truncate(SOURCE_SYSTEM, obj)

        counts["workers"] = land.land(
            SOURCE_SYSTEM, "workers", workers_flat, run_id, record_id_field="worker_id"
        )
        counts["profile_versions"] = land.land(
            SOURCE_SYSTEM,
            "profile_versions",
            versions,
            run_id,
            record_id_field="profile_version_id",
        )
        counts["persons"] = land.land(
            SOURCE_SYSTEM,
            "persons",
            persons_payload,
            run_id,
            record_id_field="person_external_id",
        )
        counts["employment_events"] = land.land(
            SOURCE_SYSTEM,
            "employment_events",
            events_payload,
            run_id,
            record_id_field="event_id",
        )

    log.info("hris.extract.done", run_id=run_id, counts=counts)
    return counts


@click.command()
@click.option("--mode", type=click.Choice(["file", "api"]), default="file")
@click.option("--sample-dir", default=str(DEFAULT_SAMPLE_DIR))
@click.option("--db-path", default=str(DEFAULT_DB_PATH))
@click.option("--no-truncate", is_flag=True, help="Append rather than replace existing raw rows.")
def main(mode: str, sample_dir: str, db_path: str, no_truncate: bool) -> None:
    """Extract HRIS data into the RAW layer."""
    if mode == "api":
        raise click.ClickException(
            "API mode is a Phase-2 deliverable; use --mode file with synthetic data for the demo."
        )
    counts = run_file_mode(Path(sample_dir), Path(db_path), truncate_first=not no_truncate)
    click.echo("Landed:")
    for k, v in counts.items():
        click.echo(f"  {k}: {v} rows")


if __name__ == "__main__":
    main()
