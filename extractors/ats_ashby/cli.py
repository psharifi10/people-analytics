"""Ashby-shaped ATS extractor.

Mirrors the HRIS extractor pattern (extractors/hris_workday/cli.py) so the
raw-layer contract is identical across sources. The five Ashby objects we
land:

* ``jobs``            one row per requisition / opening.
* ``candidates``      one row per candidate (person submitting applications).
* ``applications``    one row per application (candidate to a specific job).
* ``application_stage_events``  one row per stage transition. Unfurled here
  from the ``stage_history`` array inside each application, exactly the way
  the HRIS extractor unfurls ``profile_history`` into ``profile_versions``.
  Keeping the raw layer flat means downstream dbt models never have to
  flatten a VARIANT array.
* ``offers``          one row per offer (or offer version).

Two operating modes:

* ``file`` reads JSON payloads produced by ``sample_data/generate_ats.py``.
  Used for the local demo and CI; no credentials needed.
* ``api`` is a placeholder for the real Ashby API integration. Ashby uses
  RPC-style POST endpoints with HTTP Basic auth, cursor pagination
  (``nextCursor``) and incremental sync via ``syncToken``. Implementing it
  is a configuration change, not a redesign: the landing contract here is
  identical.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import click
import structlog

from extractors.common.landing import LandingClient

log = structlog.get_logger()

SOURCE_SYSTEM = "ats_ashby"

DEFAULT_SAMPLE_DIR = Path(__file__).resolve().parents[2] / "sample_data" / "generated"
DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "warehouse" / "people.duckdb"

# Raw object names landed by this extractor. Order matters only for
# truncate / land readability; the raw layer has no FK constraints.
RAW_OBJECTS = (
    "jobs",
    "candidates",
    "applications",
    "application_stage_events",
    "offers",
)


def run_file_mode(
    sample_dir: Path,
    db_path: Path,
    truncate_first: bool = True,
) -> dict[str, int]:
    """Read Ashby JSON files from ``sample_dir`` and land them into ``db_path``."""
    # One run_id per extraction. Propagates through every landed row and
    # downstream into dim/fact projections as ingested_run_id for lineage.
    run_id = str(uuid4())
    log.info("ats.extract.start", mode="file", run_id=run_id, sample_dir=str(sample_dir))

    jobs_payload = json.loads((sample_dir / "ashby_jobs.json").read_text())
    candidates_payload = json.loads((sample_dir / "ashby_candidates.json").read_text())
    applications_payload = json.loads((sample_dir / "ashby_applications.json").read_text())
    offers_payload = json.loads((sample_dir / "ashby_offers.json").read_text())

    # Unfurl stage_history into its own flat object. Mirrors the
    # workers.profile_history -> profile_versions unfurl in the HRIS
    # extractor. Composite natural key:
    #   application_id | transitioned_at | to_stage
    # (to_stage included so simultaneous transitions on the same app stay
    # unique; in practice transitioned_at is already unique per app.)
    stage_events: list[dict[str, object]] = []
    applications_flat: list[dict[str, object]] = []
    for app in applications_payload:
        history = app.pop("stage_history", []) or []
        for ev in history:
            stage_events.append(
                {
                    "stage_event_id": (
                        f"{app['application_id']}|{ev['transitioned_at']}|{ev['to_stage']}"
                    ),
                    "application_id": app["application_id"],
                    "candidate_id": app["candidate_id"],
                    "requisition_id": app["requisition_id"],
                    "from_stage": ev.get("from_stage"),
                    "to_stage": ev["to_stage"],
                    "transitioned_at": ev["transitioned_at"],
                    "source_updated_at": app["source_updated_at"],
                }
            )
        applications_flat.append(app)

    counts: dict[str, int] = {}
    with LandingClient(db_path) as land:
        if truncate_first:
            # Demo mode: wipe before re-loading so re-runs are deterministic.
            # Production would set truncate_first=False and rely on dbt
            # incremental dedup logic instead.
            for obj in RAW_OBJECTS:
                land.truncate(SOURCE_SYSTEM, obj)

        counts["jobs"] = land.land(
            SOURCE_SYSTEM, "jobs", jobs_payload, run_id, record_id_field="requisition_id"
        )
        counts["candidates"] = land.land(
            SOURCE_SYSTEM,
            "candidates",
            candidates_payload,
            run_id,
            record_id_field="candidate_id",
        )
        counts["applications"] = land.land(
            SOURCE_SYSTEM,
            "applications",
            applications_flat,
            run_id,
            record_id_field="application_id",
        )
        counts["application_stage_events"] = land.land(
            SOURCE_SYSTEM,
            "application_stage_events",
            stage_events,
            run_id,
            record_id_field="stage_event_id",
        )
        counts["offers"] = land.land(
            SOURCE_SYSTEM,
            "offers",
            offers_payload,
            run_id,
            record_id_field="offer_id",
        )

    log.info("ats.extract.done", run_id=run_id, counts=counts)
    return counts


@click.command()
@click.option("--mode", type=click.Choice(["file", "api"]), default="file")
@click.option("--sample-dir", default=str(DEFAULT_SAMPLE_DIR))
@click.option("--db-path", default=str(DEFAULT_DB_PATH))
@click.option("--no-truncate", is_flag=True, help="Append rather than replace existing raw rows.")
def main(mode: str, sample_dir: str, db_path: str, no_truncate: bool) -> None:
    """Extract Ashby ATS data into the RAW layer."""
    if mode == "api":
        raise click.ClickException(
            "API mode is a future deliverable; use --mode file with synthetic data for the demo."
        )
    counts = run_file_mode(Path(sample_dir), Path(db_path), truncate_first=not no_truncate)
    click.echo("Landed:")
    for k, v in counts.items():
        click.echo(f"  {k}: {v} rows")


if __name__ == "__main__":
    main()
