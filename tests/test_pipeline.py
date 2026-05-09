"""Smoke tests for the synthetic data generator + extractor + landing path."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb
import pytest

from extractors.hris_workday.cli import run_file_mode
from sample_data.generate import Generator


@pytest.fixture(scope="module")
def small_demo(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """Generate a tiny dataset and land it into a temp DuckDB once per module."""
    out_dir = tmp_path_factory.mktemp("synth")
    db_path = tmp_path_factory.mktemp("warehouse") / "test.duckdb"
    Generator(employees=50, years=1, seed=1, output_dir=out_dir).run()
    run_file_mode(out_dir, db_path, truncate_first=True)
    return out_dir, db_path


def test_generator_emits_expected_files(small_demo: tuple[Path, Path]) -> None:
    out_dir, _ = small_demo
    for name in (
        "workday_workers.json",
        "workday_persons.json",
        "workday_employment_events.json",
        "_manifest.json",
    ):
        assert (out_dir / name).exists()


def test_generator_is_deterministic(tmp_path: Path) -> None:
    a = tmp_path / "a"
    b = tmp_path / "b"
    Generator(employees=20, years=1, seed=42, output_dir=a).run()
    Generator(employees=20, years=1, seed=42, output_dir=b).run()
    workers_a = json.loads((a / "workday_workers.json").read_text())
    workers_b = json.loads((b / "workday_workers.json").read_text())
    # Identical seed → identical worker_ids and counts.
    assert [w["worker_id"] for w in workers_a] == [w["worker_id"] for w in workers_b]


def test_landing_creates_raw_tables(small_demo: tuple[Path, Path]) -> None:
    _, db_path = small_demo
    con = duckdb.connect(str(db_path))
    tables = {row[0] for row in con.execute(
        "select table_name from information_schema.tables where table_schema='raw'"
    ).fetchall()}
    assert {"hris_workday__workers", "hris_workday__profile_versions",
            "hris_workday__persons", "hris_workday__employment_events"} <= tables


def test_landing_metadata_columns_populated(small_demo: tuple[Path, Path]) -> None:
    _, db_path = small_demo
    con = duckdb.connect(str(db_path))
    row = con.execute(
        "select _ingested_at_utc, _source_system, _payload_hash, raw_payload "
        "from raw.hris_workday__workers limit 1"
    ).fetchone()
    assert row[0] is not None
    assert row[1] == "hris_workday"
    assert len(row[2]) == 64  # sha256 hex
    assert json.loads(row[3])["worker_id"]


def test_active_today_is_positive(small_demo: tuple[Path, Path]) -> None:
    out_dir, _ = small_demo
    manifest = json.loads((out_dir / "_manifest.json").read_text())
    assert manifest["active_today"] > 0
    assert manifest["active_today"] <= manifest["employments"]
