"""End-to-end demo orchestrator.

Runs:
  1. Synthetic data generation
  2. HRIS extractor (file mode) → DuckDB raw
  3. dbt build (run + test) against DuckDB
  4. Prints headline metrics so the user can verify it worked

Designed to be the only command a reviewer needs to run after ``uv sync``::

    uv run pa-setup
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import click
import duckdb

from extractors.hris_workday.cli import run_file_mode
from sample_data.generate import Generator

REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DIR = REPO_ROOT / "sample_data" / "generated"
DB_PATH = REPO_ROOT / "warehouse" / "people.duckdb"
DBT_DIR = REPO_ROOT / "dbt_project"


def _step(label: str) -> None:
    click.secho(f"\n>> {label}", fg="cyan", bold=True)


def _run_dbt(args: list[str]) -> None:
    cmd = ["dbt", *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    click.echo(f"  $ {' '.join(cmd)}")
    env = os.environ.copy()
    env["PA_DUCKDB_PATH"] = str(DB_PATH)
    result = subprocess.run(cmd, env=env, cwd=str(REPO_ROOT))
    if result.returncode != 0:
        raise click.ClickException(f"dbt step failed: {' '.join(args)}")


@click.command()
@click.option("--employees", default=1000, help="Steady-state synthetic employees.")
@click.option("--years", default=3, help="Years of history.")
@click.option("--seed", default=42)
@click.option("--reset", is_flag=True, help="Wipe warehouse + generated data first.")
@click.option("--skip-generate", is_flag=True, help="Re-use existing generated JSON.")
def main(employees: int, years: int, seed: int, reset: bool, skip_generate: bool) -> None:
    """Generate → land → build → test, end to end, on DuckDB."""
    if reset:
        _step("Resetting warehouse and generated data")
        if DB_PATH.exists():
            DB_PATH.unlink()
        if SAMPLE_DIR.exists():
            shutil.rmtree(SAMPLE_DIR)

    if not skip_generate:
        _step(f"Generating synthetic data ({employees} employees, {years} years, seed={seed})")
        Generator(employees=employees, years=years, seed=seed, output_dir=SAMPLE_DIR).run()
    else:
        click.echo("  (skipping data generation)")

    _step("Landing source payloads into DuckDB raw schema")
    counts = run_file_mode(SAMPLE_DIR, DB_PATH, truncate_first=True)
    for k, v in counts.items():
        click.echo(f"    {k}: {v} rows")

    _step("dbt deps (no-op if no packages.yml)")
    if (DBT_DIR / "packages.yml").exists():
        _run_dbt(["deps"])
    else:
        click.echo("  (no packages.yml; skipping)")

    _step("dbt build (run models + run tests)")
    _run_dbt(["build", "--target", "dev"])

    _step("Headline metrics from mart_workforce_metrics_daily")
    con = duckdb.connect(str(DB_PATH))
    rows = con.execute(
        """
        select date_day, active_headcount, total_fte, hires, terminations, net_headcount_change
        from marts.mart_workforce_metrics_daily
        where active_headcount > 0
        order by date_day desc
        limit 7
        """
    ).fetchall()
    summary = con.execute(
        """
        select
            min(date_day) as first_day,
            max(date_day) as last_day,
            max(active_headcount) as peak_headcount,
            sum(hires) as total_hires,
            sum(terminations) as total_terms
        from marts.mart_workforce_metrics_daily
        where active_headcount > 0
        """
    ).fetchone()
    click.echo(
        f"    coverage: {summary[0]} -> {summary[1]}  "
        f"peak headcount: {summary[2]}  "
        f"hires: {summary[3]}  terms: {summary[4]}"
    )
    click.echo(f"    last 7 days of data:")
    click.echo(f"    {'date':<12}{'headcount':>10}{'fte':>10}{'hires':>8}{'terms':>8}{'net':>6}")
    for r in rows:
        click.echo(f"    {str(r[0]):<12}{r[1]:>10}{float(r[2]):>10.1f}{r[3]:>8}{r[4]:>8}{r[5]:>6}")

    _step("Done.")
    click.secho(f"  Warehouse: {DB_PATH}", fg="green")
    click.secho("  Try: uv run python -c \"import duckdb; "
                "print(duckdb.connect('warehouse/people.duckdb')"
                ".execute('select * from marts.mart_workforce_metrics_daily limit 5').fetchdf())\"",
                fg="green")


if __name__ == "__main__":
    main()
