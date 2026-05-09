# People Analytics Foundation

> A 0-to-1 People Analytics data platform — synthetic-data-driven, runnable end-to-end on a clean clone in under a minute, no credentials required.

This repository implements the design described in [`documentation/people-analytics-design.md`](documentation/people-analytics-design.md). It demonstrates an API-first ingestion pipeline, append-only raw landing, a canonical employee-record identity model, effective-dated SCD Type 2 history, daily headcount snapshots, and a governed semantic layer — all running locally against DuckDB so a reviewer can clone and execute the entire pipeline without provisioning Snowflake.

## What this proves

- **Canonical employee record** that survives rehires (`person_key` / `employee_key` / `employment_episode_key`).
- **Effective-dated SCD2** profile history, snapshot-from-history daily headcount, point-in-time correctness.
- **Manager hierarchy** flattened via recursive CTE for span-of-control analytics.
- **Single semantic layer** — every metric defined once and consumed everywhere.
- **Tests as part of done** — dbt schema tests, custom SCD2 overlap tests, pytest for extractors.
- **Privacy as a design constraint** — restricted schemas, masking and row-access policy DDL, right-to-erasure flow.

## Architecture

See [`documentation/people-analytics-design.md`](documentation/people-analytics-design.md) for the full design. Quick view:

```
sample_data (synthetic JSON)
        │
        ▼
extractors/hris_workday  ──►  RAW (Snowflake / DuckDB)
        │
        ▼
dbt: base → stage → core (identity + SCD2) → marts → semantic
        │
        ▼
BI / ad-hoc queries
        │
Airflow orchestrates extract + dbt build
```

## Repository map

```
documentation/         Design doc (Markdown + rendered HTML)
sample_data/           Synthetic-data generator + generated JSON payloads
extractors/            Python extractors per source system
  common/              Shared HTTP client, landing utility, watermark store
  hris_workday/        Workday HRIS extractor (file mode for demo, API mode for prod)
  ats_ashby/           Ashby ATS extractor (Phase 2)
  webhooks/            Signed-webhook receiver (Phase 2)
dbt_project/           dbt project (DuckDB local, Snowflake prod)
  models/sources/      Source declarations + freshness
  models/base/         Typed flatten from raw VARIANT
  models/staging/      Renamed, deduped, latest views
  models/core/         Identity, dimensions, facts
  models/marts/        Business-ready reporting marts
  models/semantic/     Metric definitions
  snapshots/           dbt snapshots (effective-dated SCD2)
  macros/              Reusable SQL (as_of, dialect helpers)
  tests/               Custom data tests
snowflake/ddl/         Snowflake-specific DDL (roles, schemas, masking, row access)
airflow/dags/          Airflow DAG skeletons
scripts/               One-shot scripts (setup, generate, load)
tests/                 pytest suite
agent_files/           AI-agent context (instructions + session journal)
```

## Quickstart

Requires only [`uv`](https://docs.astral.sh/uv/getting-started/installation/). Python 3.12 and all dependencies are installed automatically.

```bash
# 1. Install dependencies into a managed virtualenv
uv sync

# 2. End-to-end demo: generate synth data → land → dbt build → dbt test
uv run pa-setup

# 3. (Optional) Run the Python smoke tests
uv run pytest

# 4. Inspect the warehouse
uv run python -c "import duckdb; print(duckdb.connect('warehouse/people.duckdb').execute('select * from marts.mart_workforce_metrics_daily order by date_day desc limit 5').fetchdf())"
```

The first run takes about 30 seconds and produces a populated DuckDB warehouse with:

* 9 tables and 8 views across `raw`, `base`, `staging`, `core`, `marts` schemas
* ~1,000 active employees, three years of history
* 56 dbt tests passing (uniqueness, referential integrity, SCD2 no-overlap, snapshot-implies-active-episode, etc.)
* 5 pytest smoke tests covering the generator, landing, and metadata contract

### Useful one-offs

```bash
# Regenerate data with different parameters
uv run pa-generate-data --employees 500 --years 2 --seed 7

# Re-run extractor only (after editing the JSON)
uv run pa-extract-hris

# Run dbt directly (lint, run a single model, etc.)
cd dbt_project && dbt run --select fact_headcount_snapshot_daily \
    --project-dir . --profiles-dir .
```

## Data scale (synthetic)

- ~1,000 active employees (steady-state)
- 3 years of history (hires, terminations, promotions, transfers, rehires)
- Plausible org tree (CEO → 3 functions → 8 departments → managers → ICs)
- 5 worker types (FT, PT, intern, contractor, contingent)

The generator is deterministic (seeded) so re-runs produce the same data, which makes test assertions stable.

## Production target

The Snowflake target is available for production. Set the environment variables documented in `dbt_project/profiles.yml.example`, copy that file to `~/.dbt/profiles.yml`, and run `dbt build --target prod`. The Snowflake DDL in `snowflake/ddl/` provisions roles, schemas, sequences, masking policies, and row access policies before the first dbt run.

## License

MIT — see [LICENSE](LICENSE).

## Acknowledgements

The synthetic data is generated with [Faker](https://faker.readthedocs.io). The design references public documentation from Workday, Ashby, Greenhouse, dbt, Snowflake, and Apache Airflow.
