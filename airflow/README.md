# Airflow stack

Runs the `hris_daily` DAG locally against the same DuckDB warehouse the
notebook and CLI use. Optional — `uv run pa-setup` does the same work without
Docker. This stack exists to demonstrate production-shape orchestration
(scheduler, retries, task isolation, UI).

## Quick start

```powershell
# From the repo root.

# 1. Bootstrap the metadata DB and admin user (one-shot).
docker compose up airflow-init

# 2. Start scheduler + webserver in the background.
docker compose up -d

# 3. Open the UI.
start http://localhost:8080
#    Username: airflow
#    Password: airflow

# 4. Un-pause the `hris_daily` DAG. Trigger it manually with the play button,
#    or wait for the 06:00 UTC schedule.

# 5. Tear down.
docker compose down            # stop containers, keep metadata DB
docker compose down -v         # also wipe metadata DB
```

## What the DAG does

```
extract_hris  →  dbt_build  →  dbt_source_freshness
```

* **`extract_hris`** — calls `extractors.hris_workday.cli.run_file_mode` with
  `truncate_first=False` (production semantics: append, never replace).
* **`dbt_build`** — runs `dbt build --target prod --select source:raw_hris+`,
  i.e. every model downstream of the HRIS source plus all its tests.
* **`dbt_source_freshness`** — verifies raw rows are recent enough; alerts on
  stale upstream data.

## How the container sees your project

`docker-compose.yml` mounts the host paths into the Airflow container:

| Host path        | Container path                  |
|------------------|---------------------------------|
| `./airflow/dags` | `/opt/airflow/dags`             |
| `./dbt_project`  | `/opt/airflow/dbt_project`      |
| `./warehouse`    | `/opt/airflow/warehouse`        |
| `./sample_data`  | `/opt/airflow/sample_data`      |
| `./extractors`   | `/opt/airflow/extractors`       |

That is why the DAG references `/opt/airflow/...` paths but still touches your
real repo files. Edits to a DAG or to the dbt project show up immediately —
no rebuild needed (Airflow re-parses DAGs every ~30 seconds).

## Production deployment notes

This stack is **local-only**. In production you'd swap it for one of:

* **Astronomer / MWAA / Cloud Composer** — push `airflow/dags/` to a managed
  Airflow service. No scheduler or Postgres to operate.
* **Azure Container Apps Jobs / GCP Cloud Run Jobs** — for ≤ 5 DAGs the
  whole orchestration layer can be replaced with a cron-triggered container.
  Cheaper, less to learn, less to break.
* **dbt Cloud** — handles the dbt half; pair it with a small Python container
  for the extractor.

The DAG code itself does not change between any of these.
