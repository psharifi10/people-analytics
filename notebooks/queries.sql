-- =============================================================================
-- People Analytics — starter queries
-- Run with:  duckdb warehouse\people.duckdb   then:  .read notebooks/queries.sql
-- Or open this file in VS Code w/ DuckDB SQL Tools and execute statements.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. Inventory — every table in the warehouse
-- -----------------------------------------------------------------------------
SELECT table_schema || '.' || table_name AS tbl
FROM   information_schema.tables
WHERE  table_schema NOT IN ('information_schema','pg_catalog','main')
ORDER  BY 1;


-- -----------------------------------------------------------------------------
-- 1. Workforce snapshot — most recent 14 days
--    Source: marts.mart_workforce_metrics_daily
-- -----------------------------------------------------------------------------
-- Note: filtered to dates with data, in case `today` is past the data window.
WITH last_loaded AS (
    SELECT MAX(date_day) AS d
    FROM   marts.mart_workforce_metrics_daily
    WHERE  active_headcount > 0
)
SELECT date_day,
       active_headcount,
       total_fte,
       hires,
       rehires,
       terminations,
       net_headcount_change
FROM   marts.mart_workforce_metrics_daily, last_loaded
WHERE  date_day BETWEEN last_loaded.d - INTERVAL 13 DAY AND last_loaded.d
ORDER  BY date_day DESC;


-- -----------------------------------------------------------------------------
-- 2. Monthly hires, terms, and net change for the last 12 months
-- -----------------------------------------------------------------------------
WITH last_loaded AS (
    SELECT MAX(date_day) AS d
    FROM   marts.mart_workforce_metrics_daily
    WHERE  active_headcount > 0
)
SELECT first_day_of_month                            AS month,
       SUM(hires)                                    AS hires,
       SUM(rehires)                                  AS rehires,
       SUM(terminations)                             AS terminations,
       SUM(net_headcount_change)                     AS net_change
FROM   marts.mart_workforce_metrics_daily, last_loaded
WHERE  date_day >= last_loaded.d - INTERVAL 12 MONTH
  AND  date_day <= last_loaded.d
GROUP  BY first_day_of_month
ORDER  BY first_day_of_month;


-- -----------------------------------------------------------------------------
-- 3. Annualized attrition (TTM), by department
--    Termination *events* live in fact_employment_event (snapshot only contains
--    active employees, so it can't be used for term counts).
--    avg_headcount = sum of daily snapshots / 365.
-- -----------------------------------------------------------------------------
WITH last_loaded AS (
    SELECT MAX(snapshot_date) AS d FROM core.fact_headcount_snapshot_daily
),
terms AS (
    SELECT department, COUNT(*) AS terms_12m
    FROM   core.fact_employment_event e, last_loaded
    WHERE  e.event_type = 'termination'
      AND  e.effective_date BETWEEN last_loaded.d - INTERVAL 12 MONTH AND last_loaded.d
    GROUP  BY department
),
avg_hc AS (
    SELECT s.department,
           COUNT(*) / 365.0 AS avg_headcount_12m
    FROM   core.fact_headcount_snapshot_daily s, last_loaded
    WHERE  s.snapshot_date BETWEEN last_loaded.d - INTERVAL 12 MONTH AND last_loaded.d
    GROUP  BY s.department
)
SELECT a.department,
       COALESCE(t.terms_12m, 0)                                AS terms_12m,
       ROUND(a.avg_headcount_12m, 1)                           AS avg_headcount_12m,
       ROUND(COALESCE(t.terms_12m, 0) / NULLIF(a.avg_headcount_12m, 0), 3) AS attrition_rate
FROM   avg_hc a
LEFT JOIN terms t USING (department)
ORDER  BY attrition_rate DESC;


-- -----------------------------------------------------------------------------
-- 4. Span of control — top 20 managers by direct report count (today)
-- -----------------------------------------------------------------------------
SELECT m.first_name || ' ' || m.last_name           AS manager,
       m.current_department                         AS department,
       COUNT(*)                                     AS direct_reports
FROM   core.fact_headcount_snapshot_daily s
JOIN   core.dim_employee m
  ON   s.manager_employee_key = m.employee_key
WHERE  s.snapshot_date = (SELECT MAX(snapshot_date) FROM core.fact_headcount_snapshot_daily)
GROUP  BY m.first_name, m.last_name, m.current_department
ORDER  BY direct_reports DESC
LIMIT  20;


-- -----------------------------------------------------------------------------
-- 5. Org pyramid — count of employees at each level below the top (recursive)
-- -----------------------------------------------------------------------------
WITH RECURSIVE chain AS (
    SELECT snapshot_date,
           employee_key,
           manager_employee_key AS ancestor_employee_key,
           1                    AS depth
    FROM   core.fact_headcount_snapshot_daily
    WHERE  snapshot_date = (SELECT MAX(snapshot_date) FROM core.fact_headcount_snapshot_daily)
      AND  manager_employee_key IS NOT NULL

    UNION ALL

    SELECT c.snapshot_date,
           c.employee_key,
           h.manager_employee_key,
           c.depth + 1
    FROM   chain c
    JOIN   core.fact_headcount_snapshot_daily h
      ON   c.ancestor_employee_key = h.employee_key
     AND   c.snapshot_date         = h.snapshot_date
    WHERE  h.manager_employee_key IS NOT NULL
      AND  c.depth < 20
)
SELECT depth                          AS levels_above,
       COUNT(DISTINCT employee_key)   AS employees
FROM   chain
GROUP  BY depth
ORDER  BY depth;


-- -----------------------------------------------------------------------------
-- 6. Rehires — employees with more than one employment episode
-- -----------------------------------------------------------------------------
SELECT e.first_name || ' ' || e.last_name           AS full_name,
       COUNT(*)                                     AS episode_count,
       MIN(ep.hire_date)                            AS first_hire,
       MAX(ep.hire_date)                            AS most_recent_hire
FROM   core.dim_employment_episode ep
JOIN   core.dim_employee e
  ON   ep.employee_key = e.employee_key
GROUP  BY e.first_name, e.last_name
HAVING COUNT(*) > 1
ORDER  BY episode_count DESC, most_recent_hire DESC
LIMIT  20;


-- -----------------------------------------------------------------------------
-- 7. SCD2 in action — show all profile versions of one employee
--    Picks an employee with the most versions, so you actually see history.
-- -----------------------------------------------------------------------------
WITH most_versioned AS (
    SELECT employee_key
    FROM   core.dim_employee_profile_scd
    GROUP  BY employee_key
    ORDER  BY COUNT(*) DESC
    LIMIT  1
)
SELECT s.employee_key,
       s.version_number,
       s.version_event_type,
       s.department,
       s.job_title,
       s.job_level,
       s.manager_employee_key,
       s.valid_from_date,
       s.valid_to_date,
       s.is_current
FROM   core.dim_employee_profile_scd s
JOIN   most_versioned m USING (employee_key)
ORDER  BY s.valid_from_date;


-- -----------------------------------------------------------------------------
-- 8. As-of headcount — point-in-time using SCD2 valid_from / valid_to
--    Change the date to ask "what did the org look like on X?"
-- -----------------------------------------------------------------------------
SELECT department,
       COUNT(DISTINCT employee_key) AS headcount_on_date
FROM   core.dim_employee_profile_scd
WHERE  valid_from_date <= DATE '2024-12-31'
  AND  COALESCE(valid_to_date, DATE '2999-12-31') > DATE '2024-12-31'
GROUP  BY department
ORDER  BY headcount_on_date DESC;


-- -----------------------------------------------------------------------------
-- 9. Tenure distribution (active employees today)
--    Uses dim_employment_episode.tenure_days for the current/active episode.
-- -----------------------------------------------------------------------------
SELECT CASE
         WHEN tenure_days < 365         THEN '< 1 year'
         WHEN tenure_days < 365 * 2     THEN '1-2 years'
         WHEN tenure_days < 365 * 5     THEN '2-5 years'
         WHEN tenure_days < 365 * 10    THEN '5-10 years'
         ELSE '10+ years'
       END                                AS tenure_band,
       COUNT(*)                           AS employees
FROM   core.dim_employment_episode
WHERE  is_active_episode
GROUP  BY tenure_band
ORDER  BY MIN(tenure_days);


-- -----------------------------------------------------------------------------
-- 10. Headcount by worker type (today)
-- -----------------------------------------------------------------------------
SELECT date_day,
       full_time_headcount,
       part_time_headcount,
       contractor_headcount,
       intern_headcount,
       contingent_headcount,
       active_headcount
FROM   marts.mart_workforce_metrics_daily
WHERE  active_headcount > 0
ORDER  BY date_day DESC
LIMIT  1;


-- -----------------------------------------------------------------------------
-- 11. Audit trace — given a row in dim_employee, find the raw rows that built
--      it. Uses the lineage column `ingested_run_id` carried through every
--      base/staging/core model.
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT employee_key, worker_id, ingested_run_id
    FROM   core.dim_employee
    LIMIT  1
)
SELECT t.worker_id                         AS dim_worker_id,
       t.ingested_run_id                   AS dim_run_id,
       r._ingested_at_utc,
       r._run_id                           AS raw_run_id,
       r._payload_hash,
       r._source_updated_at,
       r.raw_payload
FROM   target t
JOIN   raw.hris_workday__workers r
  ON   r._source_record_id = t.worker_id
ORDER  BY r._ingested_at_utc;
-- The first row's `raw_run_id` should equal `dim_run_id`. Older rows show
-- prior versions of the same record from earlier extractor runs.


-- -----------------------------------------------------------------------------
-- 12. Sandbox — your queries below
-- -----------------------------------------------------------------------------
