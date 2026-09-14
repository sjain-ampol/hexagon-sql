-- =============================================================================
-- fact_work_package_status_sql.sql  |  fact_work_package_status_sql
-- Sources:
--   * ${catalog}.${silver_schema}.work_package_details_sql
--   * ${catalog}.${silver_schema}.work_packages_sql
--   * LIVE.fact_task_step_sql
--   * LIVE.fact_workflow_event_sql
-- Target: ${catalog}.${gold_schema}.fact_work_package_status_sql
-- Type:   MATERIALIZED VIEW
-- Notes:  Accumulating snapshot — one row per work package.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW fact_work_package_status_sql
COMMENT 'Accumulating snapshot — one row per work package with step rollups, milestones and 5A/5B keys.'
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY (project, wp_id)
AS
WITH work_package AS (
  SELECT *
  FROM ${catalog}.${silver_schema}.work_package_details_sql
  WHERE active_flag = true
    AND dt_removed IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY instance, project, wp_id
    ORDER BY record_last_modified DESC NULLS LAST, dh_eff_dt DESC NULLS LAST
  ) = 1
),
packages AS (
  SELECT *
  FROM ${catalog}.${silver_schema}.work_packages_sql
  WHERE active_flag = true
    AND dt_removed IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY instance, project, wp_id
    ORDER BY record_last_modified DESC NULLS LAST, dh_eff_dt DESC NULLS LAST
  ) = 1
),
step_status AS (
  SELECT
    instance,
    project,
    wp_id,
    COUNT_IF(is_in_scope_step) AS in_scope_step_count,
    COUNT_IF(is_in_scope_step AND is_complete_step) AS completed_step_count,
    COUNT_IF(is_in_scope_step AND NOT is_complete_step) AS incomplete_step_count,
    COUNT_IF(is_punchlist) AS punchlist_step_count,
    MAX(completed_date) AS last_step_completed_at
  FROM LIVE.fact_task_step_sql
  GROUP BY instance, project, wp_id
),
workflow AS (
  SELECT
    instance,
    project,
    wp_id,
    MIN(state_date) FILTER (WHERE workflow_state = 'Originated') AS originated_at,
    MIN(resource_state_date) FILTER (WHERE workflow_state = 'Submitted') AS inspection_reviewed_at,
    MIN(resource_state_date) FILTER (WHERE workflow_state = 'Checked') AS ops_reviewed_at,
    MIN(resource_state_date) FILTER (WHERE workflow_state = 'Approved') AS ta_reviewed_at,
    MIN(coalesce(resource_state_date, state_date)) FILTER (WHERE workflow_state = 'Issued') AS issued_at,
    MIN(resource_state_date) FILTER (WHERE workflow_state = 'Completed') AS inspection_walkdown_at,
    MIN(resource_state_date) FILTER (WHERE workflow_state = 'Verified') AS ops_walkdown_at,
    MIN(resource_state_date) FILTER (WHERE workflow_state = 'Accepted') AS ta_walkdown_at,
    MIN(coalesce(resource_state_date, state_date)) FILTER (WHERE workflow_state = 'Closed') AS completed_at,
    MAX(source_effective_at) AS workflow_effective_at
  FROM LIVE.fact_workflow_event_sql
  GROUP BY instance, project, wp_id
)
SELECT
  concat_ws('|', CAST(wp.instance AS STRING), CAST(wp.project AS STRING)) AS project_key,
  concat_ws('|', CAST(wp.instance AS STRING), CAST(wp.project AS STRING), CAST(wp.wp_id AS STRING)) AS work_package_key,
  CASE
    WHEN upper(trim(wp.current_workflow_state)) = 'ISSUED'
     AND coalesce(ss.in_scope_step_count, 0) > 0
     AND coalesce(ss.incomplete_step_count, 0) = 0 THEN 'ISSUED_5B'
    WHEN upper(trim(wp.current_workflow_state)) = 'ISSUED' THEN 'ISSUED_5A'
    WHEN upper(trim(wp.current_workflow_state)) IN (
      'ORIGINATED', 'SUBMITTED', 'CHECKED', 'APPROVED',
      'COMPLETED', 'VERIFIED', 'ACCEPTED', 'CLOSED'
    ) THEN upper(trim(wp.current_workflow_state))
    WHEN upper(trim(wp.current_workflow_state)) = 'ISSUED TO FIELD' THEN 'ISSUED_TO_FIELD'
    ELSE '_UNKNOWN'
  END AS workflow_stage_key,
  CAST(date_format(wp.record_created, 'yyyyMMdd') AS INT) AS record_created_date_key,
  wp.instance,
  wp.project,
  wp.wp_id,
  wp.wp_name,
  wp.wp_description,
  wp.wp_summary,
  wp.workflow_id,
  wp.current_workflow_state,
  CASE
    WHEN upper(trim(wp.current_workflow_state)) = 'ISSUED'
     AND coalesce(ss.in_scope_step_count, 0) > 0
     AND coalesce(ss.incomplete_step_count, 0) = 0 THEN true
    ELSE false
  END AS is_at_5b,
  CASE
    WHEN upper(trim(wp.current_workflow_state)) = 'ISSUED'
     AND NOT (
       coalesce(ss.in_scope_step_count, 0) > 0
       AND coalesce(ss.incomplete_step_count, 0) = 0
     ) THEN true
    ELSE false
  END AS is_at_5a,
  coalesce(p.work_package_type, wp.work_package_type) AS work_package_type,
  coalesce(p.work_package_class, wp.work_package_class) AS work_package_class,
  wp.job_category,
  wp.responsible_company,
  wp.supplier_company,
  wp.wp_scheduled_by,
  wp.wp_discipline,
  wp.wp_priority,
  wp.process_breakdown,
  wp.process_breakdown_up1_summary,
  wp.process_breakdown_up2_summary,
  wp.process_breakdown_up3_summary,
  wp.work_breakdown,
  wp.physical_location,
  wp.physical_location_up3_summary AS physical_location_summary,
  wp.schedule_start_date,
  wp.schedule_end_date,
  wp.actual_start_date,
  CAST(wp.record_created AS DATE) AS record_created_date,
  coalesce(ss.in_scope_step_count, 0) AS in_scope_step_count,
  coalesce(ss.completed_step_count, 0) AS completed_step_count,
  coalesce(ss.incomplete_step_count, 0) AS incomplete_step_count,
  coalesce(ss.punchlist_step_count, 0) AS punchlist_step_count,
  ss.last_step_completed_at,
  wf.originated_at,
  wf.inspection_reviewed_at,
  wf.ops_reviewed_at,
  wf.ta_reviewed_at,
  wf.issued_at,
  wf.inspection_walkdown_at,
  wf.ops_walkdown_at,
  wf.ta_walkdown_at,
  wf.completed_at,
  wp.schedule_end_date IS NOT NULL
    AND wp.schedule_end_date < current_timestamp()
    AND upper(trim(wp.current_workflow_state)) NOT IN ('CLOSED') AS is_overdue,
  CAST(datediff(current_date(), CAST(coalesce(wp.record_last_modified, wp.record_created) AS DATE)) AS DOUBLE) AS days_in_current_stage,
  CAST(datediff(wf.issued_at, wf.originated_at) AS DOUBLE) AS cycle_time_days_originate_to_issue,
  CAST(datediff(wf.completed_at, wf.issued_at) AS DOUBLE) AS cycle_time_days_issue_to_complete,
  CAST(datediff(wf.completed_at, wf.originated_at) AS DOUBLE) AS cycle_time_days_total,
  wp.budget_mh,
  wp.actual_mh,
  greatest(
    coalesce(wp.dh_eff_dt, wp.record_last_modified, timestamp('1970-01-01')),
    coalesce(ss.last_step_completed_at, timestamp('1970-01-01')),
    coalesce(wf.workflow_effective_at, timestamp('1970-01-01'))
  ) AS data_effective_at
FROM work_package wp
LEFT JOIN packages p
  ON p.instance = wp.instance AND p.project = wp.project AND p.wp_id = wp.wp_id
LEFT JOIN step_status ss
  ON ss.instance = wp.instance AND ss.project = wp.project AND ss.wp_id = wp.wp_id
LEFT JOIN workflow wf
  ON wf.instance = wp.instance AND wf.project = wp.project AND wf.wp_id = wp.wp_id;
