-- =============================================================================
-- fact_workflow_event_sql.sql  |  fact_workflow_event_sql
-- Sources:
--   * ${catalog}.${silver_schema}.work_package_workflow_lists_sql
--   * ${catalog}.${silver_schema}.work_package_details_sql
-- Target: ${catalog}.${gold_schema}.fact_workflow_event_sql
-- Type:   MATERIALIZED VIEW
-- Notes:  One row per workflow state/resource action.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW fact_workflow_event_sql
COMMENT 'One row per workflow state/resource action. WP resolved via WORK_PACKAGE_DETAILS_ID → details.ID → wp_id.'
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY (project, state_date)
AS
WITH source_rows AS (
  SELECT
    w.id,
    w.instance,
    d.project,
    w.work_package_details_id,
    w.workflow_id,
    d.wp_id,
    d.wp_name,
    d.wp_discipline,
    w.state_status_name,
    w.state_sequence,
    w.state_date,
    w.resource_summary,
    w.resource_state_date,
    w.resource_type_id,
    w.resource_state_sequence,
    w.state_comment,
    w.state_revision,
    w.dh_eff_dt,
    lag(w.state_date) OVER (
      PARTITION BY w.instance, d.project, d.wp_id
      ORDER BY w.state_sequence, w.state_date, w.id
    ) AS previous_state_date
  FROM ${catalog}.${silver_schema}.work_package_workflow_lists_sql w
  INNER JOIN ${catalog}.${silver_schema}.work_package_details_sql d
    ON w.instance = d.instance
   AND w.work_package_details_id = d.id
   AND d.active_flag = true
   AND d.dt_removed IS NULL
  WHERE w.active_flag = true
    AND NOT coalesce(w.is_inactive, false)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY w.instance, d.project, w.id
    ORDER BY w.dh_eff_dt DESC NULLS LAST, w.resource_state_date DESC NULLS LAST, w.state_date DESC NULLS LAST
  ) = 1
)
SELECT
  concat_ws('|', CAST(instance AS STRING), CAST(project AS STRING)) AS project_key,
  concat_ws('|', CAST(instance AS STRING), CAST(project AS STRING), CAST(wp_id AS STRING)) AS work_package_key,
  concat_ws('|', CAST(instance AS STRING), CAST(project AS STRING), CAST(id AS STRING)) AS workflow_event_key,
  CASE
    WHEN upper(trim(state_status_name)) IN (
      'ORIGINATED', 'SUBMITTED', 'CHECKED', 'APPROVED', 'ISSUED',
      'ISSUED TO FIELD', 'COMPLETED', 'VERIFIED', 'ACCEPTED', 'CLOSED'
    ) THEN
      CASE upper(trim(state_status_name))
        WHEN 'ISSUED TO FIELD' THEN 'ISSUED_TO_FIELD'
        ELSE upper(trim(state_status_name))
      END
    ELSE '_UNKNOWN'
  END AS workflow_stage_key,
  CAST(date_format(state_date, 'yyyyMMdd') AS INT) AS state_date_key,
  instance,
  project,
  id AS workflow_event_id,
  work_package_details_id,
  workflow_id,
  wp_id,
  wp_name,
  wp_discipline,
  state_status_name AS workflow_state,
  state_sequence,
  state_date,
  resource_summary,
  resource_state_date,
  resource_type_id,
  resource_state_sequence,
  state_comment,
  state_revision,
  resource_summary IS NOT NULL AS is_signed,
  resource_state_date IS NOT NULL AS is_actioned,
  CAST(timestampdiff(HOUR, previous_state_date, state_date) AS DOUBLE) AS hours_since_previous_state,
  dh_eff_dt AS source_effective_at
FROM source_rows;
