-- =============================================================================
-- fact_task_step_sql.sql  |  fact_task_step_sql
-- Sources:
--   * ${catalog}.${silver_schema}.task_step_instance_details_sql
--   * ${catalog}.${silver_schema}.tasks_tests_planned_details_sql
--   * ${catalog}.${silver_schema}.assets_sql
--   * ${catalog}.${silver_schema}.work_packages_steps_sql
--   * ${catalog}.${silver_schema}.work_packages_sql
-- Target: ${catalog}.${gold_schema}.fact_task_step_sql
-- Type:   MATERIALIZED VIEW
-- Notes:  One row per task-step instance with denormalised task, asset and WP context.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW fact_task_step_sql
COMMENT 'One row per task-step instance with denormalised task, asset and WP context.'
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY (project, task_id)
AS
WITH task_step AS (
  SELECT *
  FROM ${catalog}.${silver_schema}.task_step_instance_details_sql
  WHERE active_flag = true
    AND dt_removed IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY instance, project, id
    ORDER BY record_last_modified DESC NULLS LAST, dh_eff_dt DESC NULLS LAST
  ) = 1
),
planned_task AS (
  SELECT *
  FROM ${catalog}.${silver_schema}.tasks_tests_planned_details_sql
  WHERE active_flag = true
    AND dt_removed IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY instance, project, task_id
    ORDER BY record_last_modified DESC NULLS LAST, dh_eff_dt DESC NULLS LAST
  ) = 1
),
asset AS (
  SELECT *
  FROM ${catalog}.${silver_schema}.assets_sql
  WHERE active_flag = true
    AND dt_removed IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY instance, project, asset_id
    ORDER BY record_last_modified DESC NULLS LAST, dh_eff_dt DESC NULLS LAST
  ) = 1
),
task_wp_raw AS (
  SELECT *
  FROM ${catalog}.${silver_schema}.work_packages_steps_sql
  WHERE active_flag = true
    AND dt_removed IS NULL
),
task_wp_ambiguity AS (
  SELECT
    instance,
    project,
    task_id,
    COUNT(DISTINCT wp_id) > 1 AS is_ambiguous_assignment
  FROM task_wp_raw
  GROUP BY instance, project, task_id
),
task_wp AS (
  SELECT
    r.*,
    a.is_ambiguous_assignment
  FROM task_wp_raw r
  INNER JOIN task_wp_ambiguity a
    ON a.instance = r.instance
   AND a.project = r.project
   AND a.task_id = r.task_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY r.instance, r.project, r.task_id
    ORDER BY r.record_last_modified DESC NULLS LAST, r.dh_eff_dt DESC NULLS LAST, r.wp_id
  ) = 1
),
work_package AS (
  SELECT *
  FROM ${catalog}.${silver_schema}.work_packages_sql
  WHERE active_flag = true
    AND dt_removed IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY instance, project, wp_id
    ORDER BY record_last_modified DESC NULLS LAST, dh_eff_dt DESC NULLS LAST
  ) = 1
)
SELECT
  concat_ws('|', CAST(ts.instance AS STRING), CAST(ts.project AS STRING)) AS project_key,
  concat_ws('|', CAST(ts.instance AS STRING), CAST(ts.project AS STRING), CAST(tw.wp_id AS STRING)) AS work_package_key,
  concat_ws('|', CAST(ts.instance AS STRING), CAST(ts.project AS STRING), CAST(ts.id AS STRING)) AS task_step_key,
  concat_ws('|', CAST(ts.instance AS STRING), CAST(ts.project AS STRING), CAST(ts.task_id AS STRING)) AS task_key,
  concat_ws('|', CAST(pt.instance AS STRING), CAST(pt.project AS STRING), CAST(pt.asset_id AS STRING)) AS asset_key,
  ts.instance,
  ts.project,
  ts.id AS task_step_instance_id,
  ts.task_step_id,
  ts.task_id,
  tw.wp_id,
  tw.is_ambiguous_assignment,
  coalesce(wp.wp_name, tw.wp_name) AS wp_name,
  coalesce(wp.wp_description, tw.wp_name) AS wp_description,
  wp.wp_summary,
  coalesce(wp.wp_scheduled_by, tw.completed_by) AS wp_scheduled_by,
  wp.responsible_company,
  wp.supplier_company,
  coalesce(wp.process_breakdown, pt.process_breakdown) AS process_breakdown,
  coalesce(wp.process_breakdown_up2_summary, pt.process_breakdown_up2_summary) AS process_breakdown_up2_summary,
  wp.work_breakdown_up3_summary,
  ts.task_name,
  pt.task_description,
  pt.task_model_name,
  pt.task_type,
  pt.task_discipline,
  pt.task_state,
  pt.asset_id,
  coalesce(a.asset_tag, pt.asset_tag, ts.asset_tag) AS asset_tag,
  coalesce(a.asset_tag, pt.asset_tag, ts.asset_tag) AS equipment_no,
  a.asset_description,
  a.asset_type,
  CASE
    WHEN upper(trim(a.asset_type)) = 'PSV' THEN 'PSV'
    WHEN upper(trim(a.asset_type)) = 'CV' THEN 'CONTROL_VALVE'
    WHEN upper(trim(a.asset_type)) = 'COL' THEN 'COLUMN'
    WHEN upper(trim(a.asset_type)) = 'EXCH' THEN 'EXCHANGER'
    WHEN upper(coalesce(a.asset_type, a.asset_type_summary, a.asset_description, '')) RLIKE '\\bPSV\\b|PRESSURE.?SAFETY|RELIEF VALVE' THEN 'PSV'
    WHEN upper(coalesce(a.asset_type, a.asset_type_summary, a.asset_description, '')) RLIKE '\\bCV\\b|CONTROL.?VALVE' THEN 'CONTROL_VALVE'
    WHEN upper(coalesce(a.asset_type, a.asset_type_summary, a.asset_description, '')) RLIKE '\\bCOL\\b|COLUMN|TOWER' THEN 'COLUMN'
    WHEN upper(coalesce(a.asset_type, a.asset_type_summary, a.asset_description, '')) RLIKE '\\bEXCH\\b|EXCHANGER|HEAT.?EX' THEN 'EXCHANGER'
    ELSE 'UNKNOWN'
  END AS asset_class,
  a.sap_functional_location,
  ts.step_sequence,
  ts.order_sequence,
  ts.step_action,
  ts.step_answer,
  ts.insp_answer,
  ts.inspection_type,
  ts.is_step_required,
  ts.is_na,
  coalesce(ts.is_punchlist, false) AS is_punchlist,
  ts.punchlist_item,
  ts.completed_date,
  ts.completed_by,
  ts.pct_weight,
  CASE WHEN ts.completed_date IS NOT NULL THEN coalesce(ts.pct_weight, 0) ELSE 0 END AS weighted_progress,
  CAST(datediff(ts.completed_date, ts.record_created) AS DOUBLE) AS days_to_complete,
  ts.parameter_reading,
  ts.parameter_reading1,
  ts.parameter_reading2,
  ts.parameter_reading3,
  ts.parameter_reading4,
  ts.parameter_reading5,
  ts.parameter_reading6,
  ts.parameter_reading7,
  ts.parameter_reading8,
  ts.parameter_reading9,
  ts.parameter_reading10,
  ts.parameter_reading11,
  ts.parameter_reading12,
  ts.parameter_reading13,
  ts.parameter_reading14,
  ts.parameter_reading15,
  pt.task_state IS NOT NULL AND upper(trim(pt.task_state)) <> 'CREATED' AS is_in_scope_step,
  ts.completed_date IS NOT NULL AS is_complete_step,
  coalesce(ts.is_step_required, false) = true AND ts.completed_date IS NULL AS is_incomplete_required_step,
  ts.inspection_type IS NOT NULL AS is_itp_step,
  CASE
    WHEN upper(trim(coalesce(ts.inspection_type, ''))) <> 'CLIENT HOLD POINT' THEN NULL
    WHEN coalesce(trim(ts.step_action), '') = '' THEN 'Miscellaneous'
    WHEN contains(upper(ts.step_action), 'E&I') OR contains(upper(ts.step_action), 'ELECTRICIAN') THEN 'E&I'
    WHEN contains(upper(ts.step_action), 'ROTATING') THEN 'Rotating'
    WHEN contains(upper(ts.step_action), 'NDT')
      OR contains(upper(ts.step_action), 'INSPECTION')
      OR contains(upper(ts.step_action), 'INSPECTIONS')
      OR contains(upper(ts.step_action), 'REFRACTORY') THEN 'Inspection / NDT'
    WHEN contains(upper(ts.step_action), 'RELIABILITY') OR contains(upper(ts.step_action), 'RELIABILTY') THEN 'Reliability'
    WHEN contains(upper(ts.step_action), 'PROCESS ENGINEER') OR contains(upper(ts.step_action), 'TSD') THEN 'Process / TSD'
    WHEN contains(upper(ts.step_action), 'MECHANICAL')
      OR contains(upper(ts.step_action), 'TRADESMEN')
      OR contains(upper(ts.step_action), 'BLAC SME') THEN 'Mechanical'
    WHEN contains(upper(ts.step_action), 'PROJECT MANAGER')
      OR contains(upper(ts.step_action), 'PROJECT ENGINEERING')
      OR contains(upper(ts.step_action), 'EXECUTION COORDINATOR')
      OR contains(upper(ts.step_action), 'T&I CO-ORDINATOR')
      OR contains(upper(ts.step_action), 'CO-ORDINATOR')
      OR contains(upper(ts.step_action), 'COORDINATOR') THEN 'Coordination / Project'
    WHEN contains(upper(ts.step_action), 'QA') THEN 'QA'
    WHEN contains(upper(ts.step_action), 'SME')
      OR contains(upper(ts.step_action), 'SUBJECT MATTER EXPERT')
      OR contains(upper(ts.step_action), 'ENGINEERING TEAM')
      OR contains(upper(ts.step_action), 'ENGINEERING') THEN 'Engineering / SME'
    WHEN contains(upper(ts.step_action), 'OPERATIONS') THEN 'Operations'
    WHEN contains(upper(ts.step_action), 'SUPERVISOR') THEN 'Supervisor'
    WHEN contains(upper(ts.step_action), 'FSE') THEN 'FSE'
    ELSE 'Miscellaneous'
  END AS step_action_category_client_hold_point,
  CASE
    WHEN ts.completed_by IS NOT NULL
     AND length(trim(ts.completed_by)) > 0
     AND ts.step_action IS NOT NULL
     AND contains(upper(ts.step_action), 'DESCRIPTION: WORK COMPLETE AND READY FOR HAND OVER')
    THEN 'WP Complete'
    ELSE NULL
  END AS wp_complete_flag,
  CASE
    WHEN upper(trim(coalesce(ts.insp_answer, ''))) IN ('A', 'B', 'C') THEN trim(ts.insp_answer)
    ELSE NULL
  END AS insp_answer_filter,
  CASE
    WHEN pt.task_description IS NULL THEN NULL
    WHEN contains(upper(pt.task_description), 'PUNCHLIST') THEN 'PUNCHLIST'
    WHEN contains(upper(pt.task_description), 'OFFSITES') THEN 'OFFSITES'
    WHEN contains(upper(pt.task_description), 'ITP') THEN 'ITP'
    ELSE 'OTHERS'
  END AS task_description_category,
  CAST(floor(ts.step_sequence) AS BIGINT) AS punch_group,
  CAST(date_format(ts.completed_date, 'yyyyMMdd') AS INT) AS completed_date_key,
  ts.record_created AS source_created_at,
  ts.record_last_modified AS source_modified_at,
  greatest(
    coalesce(ts.dh_eff_dt, ts.record_last_modified),
    coalesce(pt.dh_eff_dt, ts.dh_eff_dt),
    coalesce(a.dh_eff_dt, ts.dh_eff_dt),
    coalesce(tw.dh_eff_dt, ts.dh_eff_dt),
    coalesce(wp.dh_eff_dt, ts.dh_eff_dt)
  ) AS source_effective_at
FROM task_step ts
LEFT JOIN planned_task pt
  ON pt.instance = ts.instance
 AND pt.project = ts.project
 AND pt.task_id = ts.task_id
LEFT JOIN asset a
  ON a.instance = pt.instance
 AND a.project = pt.project
 AND a.asset_id = pt.asset_id
LEFT JOIN task_wp tw
  ON tw.instance = ts.instance
 AND tw.project = ts.project
 AND tw.task_id = ts.task_id
LEFT JOIN work_package wp
  ON wp.instance = tw.instance
 AND wp.project = tw.project
 AND wp.wp_id = tw.wp_id;
