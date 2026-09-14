-- =============================================================================
-- fact_wallchart_cell_sql.sql  |  fact_wallchart_cell_sql
-- Sources:
--   * LIVE.fact_task_step_sql
--   * ${catalog}.${ref_schema}.ref_wallchart_rule_sql
--   * ${catalog}.${ref_schema}.ref_wallchart_tm_inclusion_sql
-- Target: ${catalog}.${gold_schema}.fact_wallchart_cell_sql
-- Type:   MATERIALIZED VIEW
-- Notes:  Wallchart status at WP × equipment × canonical step.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW fact_wallchart_cell_sql
COMMENT 'Wallchart status at WP × equipment × canonical step. Status codes only — no emoji or HTML.'
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY (project, wp_id)
AS
WITH rule AS (
  SELECT asset_class, canonical_step_code, match_group_id, match_substring
  FROM ${catalog}.${ref_schema}.ref_wallchart_rule_sql
  WHERE is_active = true
),
grp AS (
  SELECT asset_class, canonical_step_code, match_group_id, COUNT(*) AS substr_count
  FROM rule
  GROUP BY asset_class, canonical_step_code, match_group_id
),
step_meta AS (
  SELECT
    asset_class,
    canonical_step_code,
    max(canonical_step_label) AS canonical_step_label,
    max(display_label) AS display_label,
    max(step_sequence) AS step_sequence,
    max(answer_type) AS answer_type,
    max(CAST(is_special_step AS INT)) = 1 AS is_special_step
  FROM ${catalog}.${ref_schema}.ref_wallchart_rule_sql
  WHERE is_active = true
  GROUP BY asset_class, canonical_step_code
),
tm_inclusion AS (
  SELECT project, asset_class, task_model_name
  FROM ${catalog}.${ref_schema}.ref_wallchart_tm_inclusion_sql
  WHERE is_active = true
    AND current_date() >= effective_from
    AND (effective_to IS NULL OR current_date() <= effective_to)
    AND task_model_name IS NOT NULL
    AND length(trim(task_model_name)) > 0
    AND upper(trim(task_model_name)) NOT IN ('NA', 'TM-00')
),
task_step AS (
  SELECT ts.*
  FROM LIVE.fact_task_step_sql ts
  INNER JOIN tm_inclusion i
    ON i.project = ts.project
   AND i.asset_class = CASE ts.asset_class
         WHEN 'CONTROL_VALVE' THEN 'CV'
         WHEN 'COLUMN' THEN 'COL'
         WHEN 'EXCHANGER' THEN 'EXCH'
         ELSE ts.asset_class
       END
   AND upper(trim(i.task_model_name)) = upper(trim(ts.task_model_name))
  WHERE ts.asset_class IN ('PSV', 'CONTROL_VALVE', 'COLUMN', 'EXCHANGER')
),
substr_hit AS (
  SELECT
    ts.instance,
    ts.project,
    ts.task_step_instance_id,
    r.asset_class,
    r.canonical_step_code,
    r.match_group_id,
    r.match_substring
  FROM task_step ts
  INNER JOIN rule r
    ON r.asset_class = ts.asset_class
   AND contains(upper(coalesce(ts.step_action, '')), upper(r.match_substring))
),
group_matched AS (
  SELECT
    h.instance,
    h.project,
    h.task_step_instance_id,
    h.asset_class,
    h.canonical_step_code
  FROM substr_hit h
  INNER JOIN grp g
    ON g.asset_class = h.asset_class
   AND g.canonical_step_code = h.canonical_step_code
   AND g.match_group_id = h.match_group_id
  GROUP BY
    h.instance, h.project, h.task_step_instance_id,
    h.asset_class, h.canonical_step_code, h.match_group_id, g.substr_count
  HAVING COUNT(DISTINCT h.match_substring) = g.substr_count
),
step_matched AS (
  SELECT DISTINCT instance, project, task_step_instance_id, asset_class, canonical_step_code
  FROM group_matched
),
candidate_matches AS (
  SELECT
    ts.instance,
    ts.project,
    ts.wp_id,
    ts.wp_name,
    ts.asset_class,
    ts.equipment_no,
    ts.asset_id,
    ts.task_id,
    ts.task_model_name,
    ts.step_answer,
    ts.completed_date,
    ts.is_complete_step,
    ts.source_effective_at,
    concat_ws('|', ts.asset_class, sm.canonical_step_code) AS wallchart_step_key,
    sm.canonical_step_code,
    sm.canonical_step_label,
    sm.display_label,
    sm.step_sequence AS wallchart_step_sequence,
    sm.answer_type,
    sm.is_special_step,
    COUNT(*) OVER (PARTITION BY ts.instance, ts.project, ts.task_step_instance_id) AS match_count
  FROM task_step ts
  INNER JOIN step_matched sma
    ON sma.instance = ts.instance
   AND sma.project = ts.project
   AND sma.task_step_instance_id = ts.task_step_instance_id
   AND sma.asset_class = ts.asset_class
  INNER JOIN step_meta sm
    ON sm.asset_class = sma.asset_class
   AND sm.canonical_step_code = sma.canonical_step_code
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ts.instance, ts.project, ts.task_step_instance_id
    ORDER BY sm.step_sequence, sm.canonical_step_code
  ) = 1
),
cell AS (
  SELECT
    instance,
    project,
    wp_id,
    max(wp_name) AS wp_name,
    asset_class,
    max(equipment_no) AS equipment_no,
    asset_id,
    max(task_id) AS task_id,
    max(task_model_name) AS task_model_name,
    wallchart_step_key,
    canonical_step_code,
    max(canonical_step_label) AS canonical_step_label,
    max(display_label) AS display_label,
    max(wallchart_step_sequence) AS wallchart_step_sequence,
    max(answer_type) AS answer_type,
    max(CAST(is_special_step AS INT)) = 1 AS is_special_step,
    MAX(match_count) AS source_match_count,
    COUNT(*) AS source_step_count,
    MAX(step_answer) AS answer_value,
    MAX(completed_date) AS answer_date,
    COUNT_IF(is_complete_step) AS complete_source_steps,
    COUNT_IF(NOT is_complete_step) AS incomplete_source_steps,
    MAX(source_effective_at) AS data_effective_at
  FROM candidate_matches
  GROUP BY instance, project, wp_id, asset_id, asset_class, wallchart_step_key, canonical_step_code
)
SELECT
  concat_ws('|', CAST(instance AS STRING), CAST(project AS STRING), CAST(wp_id AS STRING), CAST(asset_id AS STRING), canonical_step_code) AS wallchart_cell_key,
  wallchart_step_key,
  concat_ws('|', CAST(instance AS STRING), CAST(project AS STRING), CAST(wp_id AS STRING)) AS work_package_key,
  concat_ws('|', CAST(instance AS STRING), CAST(project AS STRING)) AS project_key,
  concat_ws('|', CAST(instance AS STRING), CAST(project AS STRING), CAST(asset_id AS STRING)) AS asset_key,
  instance,
  project,
  wp_id,
  wp_name,
  asset_id,
  equipment_no,
  asset_class,
  task_id,
  task_model_name,
  canonical_step_code,
  canonical_step_label,
  display_label,
  wallchart_step_sequence,
  answer_type,
  is_special_step,
  source_match_count,
  source_step_count,
  answer_value,
  answer_date,
  complete_source_steps,
  incomplete_source_steps,
  CASE
    WHEN is_special_step AND coalesce(answer_value, '') = '' THEN 'NOT_APPLICABLE'
    WHEN answer_type = 'DATE' AND answer_value = '<To be updated>' THEN 'UPDATE_REQUIRED'
    WHEN answer_type = 'EWR_STATUS' AND upper(answer_value) = 'COMPLETED' THEN 'COMPLETE'
    WHEN answer_type = 'EWR_STATUS' AND upper(answer_value) = 'INCOMPLETE' THEN 'INCOMPLETE'
    WHEN answer_type = 'EWR_STATUS' THEN 'NOT_APPLICABLE'
    WHEN incomplete_source_steps > 0 THEN 'INCOMPLETE'
    ELSE 'COMPLETE'
  END AS wallchart_status_code,
  CASE
    WHEN answer_type = 'DATE' THEN coalesce(answer_value, CAST(answer_date AS STRING))
    WHEN answer_type IN ('TEXT', 'EWR_STATUS') THEN answer_value
    ELSE NULL
  END AS wallchart_display_value,
  CAST(date_format(answer_date, 'yyyyMMdd') AS INT) AS answer_date_key,
  data_effective_at
FROM cell;
