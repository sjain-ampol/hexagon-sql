-- =============================================================================
-- dim_project_sql.sql  |  dim_project_sql
-- Source: ${catalog}.${silver_schema}.projects_sql
-- Target: ${catalog}.${gold_schema}.dim_project_sql
-- Type:   MATERIALIZED VIEW
-- Notes:  Conformed project dimension — UC RLS anchor.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_project_sql
COMMENT 'Conformed project dimension — UC RLS anchor.'
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY (project_id)
AS
SELECT
  instance AS instance,
  id AS project_id,
  concat_ws('|', CAST(instance AS STRING), CAST(id AS STRING)) AS project_key,
  project_identifier AS project_identifier,
  project_name AS project_name,
  summary AS project_summary,
  active_flag AS is_active
FROM ${catalog}.${silver_schema}.projects_sql
WHERE active_flag = true;
