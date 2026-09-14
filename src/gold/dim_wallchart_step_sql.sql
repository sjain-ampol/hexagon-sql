-- =============================================================================
-- dim_wallchart_step_sql.sql  |  dim_wallchart_step_sql
-- Source: ${catalog}.${ref_schema}.ref_wallchart_rule_sql
-- Target: ${catalog}.${gold_schema}.dim_wallchart_step_sql
-- Type:   MATERIALIZED VIEW
-- Notes:  Canonical wallchart display steps by asset class.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_wallchart_step_sql
COMMENT 'Canonical wallchart steps by asset class, derived from business-owned reference rules.'
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY (asset_class)
AS
SELECT
  concat_ws('|', asset_class, canonical_step_code) AS wallchart_step_key,
  asset_class,
  canonical_step_code,
  max(canonical_step_label) AS canonical_step_label,
  max(display_label) AS display_label,
  CAST(max(step_sequence) AS DECIMAL(10, 2)) AS step_sequence,
  max(answer_type) AS answer_type,
  max(CAST(is_special_step AS INT)) = 1 AS is_special_step
FROM ${catalog}.${ref_schema}.ref_wallchart_rule_sql
WHERE is_active = true
  AND step_sequence IS NOT NULL
GROUP BY asset_class, canonical_step_code;
