-- =============================================================================
-- dim_workflow_stage_sql.sql  |  dim_workflow_stage_sql
-- Source: VALUES clause (governed workflow stage mapping)
-- Target: ${catalog}.${gold_schema}.dim_workflow_stage_sql
-- Type:   MATERIALIZED VIEW
-- Notes:  Includes Issued 5A / 5B snapshot keys.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_workflow_stage_sql
COMMENT 'Governed Hexagon state → dashboard stage labels including Issued 5A/5B.'
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY (workflow_stage_key)
AS
SELECT
  workflow_stage_key,
  source_state,
  CAST(stage_order AS DECIMAL(4, 1)) AS stage_order,
  stage_label,
  stage_group,
  is_5a,
  is_5b,
  responsible_role
FROM VALUES
  ('ORIGINATED',       'Originated',       1.0,  'QA Prep.',                  'Preparation', false, false, 'QA'),
  ('SUBMITTED',        'Submitted',        2.0,  'Inspection Review',         'Review',      false, false, 'Inspection'),
  ('CHECKED',          'Checked',          3.0,  'Ops Review',                'Review',      false, false, 'Operations'),
  ('APPROVED',         'Approved',         4.0,  'TA Coordinator Review',     'Review',      false, false, 'TA Coordinator'),
  ('ISSUED',           'Issued',           5.0,  'Issued',                    'Execution',   false, false, 'Supervisor'),
  ('ISSUED_5A',        'Issued',           5.1,  '5A - Issued to Supervisor', 'Execution',   true,  false, 'Supervisor'),
  ('ISSUED_5B',        'Issued',           5.2,  '5B - QA Walkdown',          'Execution',   false, true,  'QA'),
  ('ISSUED_TO_FIELD',  'Issued to Field',  7.0,  'Issued to Field',           'Execution',   false, false, 'Field'),
  ('COMPLETED',        'Completed',        8.0,  'Inspection Walkdown',       'Walkdown',    false, false, 'Inspection'),
  ('VERIFIED',         'Verified',         9.0,  'Ops Walkdown',              'Walkdown',    false, false, 'Operations'),
  ('ACCEPTED',         'Accepted',        10.0,  'TA Coordinator Walkdown',   'Walkdown',    false, false, 'TA Coordinator'),
  ('CLOSED',           'Closed',          11.0,  'WP Complete',               'Complete',    false, false, 'QA'),
  ('_UNKNOWN',         'Unknown',         99.0,  'Unknown',                   'Unknown',     false, false, 'Unknown')
AS t(
  workflow_stage_key,
  source_state,
  stage_order,
  stage_label,
  stage_group,
  is_5a,
  is_5b,
  responsible_role
);
