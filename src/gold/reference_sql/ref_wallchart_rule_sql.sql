-- Wallchart matching rules migrated from PBIT Power Query keyword tables.
-- Source: {PSV,ControlValves,Columns,Exchangers}_Keywords_StepAndAction (M literals).
-- Match model: AND within match_group_id (all substrings contained), OR across groups.
-- Substring 'contains' matching (NOT step_sequence). Managed table, edited outside pipeline.
-- NOTE: the create_reference_tables notebook performs the idempotent reseed by
-- applying the source-of-truth rule body and renaming the target to *_sql.
CREATE SCHEMA IF NOT EXISTS {catalog}.{ref_schema};

CREATE TABLE IF NOT EXISTS {catalog}.{ref_schema}.ref_wallchart_rule_sql (
  rule_id STRING NOT NULL,
  asset_class STRING NOT NULL COMMENT 'PSV, CONTROL_VALVE, COLUMN or EXCHANGER',
  canonical_step_code STRING NOT NULL,
  canonical_step_label STRING,
  display_label STRING,
  step_sequence DECIMAL(10,2),
  match_group_id INT NOT NULL COMMENT 'AND within group; OR across groups',
  match_substring STRING NOT NULL COMMENT 'Required substring; matched via contains(step_action)',
  step_label_phrase STRING COMMENT 'Secondary Perform:/HOLD: predicate from PBIT (stored, not yet applied)',
  answer_type STRING NOT NULL COMMENT 'STATUS, DATE or EWR_STATUS',
  is_special_step BOOLEAN NOT NULL COMMENT 'Date-collector helper row',
  effective_from DATE NOT NULL,
  effective_to DATE,
  is_active BOOLEAN NOT NULL
)
USING DELTA
COMMENT 'Governed wallchart step matching rules migrated from Power BI M keyword tables';
