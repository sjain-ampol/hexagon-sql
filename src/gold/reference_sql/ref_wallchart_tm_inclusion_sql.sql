-- Task-model inclusion for wallchart scope (WallchartTMtable.xlsx).
-- DDL only — row data is loaded by create_reference_tables from a Unity Catalog
-- Volume Excel file (tm_inclusion_volume_path widget), using the same unpivot /
-- asset-class mapping as PBIT "WallChartTM Table - Inclusions".
-- asset_class uses PBIT codes: PSV, CV, COL, EXCH (not Gold CONTROL_VALVE/COLUMN/EXCHANGER).
CREATE SCHEMA IF NOT EXISTS {catalog}.{ref_schema};

CREATE TABLE IF NOT EXISTS {catalog}.{ref_schema}.ref_wallchart_tm_inclusion_sql (
  project BIGINT NOT NULL COMMENT 'Hexagon project id',
  asset_class STRING NOT NULL COMMENT 'PBIT wallchart type: PSV, CV, COL, EXCH',
  task_model_name STRING NOT NULL COMMENT 'Task model code e.g. TM-00177',
  source_column STRING COMMENT 'Optional Excel column name for audit',
  is_active BOOLEAN NOT NULL,
  effective_from DATE NOT NULL,
  effective_to DATE
)
USING DELTA
COMMENT 'Project x asset x task-model inclusion for wallchart — loaded from Volume Excel by create_reference_tables';
