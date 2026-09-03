-- =============================================================================
-- 01_projects.sql  |  hexagon_projects
-- Source: hexagon.hexagon_bronze.hexagon_projects_py
-- Target: hexagon.hexagon_silver.projects_sql
-- Keys:   INSTANCE, ID
-- Seq:    load_timestamp  (no RecordLastModified on this entity)
-- Except: load_timestamp
-- =============================================================================

-- Streaming view: bronze PascalCase -> UPPER_SNAKE_CASE
CREATE TEMPORARY STREAMING LIVE VIEW v_hexagon_projects AS
SELECT
  Instances_Id                          AS INSTANCE,
  Id                                    AS ID,
  ProjectIdentifier                     AS PROJECT_IDENTIFIER,
  ProjectName                           AS PROJECT_NAME,
  Summary                               AS SUMMARY,
  true                                  AS ACTIVE_FLAG,
  cast(session_user() as string)        AS LOADED_BY,
  _ingested_at                          AS DH_EFF_DT,
  _ingested_at,
  _source_system,
  _source_table,
  current_timestamp()                   AS load_timestamp
FROM STREAM(hexagon.hexagon_bronze.hexagon_projects_py);

-- Target streaming table
CREATE OR REFRESH STREAMING TABLE projects_sql
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking'    = 'true'
);

-- SCD Type 1 merge
APPLY CHANGES INTO LIVE.projects_sql
FROM STREAM(LIVE.v_hexagon_projects)
KEYS (INSTANCE, ID)
IGNORE NULL UPDATES
SEQUENCE BY load_timestamp
COLUMNS * EXCEPT (load_timestamp)
STORED AS SCD TYPE 1;

-- =============================================================================
-- Recon flow: hard-delete reconciliation from hexagon_projects_recon
-- =============================================================================

CREATE TEMPORARY STREAMING LIVE VIEW v_recon_hexagon_projects AS
SELECT
  Instances_Id                                        AS INSTANCE,
  Id                                                  AS ID,
  if(hard_delete, false, cast(null as boolean))       AS ACTIVE_FLAG,
  dt_removed                                          AS DT_REMOVED,
  ingested_at                                         AS load_timestamp
FROM STREAM(hexagon.hexagon_bronze.hexagon_projects_recon);

CREATE FLOW recon_projects_sql
AS APPLY CHANGES INTO LIVE.projects_sql
FROM STREAM(LIVE.v_recon_hexagon_projects)
KEYS (INSTANCE, ID)
IGNORE NULL UPDATES
SEQUENCE BY load_timestamp
COLUMNS * EXCEPT (load_timestamp)
STORED AS SCD TYPE 1;
