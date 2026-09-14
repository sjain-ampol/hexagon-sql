-- =============================================================================
-- 06_work_package_workflow_lists.sql  |  hexagon_work_package_workflow_lists
-- Source: ${catalog}.${bronze_schema}.hexagon_work_package_workflow_lists_py
-- Target: hexagon.hexagon_silver.work_package_workflow_lists_sql
-- Keys:   INSTANCE, WORK_PACKAGE_DETAILS_ID, ID
-- Seq:    _scd_sequence = coalesce(StateDate, ResourceStateDate, _ingested_at)
-- Except: load_timestamp, _scd_sequence
-- Note:   No WorkBreakdownUp3Id, no dtRemoved, no RecordLastModified on source.
--         PROJECT is cast(null as bigint), DT_REMOVED is cast(null as timestamp),
--         ACTIVE_FLAG is always true.
-- =============================================================================

CREATE TEMPORARY VIEW v_hexagon_work_package_workflow_lists AS
SELECT
  Instances_Id                                              AS INSTANCE,
  cast(null as bigint)                                      AS PROJECT,
  WorkPackageDetails_Id                                     AS WORK_PACKAGE_DETAILS_ID,
  Id                                                        AS ID,
  WorkflowId                                                AS WORKFLOW_ID,
  StateStatusName                                           AS STATE_STATUS_NAME,
  StateSequence                                             AS STATE_SEQUENCE,
  IsInactive                                                AS IS_INACTIVE,
  StateComment                                              AS STATE_COMMENT,
  StateRevision                                             AS STATE_REVISION,
  StateDate                                                 AS STATE_DATE,
  ResourceSummary                                           AS RESOURCE_SUMMARY,
  ResourceStateDate                                         AS RESOURCE_STATE_DATE,
  ResourceTypeId                                            AS RESOURCE_TYPE_ID,
  ResourceStateSequence                                     AS RESOURCE_STATE_SEQUENCE,
  true                                                      AS ACTIVE_FLAG,
  cast(session_user() as string)                            AS LOADED_BY,
  _ingested_at                                              AS DH_EFF_DT,
  cast(null as timestamp)                                   AS DT_REMOVED,
  _ingested_at,
  _source_system,
  _source_table,
  current_timestamp()                                       AS load_timestamp,
  coalesce(StateDate, ResourceStateDate, _ingested_at)      AS _scd_sequence
FROM STREAM(${catalog}.${bronze_schema}.hexagon_work_package_workflow_lists_py) WITH (SKIPCHANGECOMMITS);

CREATE OR REFRESH STREAMING TABLE work_package_workflow_lists_sql
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking'    = 'true'
);

CREATE FLOW cdc_work_package_workflow_lists_sql AS AUTO CDC INTO work_package_workflow_lists_sql
FROM STREAM(v_hexagon_work_package_workflow_lists)
KEYS (INSTANCE, WORK_PACKAGE_DETAILS_ID, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;

-- =============================================================================
-- Recon flow: hard-delete reconciliation from
-- hexagon_work_package_workflow_lists_recon
-- =============================================================================

CREATE TEMPORARY VIEW v_recon_hexagon_work_package_workflow_lists AS
SELECT
  Instances_Id                                        AS INSTANCE,
  WorkPackageDetails_Id                               AS WORK_PACKAGE_DETAILS_ID,
  Id                                                  AS ID,
  if(hard_delete, false, cast(null as boolean))       AS ACTIVE_FLAG,
  dt_removed                                          AS DT_REMOVED,
  ingested_at                                         AS _scd_sequence,
  current_timestamp()                                 AS load_timestamp
FROM STREAM(${catalog}.${bronze_schema}.hexagon_work_package_workflow_lists_recon) WITH (SKIPCHANGECOMMITS);

CREATE FLOW recon_work_package_workflow_lists_sql
AS AUTO CDC INTO work_package_workflow_lists_sql
FROM STREAM(v_recon_hexagon_work_package_workflow_lists)
KEYS (INSTANCE, WORK_PACKAGE_DETAILS_ID, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;
