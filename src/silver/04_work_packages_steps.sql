-- =============================================================================
-- 04_work_packages_steps.sql  |  hexagon_work_packages_steps
-- Source: ${catalog}.${bronze_schema}.hexagon_work_packages_steps_py
-- Target: hexagon.hexagon_silver.work_packages_steps_sql
-- Keys:   INSTANCE, WORK_PACKAGES__ID, ID   (double underscore is exact)
-- Seq:    _scd_sequence = coalesce(RecordLastModified, _ingested_at)
-- Except: load_timestamp, _scd_sequence
-- =============================================================================

CREATE TEMPORARY VIEW v_hexagon_work_packages_steps AS
SELECT
  Instances_Id                                AS INSTANCE,
  WorkBreakdownUp3Id                          AS PROJECT,
  Id                                          AS ID,
  WorkPackages_Id                             AS WORK_PACKAGES__ID,
  AccessCode                                  AS ACCESS_CODE,
  cast(ActualMH as double)                    AS ACTUAL_MH,
  cast(ActualWTV as double)                   AS ACTUAL_WTV,
  AssetId                                     AS ASSET_ID,
  AssetPackId                                 AS ASSET_PACK_ID,
  AssetPackName                               AS ASSET_PACK_NAME,
  AssetTag                                    AS ASSET_TAG,
  cast(BudgetMH as double)                    AS BUDGET_MH,
  CanAddStep                                  AS CAN_ADD_STEP,
  CheckInOutResource                          AS CHECK_IN_OUT_RESOURCE,
  CheckInOutResourceId                        AS CHECK_IN_OUT_RESOURCE_ID,
  CheckedInDate                               AS CHECKED_IN_DATE,
  CheckedOutDate                              AS CHECKED_OUT_DATE,
  Comments                                    AS COMMENTS,
  CompletedBy                                 AS COMPLETED_BY,
  CompletedById                               AS COMPLETED_BY_ID,
  CompletedDate                               AS COMPLETED_DATE,
  DocumentId                                  AS DOCUMENT_ID,
  DocumentName                                AS DOCUMENT_NAME,
  cast(EVMH as double)                        AS EVMH,
  FIWP,
  FWCId                                       AS FWC_ID,
  FWV,
  IWPId                                       AS IWP_ID,
  IWPName                                     AS IWP_NAME,
  IgnoreStep                                  AS IGNORE_STEP,
  InspAnswer                                  AS INSP_ANSWER,
  InspectionType                              AS INSPECTION_TYPE,
  InspectionTypeId                            AS INSPECTION_TYPE_ID,
  IsCheckedOut                                AS IS_CHECKED_OUT,
  IsNA                                        AS IS_NA,
  IsNaDisabled                                AS IS_NA_DISABLED,
  IsStepRequired                              AS IS_STEP_REQUIRED,
  LoopId                                      AS LOOP_ID,
  LoopName                                    AS LOOP_NAME,
  cast(OrderSequence as bigint)               AS ORDER_SEQUENCE,
  ParameterReading1                           AS PARAMETER_READING1,
  ParameterReading2                           AS PARAMETER_READING2,
  ParameterReading3                           AS PARAMETER_READING3,
  ParameterReading4                           AS PARAMETER_READING4,
  ParameterReading5                           AS PARAMETER_READING5,
  ParameterReading6                           AS PARAMETER_READING6,
  ParameterReading7                           AS PARAMETER_READING7,
  ParameterReading8                           AS PARAMETER_READING8,
  ParameterReading9                           AS PARAMETER_READING9,
  ParameterReading10                          AS PARAMETER_READING10,
  ParameterReading11                          AS PARAMETER_READING11,
  ParameterReading12                          AS PARAMETER_READING12,
  ParameterReading13                          AS PARAMETER_READING13,
  ParameterReading14                          AS PARAMETER_READING14,
  ParameterReading15                          AS PARAMETER_READING15,
  ParentId                                    AS PARENT_ID,
  PlanEndDate                                 AS PLAN_END_DATE,
  PlanStartDate                               AS PLAN_START_DATE,
  cast(PlanWTV as double)                     AS PLAN_WTV,
  RecordCreated                               AS RECORD_CREATED,
  RecordCreatedBy                             AS RECORD_CREATED_BY,
  RecordCreatedById                           AS RECORD_CREATED_BY_ID,
  RecordLastModified                          AS RECORD_LAST_MODIFIED,
  RecordLastModifiedBy                        AS RECORD_LAST_MODIFIED_BY,
  RecordLastModifiedById                      AS RECORD_LAST_MODIFIED_BY_ID,
  RecordRemovedBy                             AS RECORD_REMOVED_BY,
  RecordRemovedById                           AS RECORD_REMOVED_BY_ID,
  ShowHeader                                  AS SHOW_HEADER,
  ShowLabel                                   AS SHOW_LABEL,
  StepAction                                  AS STEP_ACTION,
  StepAnswerId                                AS STEP_ANSWER_ID,
  StepAnswerNCId                              AS STEP_ANSWER_NC_ID,
  StepHeaders                                 AS STEP_HEADERS,
  StepParameters                              AS STEP_PARAMETERS,
  StepQuestion                                AS STEP_QUESTION,
  StepSequence                                AS STEP_SEQUENCE,
  StepSequenceId                              AS STEP_SEQUENCE_ID,
  StepTypeId                                  AS STEP_TYPE_ID,
  TaskId                                      AS TASK_ID,
  TaskName                                    AS TASK_NAME,
  WPId                                        AS WP_ID,
  WPName                                      AS WP_NAME,
  WPStepId                                    AS WP_STEP_ID,
  WPStepName                                  AS WP_STEP_NAME,
  WVUoM                                       AS WV_UOM,
  WorkBreakdownUp3Id                          AS WORK_BREAKDOWN_UP3_ID,
  WorkStepType                                AS WORK_STEP_TYPE,
  WorkStepTypeId                              AS WORK_STEP_TYPE_ID,
  WorkVolumeType                              AS WORK_VOLUME_TYPE,
  dtRemoved                                   AS DT_REMOVED,
  (dtRemoved is null)                         AS ACTIVE_FLAG,
  cast(session_user() as string)              AS LOADED_BY,
  _ingested_at                                AS DH_EFF_DT,
  _ingested_at,
  _source_system,
  _source_table,
  current_timestamp()                         AS load_timestamp,
  coalesce(RecordLastModified, _ingested_at)  AS _scd_sequence
FROM STREAM(${catalog}.${bronze_schema}.hexagon_work_packages_steps_py) WITH (SKIPCHANGECOMMITS);

CREATE OR REFRESH STREAMING TABLE work_packages_steps_sql
CLUSTER BY (INSTANCE, WORK_PACKAGES__ID, ID)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking'    = 'true'
);

CREATE FLOW cdc_work_packages_steps_sql AS AUTO CDC INTO work_packages_steps_sql
FROM STREAM(v_hexagon_work_packages_steps)
KEYS (INSTANCE, WORK_PACKAGES__ID, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;

-- =============================================================================
-- Recon flow: hard-delete reconciliation from hexagon_work_packages_steps_recon
-- =============================================================================

CREATE TEMPORARY VIEW v_recon_hexagon_work_packages_steps AS
SELECT
  Instances_Id                                        AS INSTANCE,
  WorkPackages_Id                                     AS WORK_PACKAGES__ID,
  Id                                                  AS ID,
  if(hard_delete, false, cast(null as boolean))       AS ACTIVE_FLAG,
  dt_removed                                          AS DT_REMOVED,
  ingested_at                                         AS _scd_sequence,
  current_timestamp()                                 AS load_timestamp
FROM STREAM(${catalog}.${bronze_schema}.hexagon_work_packages_steps_recon) WITH (SKIPCHANGECOMMITS);

CREATE FLOW recon_work_packages_steps_sql
AS AUTO CDC INTO work_packages_steps_sql
FROM STREAM(v_recon_hexagon_work_packages_steps)
KEYS (INSTANCE, WORK_PACKAGES__ID, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;
