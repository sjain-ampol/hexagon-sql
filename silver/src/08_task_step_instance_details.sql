-- =============================================================================
-- 08_task_step_instance_details.sql  |  hexagon_task_step_instance_details
-- Source: hexagon.hexagon_bronze.hexagon_task_step_instance_details_py
-- Target: hexagon.hexagon_silver.task_step_instance_details_sql
-- Keys:   INSTANCE, PROJECT, ID
-- Seq:    _scd_sequence = coalesce(RecordLastModified, _ingested_at)
-- Except: load_timestamp, _scd_sequence
-- =============================================================================

CREATE TEMPORARY STREAMING LIVE VIEW v_hexagon_task_step_instance_details AS
SELECT
  Instances_Id                                AS INSTANCE,
  Projects_Id                                 AS PROJECT,
  Id                                          AS ID,
  AssetPackName                               AS ASSET_PACK_NAME,
  AssetTag                                    AS ASSET_TAG,
  Comment                                     AS COMMENT,
  CompanyInstanceId                           AS COMPANY_INSTANCE_ID,
  CompletedBy                                 AS COMPLETED_BY,
  CompletedDate                               AS COMPLETED_DATE,
  DocumentId                                  AS DOCUMENT_ID,
  InspAnswer                                  AS INSP_ANSWER,
  InspectionType                              AS INSPECTION_TYPE,
  IsNa                                        AS IS_NA,
  IsNaDisabled                                AS IS_NA_DISABLED,
  IsPunchlist                                 AS IS_PUNCHLIST,
  IsStepRequired                              AS IS_STEP_REQUIRED,
  LoopName                                    AS LOOP_NAME,
  cast(OrderSequence as bigint)               AS ORDER_SEQUENCE,
  ParameterReading                            AS PARAMETER_READING,
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
  PctWeight                                   AS PCT_WEIGHT,
  PunchlistItem                               AS PUNCHLIST_ITEM,
  RecordCreated                               AS RECORD_CREATED,
  RecordCreatedBy                             AS RECORD_CREATED_BY,
  RecordLastModified                          AS RECORD_LAST_MODIFIED,
  RecordLastModifiedBy                        AS RECORD_LAST_MODIFIED_BY,
  RecordRemovedBy                             AS RECORD_REMOVED_BY,
  StepAction                                  AS STEP_ACTION,
  StepAnswer                                  AS STEP_ANSWER,
  StepAnswerId                                AS STEP_ANSWER_ID,
  StepAnswerMap                               AS STEP_ANSWER_MAP,
  StepIntialsDate                             AS STEP_INTIALS_DATE,
  StepSequence                                AS STEP_SEQUENCE,
  StepType                                    AS STEP_TYPE,
  StepTypeId                                  AS STEP_TYPE_ID,
  TaskId                                      AS TASK_ID,
  TaskName                                    AS TASK_NAME,
  TaskStepId                                  AS TASK_STEP_ID,
  WorkBreakdownUp3Id                          AS WORK_BREAKDOWN_UP3_ID,
  dtRemoved                                   AS DT_REMOVED,
  (dtRemoved is null)                         AS ACTIVE_FLAG,
  cast(session_user() as string)              AS LOADED_BY,
  _ingested_at                                AS DH_EFF_DT,
  _ingested_at,
  _source_system,
  _source_table,
  current_timestamp()                         AS load_timestamp,
  coalesce(RecordLastModified, _ingested_at)  AS _scd_sequence
FROM STREAM(hexagon.hexagon_bronze.hexagon_task_step_instance_details_py);

CREATE OR REFRESH STREAMING TABLE task_step_instance_details_sql
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking'    = 'true'
);

APPLY CHANGES INTO LIVE.task_step_instance_details_sql
FROM STREAM(LIVE.v_hexagon_task_step_instance_details)
KEYS (INSTANCE, PROJECT, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;

-- =============================================================================
-- Recon flow: hard-delete reconciliation from
-- hexagon_task_step_instance_details_recon
-- =============================================================================

CREATE TEMPORARY STREAMING LIVE VIEW v_recon_hexagon_task_step_instance_details AS
SELECT
  Instances_Id                                        AS INSTANCE,
  Projects_Id                                         AS PROJECT,
  Id                                                  AS ID,
  if(hard_delete, false, cast(null as boolean))       AS ACTIVE_FLAG,
  dt_removed                                          AS DT_REMOVED,
  ingested_at                                         AS _scd_sequence,
  current_timestamp()                                 AS load_timestamp
FROM STREAM(hexagon.hexagon_bronze.hexagon_task_step_instance_details_recon);

CREATE FLOW recon_task_step_instance_details_sql
AS APPLY CHANGES INTO LIVE.task_step_instance_details_sql
FROM STREAM(LIVE.v_recon_hexagon_task_step_instance_details)
KEYS (INSTANCE, PROJECT, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;
