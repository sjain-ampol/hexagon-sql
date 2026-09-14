-- =============================================================================
-- 07_tasks_tests_planned_details.sql  |  hexagon_tasks_tests_planned_details
-- Source: ${catalog}.${bronze_schema}.hexagon_tasks_tests_planned_details_py
-- Target: hexagon.hexagon_silver.tasks_tests_planned_details_sql
-- Keys:   INSTANCE, ID
-- Seq:    _scd_sequence = coalesce(RecordLastModified, _ingested_at)
-- Except: load_timestamp, _scd_sequence
-- =============================================================================

CREATE TEMPORARY VIEW v_hexagon_tasks_tests_planned_details AS
SELECT
  Instances_Id                                AS INSTANCE,
  WorkBreakdownUp3Id                          AS PROJECT,
  Id                                          AS ID,
  ActualEndDate                               AS ACTUAL_END_DATE,
  ActualStartDate                             AS ACTUAL_START_DATE,
  ApprovedBy                                  AS APPROVED_BY,
  ApprovedDate                                AS APPROVED_DATE,
  AssetId                                     AS ASSET_ID,
  AssetPackId                                 AS ASSET_PACK_ID,
  AssetPackName                               AS ASSET_PACK_NAME,
  AssetTag                                    AS ASSET_TAG,
  CertLockSyst                                AS CERT_LOCK_SYST,
  CertificateCategory                         AS CERTIFICATE_CATEGORY,
  CheckInOutResource                          AS CHECK_IN_OUT_RESOURCE,
  CheckInOutResourceId                        AS CHECK_IN_OUT_RESOURCE_ID,
  CheckedInDate                               AS CHECKED_IN_DATE,
  CheckedOutDate                              AS CHECKED_OUT_DATE,
  ClosingComment                              AS CLOSING_COMMENT,
  Comment                                     AS COMMENT,
  CompanyInstanceId                           AS COMPANY_INSTANCE_ID,
  CompletedBy                                 AS COMPLETED_BY,
  CompletedByCompany                          AS COMPLETED_BY_COMPANY,
  CompletedByTitle                            AS COMPLETED_BY_TITLE,
  ContractName                                AS CONTRACT_NAME,
  EnvironmentalRiskComment                    AS ENVIRONMENTAL_RISK_COMMENT,
  ExecutionType                               AS EXECUTION_TYPE,
  ExecutionTypeId                             AS EXECUTION_TYPE_ID,
  Field001                                    AS FIELD001,
  Field002                                    AS FIELD002,
  Field003                                    AS FIELD003,
  Field004                                    AS FIELD004,
  Field005                                    AS FIELD005,
  HasCancelled                                AS HAS_CANCELLED,
  HasPermits                                  AS HAS_PERMITS,
  HasRa                                       AS HAS_RA,
  InterfaceToolID                             AS INTERFACE_TOOL_ID,
  IsCheckedOut                                AS IS_CHECKED_OUT,
  IsException                                 AS IS_EXCEPTION,
  IsLockDates                                 AS IS_LOCK_DATES,
  IsolationRequired                           AS ISOLATION_REQUIRED,
  JobId                                       AS JOB_ID,
  JobName                                     AS JOB_NAME,
  LoopId                                      AS LOOP_ID,
  LoopName                                    AS LOOP_NAME,
  Manhours                                    AS MANHOURS,
  ModelAssetTypeId                            AS MODEL_ASSET_TYPE_ID,
  PartActualEndDate                           AS PART_ACTUAL_END_DATE,
  PartCompletedBy                             AS PART_COMPLETED_BY,
  Persons                                     AS PERSONS,
  PhysicalLocation                            AS PHYSICAL_LOCATION,
  PhysicalLocationId                          AS PHYSICAL_LOCATION_ID,
  PhysicalLocationUp1Id                       AS PHYSICAL_LOCATION_UP1_ID,
  PhysicalLocationUp1Summary                  AS PHYSICAL_LOCATION_UP1_SUMMARY,
  PhysicalLocationUp2Id                       AS PHYSICAL_LOCATION_UP2_ID,
  PhysicalLocationUp2Summary                  AS PHYSICAL_LOCATION_UP2_SUMMARY,
  PhysicalLocationUp3Id                       AS PHYSICAL_LOCATION_UP3_ID,
  PhysicalLocationUp3Summary                  AS PHYSICAL_LOCATION_UP3_SUMMARY,
  ProcessBreakdown                            AS PROCESS_BREAKDOWN,
  ProcessBreakdownId                          AS PROCESS_BREAKDOWN_ID,
  ProcessBreakdownUp1Id                       AS PROCESS_BREAKDOWN_UP1_ID,
  ProcessBreakdownUp1Summary                  AS PROCESS_BREAKDOWN_UP1_SUMMARY,
  ProcessBreakdownUp2Id                       AS PROCESS_BREAKDOWN_UP2_ID,
  ProcessBreakdownUp2Summary                  AS PROCESS_BREAKDOWN_UP2_SUMMARY,
  ProcessBreakdownUp3Id                       AS PROCESS_BREAKDOWN_UP3_ID,
  ProcessBreakdownUp3Summary                  AS PROCESS_BREAKDOWN_UP3_SUMMARY,
  ProjectActivityId                           AS PROJECT_ACTIVITY_ID,
  ProjectActivityItem                         AS PROJECT_ACTIVITY_ITEM,
  ProjectControlTaskId                        AS PROJECT_CONTROL_TASK_ID,
  ProjectTaskItem                             AS PROJECT_TASK_ITEM,
  RecordCreated                               AS RECORD_CREATED,
  RecordCreatedBy                             AS RECORD_CREATED_BY,
  RecordLastModified                          AS RECORD_LAST_MODIFIED,
  RecordLastModifiedBy                        AS RECORD_LAST_MODIFIED_BY,
  RecordRemovedBy                             AS RECORD_REMOVED_BY,
  ResponsibleApprover                         AS RESPONSIBLE_APPROVER,
  ResponsibleCompany                          AS RESPONSIBLE_COMPANY,
  ResponsibleCompanyId                        AS RESPONSIBLE_COMPANY_ID,
  RiskRating                                  AS RISK_RATING,
  SafetyRequirementComment                    AS SAFETY_REQUIREMENT_COMMENT,
  ScheduledActualDuration                     AS SCHEDULED_ACTUAL_DURATION,
  ScheduledDuration                           AS SCHEDULED_DURATION,
  ScheduledEndDate                            AS SCHEDULED_END_DATE,
  ScheduledLagOrLead                          AS SCHEDULED_LAG_OR_LEAD,
  ScheduledStartDate                          AS SCHEDULED_START_DATE,
  StatusColor                                 AS STATUS_COLOR,
  StatusHexColor                              AS STATUS_HEX_COLOR,
  StatusRGBColor                              AS STATUS_RGB_COLOR,
  SupplierCompany                             AS SUPPLIER_COMPANY,
  Tag                                         AS TAG,
  TaskAltitude                                AS TASK_ALTITUDE,
  TaskCategory                                AS TASK_CATEGORY,
  TaskCategoryId                              AS TASK_CATEGORY_ID,
  TaskCategorySummary                         AS TASK_CATEGORY_SUMMARY,
  TaskDescription                             AS TASK_DESCRIPTION,
  TaskDiscipline                              AS TASK_DISCIPLINE,
  TaskDisciplineId                            AS TASK_DISCIPLINE_ID,
  TaskDisciplineSummary                       AS TASK_DISCIPLINE_SUMMARY,
  TaskDuration                                AS TASK_DURATION,
  TaskDurationEu                              AS TASK_DURATION_EU,
  TaskException                               AS TASK_EXCEPTION,
  TaskId                                      AS TASK_ID,
  TaskLatitude                                AS TASK_LATITUDE,
  TaskLongitude                               AS TASK_LONGITUDE,
  TaskModelName                               AS TASK_MODEL_NAME,
  TaskName                                    AS TASK_NAME,
  TaskPriority                                AS TASK_PRIORITY,
  TaskPrioritySummary                         AS TASK_PRIORITY_SUMMARY,
  TaskRevision                                AS TASK_REVISION,
  TaskState                                   AS TASK_STATE,
  TaskStateId                                 AS TASK_STATE_ID,
  TaskSummary                                 AS TASK_SUMMARY,
  TaskType                                    AS TASK_TYPE,
  TaskTypeId                                  AS TASK_TYPE_ID,
  TaskTypeSummary                             AS TASK_TYPE_SUMMARY,
  WorkBreakdown                               AS WORK_BREAKDOWN,
  WorkBreakdownId                             AS WORK_BREAKDOWN_ID,
  WorkBreakdownUp1Id                          AS WORK_BREAKDOWN_UP1_ID,
  WorkBreakdownUp1Summary                     AS WORK_BREAKDOWN_UP1_SUMMARY,
  WorkBreakdownUp2Id                          AS WORK_BREAKDOWN_UP2_ID,
  WorkBreakdownUp2Summary                     AS WORK_BREAKDOWN_UP2_SUMMARY,
  WorkBreakdownUp3Id                          AS WORK_BREAKDOWN_UP3_ID,
  WorkBreakdownUp3Summary                     AS WORK_BREAKDOWN_UP3_SUMMARY,
  WorkGroupId                                 AS WORK_GROUP_ID,
  WorkGroupName                               AS WORK_GROUP_NAME,
  dtRemoved                                   AS DT_REMOVED,
  (dtRemoved is null)                         AS ACTIVE_FLAG,
  cast(session_user() as string)              AS LOADED_BY,
  _ingested_at                                AS DH_EFF_DT,
  _ingested_at,
  _source_system,
  _source_table,
  current_timestamp()                         AS load_timestamp,
  coalesce(RecordLastModified, _ingested_at)  AS _scd_sequence
FROM STREAM(${catalog}.${bronze_schema}.hexagon_tasks_tests_planned_details_py) WITH (SKIPCHANGECOMMITS);

CREATE OR REFRESH STREAMING TABLE tasks_tests_planned_details_sql
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking'    = 'true'
);

CREATE FLOW cdc_tasks_tests_planned_details_sql AS AUTO CDC INTO tasks_tests_planned_details_sql
FROM STREAM(v_hexagon_tasks_tests_planned_details)
KEYS (INSTANCE, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;

-- =============================================================================
-- Recon flow: hard-delete reconciliation from
-- hexagon_tasks_tests_planned_details_recon
-- =============================================================================

CREATE TEMPORARY VIEW v_recon_hexagon_tasks_tests_planned_details AS
SELECT
  Instances_Id                                        AS INSTANCE,
  Id                                                  AS ID,
  if(hard_delete, false, cast(null as boolean))       AS ACTIVE_FLAG,
  dt_removed                                          AS DT_REMOVED,
  ingested_at                                         AS _scd_sequence,
  current_timestamp()                                 AS load_timestamp
FROM STREAM(${catalog}.${bronze_schema}.hexagon_tasks_tests_planned_details_recon) WITH (SKIPCHANGECOMMITS);

CREATE FLOW recon_tasks_tests_planned_details_sql
AS AUTO CDC INTO tasks_tests_planned_details_sql
FROM STREAM(v_recon_hexagon_tasks_tests_planned_details)
KEYS (INSTANCE, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;
