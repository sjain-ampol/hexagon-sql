-- =============================================================================
-- 03_work_packages.sql  |  hexagon_work_packages
-- Source: hexagon.hexagon_bronze.hexagon_work_packages_py
-- Target: hexagon.hexagon_silver.work_packages_sql
-- Keys:   INSTANCE, ID
-- Seq:    _scd_sequence = coalesce(RecordLastModified, _ingested_at)
-- Except: load_timestamp, _scd_sequence
-- =============================================================================

CREATE TEMPORARY STREAMING LIVE VIEW v_hexagon_work_packages AS
SELECT
  Instances_Id                                AS INSTANCE,
  WorkBreakdownUp3Id                          AS PROJECT,
  Id                                          AS ID,
  cast(ActualMH as double)                    AS ACTUAL_MH,
  ActualStartDate                             AS ACTUAL_START_DATE,
  cast(ActualWTV as double)                   AS ACTUAL_WTV,
  AutoTag                                     AS AUTO_TAG,
  cast(BudgetMH as double)                    AS BUDGET_MH,
  ClosingComments                             AS CLOSING_COMMENTS,
  Comment                                     AS COMMENT,
  CompanyInstanceId                           AS COMPANY_INSTANCE_ID,
  CurrentWorkflowState                        AS CURRENT_WORKFLOW_STATE,
  CurrentWorkflowStateId                      AS CURRENT_WORKFLOW_STATE_ID,
  Cwp                                         AS CWP,
  DataSourceId                                AS DATA_SOURCE_ID,
  cast(EVMH as double)                        AS EVMH,
  Ewp                                         AS EWP,
  ExternalLink                                AS EXTERNAL_LINK,
  Field001                                    AS FIELD001,
  Field002                                    AS FIELD002,
  Field003                                    AS FIELD003,
  Field004                                    AS FIELD004,
  Field005                                    AS FIELD005,
  Field006                                    AS FIELD006,
  Field007                                    AS FIELD007,
  Field008                                    AS FIELD008,
  Field009                                    AS FIELD009,
  Field010                                    AS FIELD010,
  Field011                                    AS FIELD011,
  Field012                                    AS FIELD012,
  Field013                                    AS FIELD013,
  Field014                                    AS FIELD014,
  Field015                                    AS FIELD015,
  Field016                                    AS FIELD016,
  Field017                                    AS FIELD017,
  Field018                                    AS FIELD018,
  Field019                                    AS FIELD019,
  Field020                                    AS FIELD020,
  JobCategory                                 AS JOB_CATEGORY,
  JobCategoryId                               AS JOB_CATEGORY_ID,
  JobCategorySummary                          AS JOB_CATEGORY_SUMMARY,
  LaborEqMat                                  AS LABOR_EQ_MAT,
  PDFImageId                                  AS PDF_IMAGE_ID,
  Parent                                      AS PARENT,
  ParentId                                    AS PARENT_ID,
  PhysicalLocationId                          AS PHYSICAL_LOCATION_ID,
  PhysicalLocationSummary                     AS PHYSICAL_LOCATION_SUMMARY,
  PhysicalLocationUp1Id                       AS PHYSICAL_LOCATION_UP1_ID,
  PhysicalLocationUp1Summary                  AS PHYSICAL_LOCATION_UP1_SUMMARY,
  PhysicalLocationUp2Id                       AS PHYSICAL_LOCATION_UP2_ID,
  PhysicalLocationUp2Summary                  AS PHYSICAL_LOCATION_UP2_SUMMARY,
  PhysicalLocationUp3Id                       AS PHYSICAL_LOCATION_UP3_ID,
  PhysicalLocationUp3Summary                  AS PHYSICAL_LOCATION_UP3_SUMMARY,
  cast(PlanWTV as double)                     AS PLAN_WTV,
  ProcessBreakdown                            AS PROCESS_BREAKDOWN,
  ProcessBreakdownId                          AS PROCESS_BREAKDOWN_ID,
  ProcessBreakdownUp1Id                       AS PROCESS_BREAKDOWN_UP1_ID,
  ProcessBreakdownUp1Summary                  AS PROCESS_BREAKDOWN_UP1_SUMMARY,
  ProcessBreakdownUp2Id                       AS PROCESS_BREAKDOWN_UP2_ID,
  ProcessBreakdownUp2Summary                  AS PROCESS_BREAKDOWN_UP2_SUMMARY,
  ProcessBreakdownUp3Id                       AS PROCESS_BREAKDOWN_UP3_ID,
  ProcessBreakdownUp3Summary                  AS PROCESS_BREAKDOWN_UP3_SUMMARY,
  ProgressMethod                              AS PROGRESS_METHOD,
  RGBColor                                    AS RGB_COLOR,
  RecordCreated                               AS RECORD_CREATED,
  RecordCreatedBy                             AS RECORD_CREATED_BY,
  RecordCreatedById                           AS RECORD_CREATED_BY_ID,
  RecordLastModified                          AS RECORD_LAST_MODIFIED,
  RecordLastModifiedBy                        AS RECORD_LAST_MODIFIED_BY,
  RecordLastModifiedById                      AS RECORD_LAST_MODIFIED_BY_ID,
  RecordRemovedBy                             AS RECORD_REMOVED_BY,
  RecordRemovedById                           AS RECORD_REMOVED_BY_ID,
  ResponsibleCompany                          AS RESPONSIBLE_COMPANY,
  ResponsibleCompanyId                        AS RESPONSIBLE_COMPANY_ID,
  ScannedImageId                              AS SCANNED_IMAGE_ID,
  ScheduleEndDate                             AS SCHEDULE_END_DATE,
  ScheduleStartDate                           AS SCHEDULE_START_DATE,
  StatusColor                                 AS STATUS_COLOR,
  StatusHexColor                              AS STATUS_HEX_COLOR,
  SupplierCompany                             AS SUPPLIER_COMPANY,
  SupplierCompanyId                           AS SUPPLIER_COMPANY_ID,
  UsesProcessBreakdown                        AS USES_PROCESS_BREAKDOWN,
  WPDescription                               AS WP_DESCRIPTION,
  WPDiscipline                                AS WP_DISCIPLINE,
  WPDisciplineId                              AS WP_DISCIPLINE_ID,
  WPId                                        AS WP_ID,
  WPName                                      AS WP_NAME,
  WPPriority                                  AS WP_PRIORITY,
  WPPriorityId                                AS WP_PRIORITY_ID,
  WPScheduledBy                               AS WP_SCHEDULED_BY,
  WPScheduledById                             AS WP_SCHEDULED_BY_ID,
  WPSummary                                   AS WP_SUMMARY,
  WorkBreakdown                               AS WORK_BREAKDOWN,
  WorkBreakdownId                             AS WORK_BREAKDOWN_ID,
  WorkBreakdownUp1Id                          AS WORK_BREAKDOWN_UP1_ID,
  WorkBreakdownUp1Summary                     AS WORK_BREAKDOWN_UP1_SUMMARY,
  WorkBreakdownUp2Id                          AS WORK_BREAKDOWN_UP2_ID,
  WorkBreakdownUp2Summary                     AS WORK_BREAKDOWN_UP2_SUMMARY,
  WorkBreakdownUp3Id                          AS WORK_BREAKDOWN_UP3_ID,
  WorkBreakdownUp3Summary                     AS WORK_BREAKDOWN_UP3_SUMMARY,
  WorkPackageClass                            AS WORK_PACKAGE_CLASS,
  WorkPackageType                             AS WORK_PACKAGE_TYPE,
  WorkPackageTypeId                           AS WORK_PACKAGE_TYPE_ID,
  WorkflowId                                  AS WORKFLOW_ID,
  dtRemoved                                   AS DT_REMOVED,
  (dtRemoved is null)                         AS ACTIVE_FLAG,
  cast(session_user() as string)              AS LOADED_BY,
  _ingested_at                                AS DH_EFF_DT,
  _ingested_at,
  _source_system,
  _source_table,
  current_timestamp()                         AS load_timestamp,
  coalesce(RecordLastModified, _ingested_at)  AS _scd_sequence
FROM STREAM(hexagon.hexagon_bronze.hexagon_work_packages_py);

CREATE OR REFRESH STREAMING TABLE work_packages_sql
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking'    = 'true'
);

APPLY CHANGES INTO LIVE.work_packages_sql
FROM STREAM(LIVE.v_hexagon_work_packages)
KEYS (INSTANCE, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;

-- =============================================================================
-- Recon flow: hard-delete reconciliation from hexagon_work_packages_recon
-- =============================================================================

CREATE TEMPORARY STREAMING LIVE VIEW v_recon_hexagon_work_packages AS
SELECT
  Instances_Id                                        AS INSTANCE,
  Id                                                  AS ID,
  if(hard_delete, false, cast(null as boolean))       AS ACTIVE_FLAG,
  dt_removed                                          AS DT_REMOVED,
  ingested_at                                         AS _scd_sequence,
  current_timestamp()                                 AS load_timestamp
FROM STREAM(hexagon.hexagon_bronze.hexagon_work_packages_recon);

CREATE FLOW recon_work_packages_sql
AS APPLY CHANGES INTO LIVE.work_packages_sql
FROM STREAM(LIVE.v_recon_hexagon_work_packages)
KEYS (INSTANCE, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;
