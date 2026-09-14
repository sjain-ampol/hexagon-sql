-- =============================================================================
-- 05_work_package_details.sql  |  hexagon_work_package_details
-- Source: ${catalog}.${bronze_schema}.hexagon_work_package_details_py
-- Target: hexagon.hexagon_silver.work_package_details_sql
-- Keys:   INSTANCE, ID
-- Seq:    _scd_sequence = coalesce(RecordLastModified, _ingested_at)
-- Except: load_timestamp, _scd_sequence
-- =============================================================================

CREATE TEMPORARY VIEW v_hexagon_work_package_details AS
SELECT
  Instances_Id                                AS INSTANCE,
  WorkBreakdownUp3Id                          AS PROJECT,
  Id                                          AS ID,
  cast(ActualMH as double)                    AS ACTUAL_MH,
  ActualStartDate                             AS ACTUAL_START_DATE,
  cast(ActualWTV as double)                   AS ACTUAL_WTV,
  cast(BudgetMH as double)                    AS BUDGET_MH,
  ClosingComments                             AS CLOSING_COMMENTS,
  Comment                                     AS COMMENT,
  Comments                                    AS COMMENTS,
  CompanyInstanceId                           AS COMPANY_INSTANCE_ID,
  CurrentWorkflowState                        AS CURRENT_WORKFLOW_STATE,
  Cwp                                         AS CWP,
  DataSourceId                                AS DATA_SOURCE_ID,
  cast(EVMH as double)                        AS EVMH,
  Ewp                                         AS EWP,
  ExchangeName                                AS EXCHANGE_NAME,
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
  LaborEqMat                                  AS LABOR_EQ_MAT,
  PDFImageId                                  AS PDF_IMAGE_ID,
  Parent                                      AS PARENT,
  PhysicalLocation                            AS PHYSICAL_LOCATION,
  PhysicalLocationUp1Summary                  AS PHYSICAL_LOCATION_UP1_SUMMARY,
  PhysicalLocationUp2Summary                  AS PHYSICAL_LOCATION_UP2_SUMMARY,
  PhysicalLocationUp3Summary                  AS PHYSICAL_LOCATION_UP3_SUMMARY,
  cast(PlanWTV as double)                     AS PLAN_WTV,
  ProcessBreakdown                            AS PROCESS_BREAKDOWN,
  ProcessBreakdownUp1Summary                  AS PROCESS_BREAKDOWN_UP1_SUMMARY,
  ProcessBreakdownUp2Summary                  AS PROCESS_BREAKDOWN_UP2_SUMMARY,
  ProcessBreakdownUp3Summary                  AS PROCESS_BREAKDOWN_UP3_SUMMARY,
  ProgressMethod                              AS PROGRESS_METHOD,
  RecordCreated                               AS RECORD_CREATED,
  RecordCreatedBy                             AS RECORD_CREATED_BY,
  RecordLastModified                          AS RECORD_LAST_MODIFIED,
  RecordLastModifiedBy                        AS RECORD_LAST_MODIFIED_BY,
  RecordRemovedBy                             AS RECORD_REMOVED_BY,
  ResponsibleCompany                          AS RESPONSIBLE_COMPANY,
  ResponsibleCompanyId                        AS RESPONSIBLE_COMPANY_ID,
  ScannedImageId                              AS SCANNED_IMAGE_ID,
  ScheduleEndDate                             AS SCHEDULE_END_DATE,
  ScheduleStartDate                           AS SCHEDULE_START_DATE,
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
  WPSummary                                   AS WP_SUMMARY,
  WorkBreakdown                               AS WORK_BREAKDOWN,
  WorkBreakdownUp1Summary                     AS WORK_BREAKDOWN_UP1_SUMMARY,
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
FROM STREAM(${catalog}.${bronze_schema}.hexagon_work_package_details_py) WITH (SKIPCHANGECOMMITS);

CREATE OR REFRESH STREAMING TABLE work_package_details_sql
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking'    = 'true'
);

CREATE FLOW cdc_work_package_details_sql AS AUTO CDC INTO work_package_details_sql
FROM STREAM(v_hexagon_work_package_details)
KEYS (INSTANCE, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;

-- =============================================================================
-- Recon flow: hard-delete reconciliation from hexagon_work_package_details_recon
-- =============================================================================

CREATE TEMPORARY VIEW v_recon_hexagon_work_package_details AS
SELECT
  Instances_Id                                        AS INSTANCE,
  Id                                                  AS ID,
  if(hard_delete, false, cast(null as boolean))       AS ACTIVE_FLAG,
  dt_removed                                          AS DT_REMOVED,
  ingested_at                                         AS _scd_sequence,
  current_timestamp()                                 AS load_timestamp
FROM STREAM(${catalog}.${bronze_schema}.hexagon_work_package_details_recon) WITH (SKIPCHANGECOMMITS);

CREATE FLOW recon_work_package_details_sql
AS AUTO CDC INTO work_package_details_sql
FROM STREAM(v_recon_hexagon_work_package_details)
KEYS (INSTANCE, ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;
