-- =============================================================================
-- 02_assets.sql  |  hexagon_assets
-- Source: ${catalog}.${bronze_schema}.hexagon_assets_py
-- Target: hexagon.hexagon_silver.assets_sql
-- Keys:   INSTANCE, ASSET_ID, WORK_BREAKDOWN_UP3_ID  (NOT Id)
-- Seq:    _scd_sequence = coalesce(RecordLastModified, _ingested_at)
-- Except: load_timestamp, _scd_sequence
-- =============================================================================

CREATE TEMPORARY VIEW v_hexagon_assets AS
SELECT
  Instances_Id                                AS INSTANCE,
  WorkBreakdownUp3Id                          AS PROJECT,
  Id                                          AS ID,
  AssetId                                     AS ASSET_ID,
  AssetTag                                    AS ASSET_TAG,
  AssetDescription                            AS ASSET_DESCRIPTION,
  Service                                     AS SERVICE,
  AssetSummary                                AS ASSET_SUMMARY,
  WorkScope                                   AS WORK_SCOPE,
  TechSpec                                    AS TECH_SPEC,
  VendorPartNumber                            AS VENDOR_PART_NUMBER,
  ExternalLink                                AS EXTERNAL_LINK,
  DataSourceId                                AS DATA_SOURCE_ID,
  Comments                                    AS COMMENTS,
  Latitude                                    AS LATITUDE,
  Longitude                                   AS LONGITUDE,
  Altitude                                    AS ALTITUDE,
  ParentID                                    AS PARENT_ID,
  Parent                                      AS PARENT,
  InstrumentLineNumID                         AS INSTRUMENT_LINE_NUM_ID,
  InstrumentLineNum                           AS INSTRUMENT_LINE_NUM,
  IsDocVerified                               AS IS_DOC_VERIFIED,
  IsFieldVerified                             AS IS_FIELD_VERIFIED,
  ImageID                                     AS IMAGE_ID,
  Image2ID                                    AS IMAGE2_ID,
  AssetTypeId                                 AS ASSET_TYPE_ID,
  AssetType                                   AS ASSET_TYPE,
  AssetTypeSummary                            AS ASSET_TYPE_SUMMARY,
  AssetDisciplineId                           AS ASSET_DISCIPLINE_ID,
  AssetDiscipline                             AS ASSET_DISCIPLINE,
  AssetStatusId                               AS ASSET_STATUS_ID,
  AssetStatus                                 AS ASSET_STATUS,
  AssetPriorityId                             AS ASSET_PRIORITY_ID,
  AssetPriority                               AS ASSET_PRIORITY,
  PhysicalLocationId                          AS PHYSICAL_LOCATION_ID,
  PhysicalLocationUp1Id                       AS PHYSICAL_LOCATION_UP1_ID,
  PhysicalLocationUp2Id                       AS PHYSICAL_LOCATION_UP2_ID,
  PhysicalLocationUp3Id                       AS PHYSICAL_LOCATION_UP3_ID,
  PhysicalLocation                            AS PHYSICAL_LOCATION,
  PhysicalLocationUp1Summary                  AS PHYSICAL_LOCATION_UP1_SUMMARY,
  PhysicalLocationUp2Summary                  AS PHYSICAL_LOCATION_UP2_SUMMARY,
  PhysicalLocationUp3Summary                  AS PHYSICAL_LOCATION_UP3_SUMMARY,
  ProcessBreakdownId                          AS PROCESS_BREAKDOWN_ID,
  ProcessBreakdownUp1Id                       AS PROCESS_BREAKDOWN_UP1_ID,
  ProcessBreakdownUp2Id                       AS PROCESS_BREAKDOWN_UP2_ID,
  ProcessBreakdownUp3Id                       AS PROCESS_BREAKDOWN_UP3_ID,
  ProcessBreakdown                            AS PROCESS_BREAKDOWN,
  ProcessBreakdownUp1Summary                  AS PROCESS_BREAKDOWN_UP1_SUMMARY,
  ProcessBreakdownUp2Summary                  AS PROCESS_BREAKDOWN_UP2_SUMMARY,
  ProcessBreakdownUp3Summary                  AS PROCESS_BREAKDOWN_UP3_SUMMARY,
  ControlSystemBreakdownId                    AS CONTROL_SYSTEM_BREAKDOWN_ID,
  ControlSystemBreakdownUp1Id                 AS CONTROL_SYSTEM_BREAKDOWN_UP1_ID,
  ControlSystemBreakdownUp2Id                 AS CONTROL_SYSTEM_BREAKDOWN_UP2_ID,
  ControlSystemBreakdownUp3Id                 AS CONTROL_SYSTEM_BREAKDOWN_UP3_ID,
  ControlSystemBreakdown                      AS CONTROL_SYSTEM_BREAKDOWN,
  ControlSystemBreakdownUp1Summary            AS CONTROL_SYSTEM_BREAKDOWN_UP1_SUMMARY,
  ControlSystemBreakdownUp2Summary            AS CONTROL_SYSTEM_BREAKDOWN_UP2_SUMMARY,
  ControlSystemBreakdownUp3Summary            AS CONTROL_SYSTEM_BREAKDOWN_UP3_SUMMARY,
  ElectricalSourceElectricalRoom              AS ELECTRICAL_SOURCE_ELECTRICAL_ROOM,
  ElectricalSourceMCCNumber                   AS ELECTRICAL_SOURCE_MCC_NUMBER,
  ElectricalSourceMCCCubical                  AS ELECTRICAL_SOURCE_MCC_CUBICAL,
  SAPFunctionalLocation                       AS SAP_FUNCTIONAL_LOCATION,
  SAPMaterialCode                             AS SAP_MATERIAL_CODE,
  cast(ListPrice as decimal(38,12))           AS LIST_PRICE,
  PurchaseOrder                               AS PURCHASE_ORDER,
  PurchaseDate                                AS PURCHASE_DATE,
  VendorId                                    AS VENDOR_ID,
  Vendor                                      AS VENDOR,
  RecordCreated                               AS RECORD_CREATED,
  RecordCreatedBy                             AS RECORD_CREATED_BY,
  RecordCreatedById                           AS RECORD_CREATED_BY_ID,
  RecordLastModified                          AS RECORD_LAST_MODIFIED,
  RecordLastModifiedBy                        AS RECORD_LAST_MODIFIED_BY,
  RecordLastModifiedById                      AS RECORD_LAST_MODIFIED_BY_ID,
  dtRemoved                                   AS DT_REMOVED,
  RecordRemovedBy                             AS RECORD_REMOVED_BY,
  RecordRemovedById                           AS RECORD_REMOVED_BY_ID,
  WorkBreakdownUp3Id                          AS WORK_BREAKDOWN_UP3_ID,
  CompanyInstanceId                           AS COMPANY_INSTANCE_ID,
  (dtRemoved is null)                         AS ACTIVE_FLAG,
  cast(session_user() as string)              AS LOADED_BY,
  _ingested_at                                AS DH_EFF_DT,
  _ingested_at,
  _source_system,
  _source_table,
  current_timestamp()                         AS load_timestamp,
  coalesce(RecordLastModified, _ingested_at)  AS _scd_sequence
FROM STREAM(${catalog}.${bronze_schema}.hexagon_assets_py) WITH (SKIPCHANGECOMMITS);

CREATE OR REFRESH STREAMING TABLE assets_sql
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking'    = 'true'
);

CREATE FLOW cdc_assets_sql AS AUTO CDC INTO assets_sql
FROM STREAM(v_hexagon_assets)
KEYS (INSTANCE, ASSET_ID, WORK_BREAKDOWN_UP3_ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;

-- =============================================================================
-- Recon flow: hard-delete reconciliation from hexagon_assets_recon
-- =============================================================================

CREATE TEMPORARY VIEW v_recon_hexagon_assets AS
SELECT
  Instances_Id                                        AS INSTANCE,
  AssetId                                             AS ASSET_ID,
  WorkBreakdownUp3Id                                  AS WORK_BREAKDOWN_UP3_ID,
  if(hard_delete, false, cast(null as boolean))       AS ACTIVE_FLAG,
  dt_removed                                          AS DT_REMOVED,
  ingested_at                                         AS _scd_sequence,
  current_timestamp()                                 AS load_timestamp
FROM STREAM(${catalog}.${bronze_schema}.hexagon_assets_recon) WITH (SKIPCHANGECOMMITS);

CREATE FLOW recon_assets_sql
AS AUTO CDC INTO assets_sql
FROM STREAM(v_recon_hexagon_assets)
KEYS (INSTANCE, ASSET_ID, WORK_BREAKDOWN_UP3_ID)
IGNORE NULL UPDATES
SEQUENCE BY _scd_sequence
COLUMNS * EXCEPT (load_timestamp, _scd_sequence)
STORED AS SCD TYPE 1;
