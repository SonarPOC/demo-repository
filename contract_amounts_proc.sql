CREATE PROC [trans_entitlement_d1_con].[contract_amounts_proc] @pipeline_name [VARCHAR](100),@pipeline_run_id [VARCHAR](100),@pipeline_trigger_name [VARCHAR](100),@pipeline_trigger_id [VARCHAR](100),@pipeline_trigger_type [VARCHAR](100),@pipeline_trigger_date_time_utc [DATETIME2], @ingest_partition [VARCHAR](100), @ingest_channel [VARCHAR](100), @file_path [VARCHAR](100), @root_path [VARCHAR](100) AS

BEGIN TRY

-- Creating External Table
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
    CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
    WITH ( FORMAT_TYPE = PARQUET)


IF EXISTS (SELECT 1 FROM sys.tables WHERE SCHEMA_NAME(schema_id) = 'trans_entitlement_d1_con' AND name = 'contract_amounts_temp' )
BEGIN
    DROP EXTERNAL TABLE [trans_entitlement_d1_con].[contract_amounts_temp]
END

DECLARE @sql_copy_into NVARCHAR(MAX)
SET     @sql_copy_into = '
CREATE EXTERNAL TABLE trans_entitlement_d1_con.contract_amounts_temp (
	[f_tran_code] nvarchar(4000),
[f_as_of_date] nvarchar(4000),
[f_as_of_time] nvarchar(4000),
[f_source_country_code] nvarchar(4000),
[f_instance_id] nvarchar(4000),
[f_iffile_id] nvarchar(4000),
[f_adj_amt] nvarchar(4000),
[f_customer_nbr] nvarchar(4000),
[f_cs_template_code] nvarchar(4000),
[f_cs_contract_nbr] nvarchar(4000),
[f_table_index] nvarchar(4000),
[f_adj_order_nbr] nvarchar(4000),
[f_cs_contract_line_nbr] nvarchar(4000),
[f_adj_type_code] nvarchar(4000),
[f_qty] nvarchar(4000),
[f_cs_customer_loc_set_code] nvarchar(4000),
[f_pricing_adj_name] nvarchar(4000),
[f_class_code] nvarchar(4000),
[f_offering_id] nvarchar(4000),
[f_adj_amt_fgn] nvarchar(4000),
[f_currency_code] nvarchar(4000)

    )
	WITH (
    LOCATION = ''' + @root_path + '/' + @ingest_partition +''',
	DATA_SOURCE = [stg_adledevadls2storage_dfs_core_windows_net],
    FILE_FORMAT = [SynapseParquetFormat]
    )
;
'

EXEC sp_executesql @sql_copy_into;

--LOAD-TYPE: Incremental temp2trans
--DECLARE @instance_id varchar(4) = '8702'; @pipeline_trigger_date_time_utc [DATETIME2]
WITH gen_hashkey as (
 SELECT
NULLIF([f_tran_code],'') as [f_tran_code],
NULLIF([f_as_of_date],'') as [f_as_of_date],
NULLIF([f_as_of_time],'') as [f_as_of_time],
NULLIF([f_source_country_code],'') as [f_source_country_code],
NULLIF([f_instance_id],'') as [f_instance_id],
NULLIF([f_iffile_id],'') as [f_iffile_id],
NULLIF([f_adj_amt],'') as [f_adj_amt],
NULLIF([f_customer_nbr],'') as [f_customer_nbr],
NULLIF([f_cs_template_code],'') as [f_cs_template_code],
NULLIF([f_cs_contract_nbr],'') as [f_cs_contract_nbr],
NULLIF([f_table_index],'') as [f_table_index],
NULLIF([f_adj_order_nbr],'') as [f_adj_order_nbr],
NULLIF([f_cs_contract_line_nbr],'') as [f_cs_contract_line_nbr],
NULLIF([f_adj_type_code],'') as [f_adj_type_code],
NULLIF([f_qty],'') as [f_qty],
NULLIF([f_cs_customer_loc_set_code],'') as [f_cs_customer_loc_set_code],
NULLIF([f_pricing_adj_name],'') as [f_pricing_adj_name],
NULLIF([f_class_code],'') as [f_class_code],
NULLIF([f_offering_id],'') as [f_offering_id],
NULLIF([f_adj_amt_fgn],'') as [f_adj_amt_fgn],
NULLIF([f_currency_code],'') as [f_currency_code],



 --@instance_id as [instance_id],
 
 --[infa_sortable_sequence] as [infa_sortable_sequence],
 @ingest_partition as [ingest_partition],
 @ingest_channel as [ingest_channel],
 @file_path as [file_path],
 @root_path as [root_path],
CAST(hashbytes('sha2_256', concat(upper(trim(coalesce(f_tran_code, ''))),'||')) as VARBINARY(32)) as [hash_key]

FROM [trans_entitlement_d1_con].[contract_amounts_temp]		--temp table
)
,rn as (
 SELECT *, ROW_NUMBER() OVER (PARTITION BY hash_key ORDER BY [f_tran_code] DESC)--, infa_sortable_sequence DESC)
 as _ELT_ROWNUMBERED
 FROM gen_hashkey),
data as (
 SELECT *
 FROM rn
 WHERE _ELT_ROWNUMBERED = 1
)
MERGE INTO [trans_entitlement_d1_con].[contract_amounts] tgt			--main table
USING (
 SELECT *
 FROM data
) src
ON ( src.[hash_key] = tgt.[hash_key] )
WHEN MATCHED THEN
UPDATE SET
	--add own table cols below.
	tgt.[f_tran_code] = src.[f_tran_code],
tgt.[f_as_of_date] = src.[f_as_of_date],
tgt.[f_as_of_time] = src.[f_as_of_time],
tgt.[f_source_country_code] = src.[f_source_country_code],
tgt.[f_instance_id] = src.[f_instance_id],
tgt.[f_iffile_id] = src.[f_iffile_id],
tgt.[f_adj_amt] = src.[f_adj_amt],
tgt.[f_customer_nbr] = src.[f_customer_nbr],
tgt.[f_cs_template_code] = src.[f_cs_template_code],
tgt.[f_cs_contract_nbr] = src.[f_cs_contract_nbr],
tgt.[f_table_index] = src.[f_table_index],
tgt.[f_adj_order_nbr] = src.[f_adj_order_nbr],
tgt.[f_cs_contract_line_nbr] = src.[f_cs_contract_line_nbr],
tgt.[f_adj_type_code] = src.[f_adj_type_code],
tgt.[f_qty] = src.[f_qty],
tgt.[f_cs_customer_loc_set_code] = src.[f_cs_customer_loc_set_code],
tgt.[f_pricing_adj_name] = src.[f_pricing_adj_name],
tgt.[f_class_code] = src.[f_class_code],
tgt.[f_offering_id] = src.[f_offering_id],
tgt.[f_adj_amt_fgn] = src.[f_adj_amt_fgn],
tgt.[f_currency_code] = src.[f_currency_code],



	--tgt.[instance_id] = src.[instance_id],
	tgt.[ingest_partition] = src.[ingest_partition],
	tgt.[ingest_channel] = src.[ingest_channel],
	tgt.[file_path] = src.[file_path],
	tgt.[root_path] = src.[root_path],
	tgt.[trans_load_date_time_utc] = GETDATE(),
	
	tgt.[pipeline_name] = @pipeline_name,
	tgt.[pipeline_run_id] = @pipeline_run_id,
	tgt.[pipeline_trigger_name] = @pipeline_trigger_name,
	tgt.[pipeline_trigger_id] = @pipeline_trigger_id,
	tgt.[pipeline_trigger_type] = @pipeline_trigger_type,
	tgt.[pipeline_trigger_date_time_utc] = @pipeline_trigger_date_time_utc
	
WHEN NOT MATCHED THEN
 INSERT (
	[f_tran_code],
[f_as_of_date],
[f_as_of_time],
[f_source_country_code],
[f_instance_id],
[f_iffile_id],
[f_adj_amt],
[f_customer_nbr],
[f_cs_template_code],
[f_cs_contract_nbr],
[f_table_index],
[f_adj_order_nbr],
[f_cs_contract_line_nbr],
[f_adj_type_code],
[f_qty],
[f_cs_customer_loc_set_code],
[f_pricing_adj_name],
[f_class_code],
[f_offering_id],
[f_adj_amt_fgn],
[f_currency_code],


	
	--[instance_id],
	[hash_key],
	[ingest_partition],
	[ingest_channel],
	[file_path],
	[root_path],
	[trans_load_date_time_utc],
	--[adle_transaction_code],
	[pipeline_name],
	[pipeline_run_id],
	[pipeline_trigger_name],
	[pipeline_trigger_id],
	[pipeline_trigger_type],
	[pipeline_trigger_date_time_utc]
 )
 VALUES (
	[src].[f_tran_code],
[src].[f_as_of_date],
[src].[f_as_of_time],
[src].[f_source_country_code],
[src].[f_instance_id],
[src].[f_iffile_id],
[src].[f_adj_amt],
[src].[f_customer_nbr],
[src].[f_cs_template_code],
[src].[f_cs_contract_nbr],
[src].[f_table_index],
[src].[f_adj_order_nbr],
[src].[f_cs_contract_line_nbr],
[src].[f_adj_type_code],
[src].[f_qty],
[src].[f_cs_customer_loc_set_code],
[src].[f_pricing_adj_name],
[src].[f_class_code],
[src].[f_offering_id],
[src].[f_adj_amt_fgn],
[src].[f_currency_code],


	--[src].[instance_id],
	[src].[hash_key],
	[src].[ingest_partition],
	[src].[ingest_channel],
	[src].[file_path],
	[src].[root_path],
	GETDATE(),
    --[src].[adle_transaction_code],
	
	
	@pipeline_name,
	@pipeline_run_id,
	@pipeline_trigger_name,
	@pipeline_trigger_id,
	@pipeline_trigger_type,
	@pipeline_trigger_date_time_utc
 );
END TRY
BEGIN CATCH
 DECLARE @db_name VARCHAR(200),
 @schema_name VARCHAR(200),
 @error_nbr INT,
 @error_severity INT,
 @error_state INT,
 @stored_proc_name VARCHAR(200),
 @error_message VARCHAR(8000),
 @created_date_time DATETIME2
 SET @db_name=DB_NAME()
 SET @schema_name=SUBSTRING (@pipeline_name, CHARINDEX('2', @pipeline_name) + 1, LEN(@pipeline_name) - CHARINDEX('2', @pipeline_name) - 3 )
 SET @error_nbr=ERROR_NUMBER()
 SET @error_severity=ERROR_SEVERITY()
 SET @error_state=ERROR_STATE()
 SET @stored_proc_name=ERROR_PROCEDURE()
 SET @error_message=ERROR_MESSAGE()
 SET @created_date_time=GETDATE()
 EXECUTE [adle_platform_orchestration].[elt_error_log_proc]
 @db_name,
 'ERROR',
 @schema_name,
 @error_nbr,
 @error_severity,
 @error_state,
 @stored_proc_name,
 'PROC',
 @error_message,
 @created_date_time,
 @pipeline_name,
 @pipeline_run_id,
 @pipeline_trigger_name,
 @pipeline_trigger_id,
 @pipeline_trigger_type,
 @pipeline_trigger_date_time_utc
 ;
 THROW;
END CATCH
;
GO