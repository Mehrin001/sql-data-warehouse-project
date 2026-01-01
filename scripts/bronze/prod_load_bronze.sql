/*
==============================================================================================
Stored procedure : Load Bronze Layes (Sourse -> Bronze)
==============================================================================================
Script Purpose :
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the followimg actions :
      - Truncates the bronze tables before loading data
      - Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

 Parameters :
    None
    This stored procedure does not accept any parameter or return any values

 Usage Example :
  EXEC bronze.load_bronze
==============================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
  DECLARE @Start_Time DATETIME ,@End_Time DATETIME, @Batch_Start_Time DATETIME, @Batch_End_Time DATETIME
	BEGIN TRY
	    SET @Batch_Start_Time = GETDATE();
		PRINT '====================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '====================';

		PRINT '--------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------';

		SET @Start_Time = GETDATE();
		PRINT '>> Truncating table :  bronze.crm_cust_info_stage';
		TRUNCATE TABLE bronze.crm_cust_info_stage;

		PRINT '>> Inserting data into table :  bronze.crm_cust_info_stage';
		BULK INSERT bronze.crm_cust_info_stage
		FROM 'C:\Users\Mehrin\Documents\SQL Server Management Studio\Temp\sql-data-warehouse-project\datasets\source_crm\cust_info1.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @End_Time = GETDATE(); 
		PRINT '>> LOAD DURATION : bronze.crm_cust_info_stage :' + CAST(DATEDIFF(SECOND,@Start_Time ,@End_Time) AS NVARCHAR) + ' second';
	    PRINT '----------------------------------------------------------';


		PRINT '--------------------------------------------------------------------------------------------';
		PRINT '>> Inserting data into table :  bronze.crm_cust_info  from bronze.crm_cust_info_stage Table';
		PRINT '--------------------------------------------------------------------------------------------';
		INSERT INTO bronze.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			TRY_CONVERT(INT, cst_id),
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			CASE
				WHEN LTRIM(RTRIM(cst_create_date)) = '' THEN NULL
				ELSE TRY_CONVERT(DATE, cst_create_date, 103)  -- DD/MM/YYYY
			END
		FROM bronze.crm_cust_info_stage;
	
	 
	    SET @Start_Time = GETDATE();
		PRINT '>> Truncating table :  bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting data into table :  bronze.crm_prd_info';
		--INSERTING DATA IN TABLE "bronze.crm_prd_info"
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Mehrin\Documents\SQL Server Management Studio\Temp\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_Time = GETDATE(); 
		PRINT '>> LOAD DURATION : bronze.crm_prd_info : ' + CAST(DATEDIFF(SECOND,@Start_Time ,@End_Time) AS NVARCHAR) + ' second';
		PRINT '------------------------------------------------------------------------------------------------------------------';


		SET @Start_Time = GETDATE();
		PRINT '>> Truncating table :  bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		--INSERTING DATA IN TABLE "bronze.crm_sales_details"
		PRINT '>> Inserting data into table :  bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Mehrin\Documents\SQL Server Management Studio\Temp\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_Time = GETDATE(); 
		PRINT '>> LOAD DURATION : bronze.crm_sales_details :' + CAST(DATEDIFF(SECOND,@Start_Time ,@End_Time) AS NVARCHAR) + ' second';
	    PRINT '-----------------------------------------------------------------------------------'


		PRINT '--------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------';

		SET @Start_Time = GETDATE();
		PRINT '>> Truncating table :  bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting data into table :  bronze.erp_cust_az12';
		--INSERTING DATA IN TABLE "bronze.erp_cust_az12"
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Mehrin\Documents\SQL Server Management Studio\Temp\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_Time = GETDATE(); 
		PRINT '>> LOAD DURATION : bronze.erp_cust_az12 : ' + CAST(DATEDIFF(SECOND,@Start_Time ,@End_Time) AS NVARCHAR) + ' second';
		PRINT '------------------------------------------------------------------------------------------------------------------';

		SET @Start_Time = GETDATE();
		PRINT '>> Truncating table :  bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting data into table : bronze.erp_loc_a101';
		--INSERTING DATA IN TABLE "bronze.erp_cust_az12"
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Mehrin\Documents\SQL Server Management Studio\Temp\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_Time = GETDATE(); 
		PRINT '>> LOAD DURATION : bronze.erp_loc_a101 :' + CAST(DATEDIFF(SECOND,@Start_Time ,@End_Time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------------------------------------------------------------------------';

		SET @Start_Time = GETDATE();
		PRINT '>> Truncating table :  bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting data into table : bronze.erp_px_cat_g1v2';
		--INSERTING DATA IN TABLE "bronze.erp_px_cat_g1v2"
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Mehrin\Documents\SQL Server Management Studio\Temp\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_Time = GETDATE(); 
		PRINT '>> LOAD DURATION OF TABLE: bronze.erp_px_cat_g1v2' + CAST(DATEDIFF(SECOND,@Start_Time ,@End_Time) AS NVARCHAR) + ' second';
	    PRINT '-----------------------------------------------------------------------------------------------------------------------';

		SET @Batch_End_Time = GETDATE();
		PRINT '==================================================================================================';
		PRINT 'LOADING  BRONZE LAYER IS COMPLETED';
		PRINT 'TOTAL TIME :' + CAST(DATEDIFF(SECOND, @Batch_Start_Time,@Batch_End_Time) AS NVARCHAR) + ' SECONDS';
		PRINT '==================================================================================================';

	END TRY
	BEGIN CATCH
		PRINT '==================================================';
		PRINT 'ERROR OCCUERED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number'  + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State'  + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==================================================';		          
	END CATCH
END

EXEC bronze.load_bronze
