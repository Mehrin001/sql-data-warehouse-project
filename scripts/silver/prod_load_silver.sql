
/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE sliver.load_silver
AS
BEGIN
    DECLARE 
        @Batch_Start_Time DATETIME,
        @Batch_End_Time   DATETIME,
        @Start_Time       DATETIME,
        @End_Time         DATETIME;

    BEGIN TRY
        SET @Batch_Start_Time = GETDATE();

        PRINT '==================================================';
        PRINT '        STARTING SILVER LAYER LOAD PROCESS        ';
        PRINT '==================================================';

        /* ==================================================
           TABLE: sliver.crm_cust_info
        ================================================== */
        SET @Start_Time = GETDATE();
        PRINT '>> Loading Table : sliver.crm_cust_info';

        TRUNCATE TABLE sliver.crm_cust_info;

        INSERT INTO sliver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE rn = 1;

        SET @End_Time = GETDATE();
        PRINT '>> Time Taken (seconds): ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS VARCHAR);


        /* ==================================================
           TABLE: sliver.crm_prd_info
        ================================================== */
        SET @Start_Time = GETDATE();
        PRINT '>> Loading Table : sliver.crm_prd_info';

        TRUNCATE TABLE sliver.crm_prd_info;

        INSERT INTO sliver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
                AS DATE
            )
        FROM bronze.crm_prd_info;

        SET @End_Time = GETDATE();
        PRINT '>> Time Taken (seconds): ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS VARCHAR);


        /* ==================================================
           TABLE: sliver.crm_sales_details
        ================================================== */
        SET @Start_Time = GETDATE();
        PRINT '>> Loading Table : sliver.crm_sales_details';

        TRUNCATE TABLE sliver.crm_sales_details;

        INSERT INTO sliver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,
            CASE 
                WHEN sls_sales IS NULL 
                     OR sls_sales <= 0 
                     OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @End_Time = GETDATE();
        PRINT '>> Time Taken (seconds): ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS VARCHAR);


        /* ==================================================
           TABLE: sliver.erp_px_cat_g1v2
        ================================================== */
        SET @Start_Time = GETDATE();
        PRINT '>> Loading Table : sliver.erp_px_cat_g1v2';

        TRUNCATE TABLE sliver.erp_px_cat_g1v2;

        INSERT INTO sliver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @End_Time = GETDATE();
        PRINT '>> Time Taken (seconds): ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS VARCHAR);


        /* ==================================================
           TABLE: sliver.erp_cust_az12
        ================================================== */
        SET @Start_Time = GETDATE();
        PRINT '>> Loading Table : sliver.erp_cust_az12';

        TRUNCATE TABLE sliver.erp_cust_az12;

        INSERT INTO sliver.erp_cust_az12 (cid, cst_key, bdate, gen)
        SELECT
            cid,
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @End_Time = GETDATE();
        PRINT '>> Time Taken (seconds): ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS VARCHAR);


        /* ==================================================
           TABLE: sliver.erp_loc_a101
        ================================================== */
        SET @Start_Time = GETDATE();
        PRINT '>> Loading Table : sliver.erp_loc_a101';

        TRUNCATE TABLE sliver.erp_loc_a101;

        INSERT INTO sliver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        SET @End_Time = GETDATE();
        PRINT '>> Time Taken (seconds): ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS VARCHAR);

        SET @Batch_End_Time = GETDATE();

        PRINT '==================================================';
        PRINT ' SILVER LAYER LOAD COMPLETED SUCCESSFULLY ';
        PRINT ' TOTAL TIME (seconds): ' + CAST(DATEDIFF(SECOND, @Batch_Start_Time, @Batch_End_Time) AS VARCHAR);
        PRINT '==================================================';

    END TRY
    BEGIN CATCH
        PRINT '==================================================';
        PRINT ' ERROR OCCURRED DURING SILVER LAYER LOAD ';
        PRINT ' Error Message : ' + ERROR_MESSAGE();
        PRINT ' Error Number  : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT ' Error State   : ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '==================================================';
    END CATCH
END;
GO

EXEC sliver.load_silver;
