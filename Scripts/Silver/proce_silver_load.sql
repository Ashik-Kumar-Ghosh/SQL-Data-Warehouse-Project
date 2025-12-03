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
    CALL Silver.load_silver();
===============================================================================
*/

CREATE PROCEDURE Silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration_seconds INTEGER;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    batch_duration INTEGER;
BEGIN
    batch_start_time := CURRENT_TIMESTAMP;

    RAISE NOTICE '=====================================================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '=====================================================================================';

	BEGIN
        RAISE NOTICE '--------------------------------------------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '---------------------------------------------------------------------------------------';


		-- CRM Customer Info
		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating Table : Silver.crm_cust_info';
		Truncate Table Silver.crm_cust_info;
		
		RAISE NOTICE '>> Inserting Data Into: Silver.crm_cust_info';
		INSERT INTO Silver.crm_cust_info(
		cst_id, 
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
		
		SELECT cst_id,cst_key,
		TRIM(cst_firstname) AS cst_firstname, 
		TRIM(cst_lastname) AS cst_lastname,
		CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'     --APPLY TRIM() JUST IN CASE SPACES APPEAR LATER IN YOUR COLUMN
		ELSE 'n/a'
		END AS cst_marital_status,
		CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'     --APPLY TRIM() JUST IN CASE SPACES APPEAR LATER IN YOUR COLUMN
		ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date FROM(
		SELECT * , ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as Flag_last   ----USED ROW_NUMBER: TO GET THE LATEST DATE VALUE BY ORDER BY DESC
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL) 
		WHERE flag_last = 1;
		
		end_time := CURRENT_TIMESTAMP;
		        duration_seconds := EXTRACT(EPOCH FROM end_time - start_time);
		        RAISE NOTICE '>> Load Duration: % Seconds', duration_seconds;
		        RAISE NOTICE '-------------------------------------------------------------------------------------------------------';
		

		-- CRM Product Info
        start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating Table: Silver.crm_prd_info';
		TRUNCATE TABLE Silver.crm_prd_info;
		
		RAISE NOTICE '>>INSERTING DATA INTO: Silver.crm_prd_info';
		INSERT INTO Silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, 
		SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,      
		prd_nm,
		COALESCE(prd_cost, '0') AS prd_cost,                       
		CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
		END AS prd_line,       -- MAP PRODUCT LINE CODES TO DESCRIPTIVE VALUES
		prd_start_dt,
		LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt
		FROM Bronze.crm_prd_info;

		end_time := CURRENT_TIMESTAMP;
		        duration_seconds := EXTRACT(EPOCH FROM end_time - start_time);
		        RAISE NOTICE '>> Load Duration: % Seconds', duration_seconds;
		        RAISE NOTICE '-------------------------------------------------------------------------------------------------------';
		

			-- CRM Sales Details
        start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating Table: Silver.crm_sales_details';
		
		TRUNCATE TABLE Silver.crm_sales_details;
		RAISE NOTICE '>>INSERTING DATA INTO: Silver.crm_sales_details';
		INSERT INTO Silver.crm_sales_details(
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
		CASE
		WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE
		WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE
		WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN ABS(sls_quantity) * ABS(sls_price)
		ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE
		WHEN sls_price IS NULL OR sls_price != 0
			THEN ABS(sls_sales )/ NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details;


		RAISE NOTICE '--------------------------------------------------------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '---------------------------------------------------------------------------------------';


			-- ERP Customer Info
        start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating Table: Silver.erp_cust_az12';
		TRUNCATE TABLE Silver.erp_cust_az12;
		RAISE NOTICE '>>INSERTING DATA INTO: Silver.erp_cust_az12';
		INSERT INTO Silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, length(cid))   
			ELSE cid
			END AS cid,
		CASE WHEN bdate > CURRENT_DATE THEN NULL
			ELSE bdate
			END AS bdate,                                              
		CASE
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
			END AS gen                                                  
		FROM  bronze.erp_cust_az12;

		end_time := CURRENT_TIMESTAMP;
		        duration_seconds := EXTRACT(EPOCH FROM end_time - start_time);
		        RAISE NOTICE '>> Load Duration: % Seconds', duration_seconds;
		        RAISE NOTICE '-------------------------------------------------------------------------------------------------------';


		-- ERP Location Info
        start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating Table: Silver.erp_loc_a101';
		TRUNCATE TABLE Silver.erp_loc_a101;
		
		RAISE NOTICE '>>INSERTING DATA INTO: Silver.erp_loc_a101';
		INSERT INTO Silver.erp_loc_a101(
		cid,
		cntry
		)
		SELECT
		REPLACE(cid, '-', '') as cid,
		CASE
			WHEN (TRIM(cntry)) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
			END AS cntry                                 
		FROM bronze.erp_loc_a101;

		end_time := CURRENT_TIMESTAMP;
		        duration_seconds := EXTRACT(EPOCH FROM end_time - start_time);
		        RAISE NOTICE '>> Load Duration: % Seconds', duration_seconds;
		        RAISE NOTICE '-------------------------------------------------------------------------------------------------------';


		-- ERP Product Category Info
        start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating Table: Silver.erp_px_cat_g1v2';
		TRUNCATE TABLE Silver.erp_px_cat_g1v2;
		
		RAISE NOTICE '>>INSERTING DATA INTO:  Silver.erp_px_cat_g1v2';
		INSERT INTO Silver.erp_px_cat_g1v2
		(id, cat, subcat, maintenance)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;

		end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM end_time - start_time);
        RAISE NOTICE '>> Load Duration: % Seconds', duration_seconds;
        RAISE NOTICE '-------------------------------------------------------------------------------------------------------';

        batch_end_time := CURRENT_TIMESTAMP;
        batch_duration := EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
        RAISE NOTICE '========================================================================================================';
        RAISE NOTICE 'Loading Silver Layer is Completed';
        RAISE NOTICE '>> Total Batch Duration: % Seconds', batch_duration;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '=====================================================================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
            RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
            RAISE NOTICE 'ERROR MESSAGE: %', SQLERRM;
            RAISE NOTICE '=====================================================================================';
    END;
END;
$$;



-- To execute the procedure:
CALL bronze.load_bronze();
















