/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from CSV files into bronze tables.

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE PROCEDURE bronze.load_bronze()
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
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '=====================================================================================';

    BEGIN
        RAISE NOTICE '--------------------------------------------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '---------------------------------------------------------------------------------------';

        -- CRM Customer Info
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISE NOTICE 'Inserting Data Into: bronze.crm_cust_info';
        COPY bronze.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        FROM 'D:source_crm\cust_info.csv'
        CSV HEADER;

        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM end_time - start_time);
        RAISE NOTICE '>> Load Duration: % Seconds', duration_seconds;
        RAISE NOTICE '-------------------------------------------------------------------------------------------------------';

        -- CRM Product Info
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISE NOTICE 'Inserting Data Into: bronze.crm_prd_info';
        COPY bronze.crm_prd_info(prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        FROM 'D:source_crm\prd_info.csv'
        CSV HEADER;

        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM end_time - start_time);
        RAISE NOTICE '>> Load Duration: % Seconds', duration_seconds;
        RAISE NOTICE '-------------------------------------------------------------------------------------------------------';

        -- CRM Sales Details
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISE NOTICE 'Inserting Data Into: bronze.crm_sales_details';
        COPY bronze.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        FROM 'D:source_crm\sales_details.csv'
        CSV HEADER;

        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM end_time - start_time);
        RAISE NOTICE '>> Load Duration: % Seconds', duration_seconds;
        RAISE NOTICE '-------------------------------------------------------------------------------------------------------';

        RAISE NOTICE '--------------------------------------------------------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '---------------------------------------------------------------------------------------';

        -- ERP Customer Info
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.erp_cus_az12';
        TRUNCATE TABLE bronze.erp_cus_az12;

        RAISE NOTICE 'Inserting Data Into: bronze.erp_cus_az12';
        COPY bronze.erp_cus_az12(CID, BDATE, GEN)
        FROM 'D:source_erp\CUST_AZ12.csv'
        CSV HEADER;

        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM end_time - start_time);
        RAISE NOTICE '>> Load Duration: % Seconds', duration_seconds;
        RAISE NOTICE '-------------------------------------------------------------------------------------------------------';

        -- ERP Location Info
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISE NOTICE 'Inserting Data Into: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101(CID, CNTRY)
        FROM 'D:source_erp\LOC_A101.csv'
        CSV HEADER;

        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM end_time - start_time);
        RAISE NOTICE '>> Load Duration: % Seconds', duration_seconds;
        RAISE NOTICE '-------------------------------------------------------------------------------------------------------';

        -- ERP Product Category Info
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE 'Inserting Data Into: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2(ID, CAT, SUBCAT, MAINTENANCE)
        FROM 'D:source_erp\PX_CAT_G1V2.csv'
        CSV HEADER;

        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM end_time - start_time);
        RAISE NOTICE '>> Load Duration: % Seconds', duration_seconds;
        RAISE NOTICE '-------------------------------------------------------------------------------------------------------';

        batch_end_time := CURRENT_TIMESTAMP;
        batch_duration := EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
        RAISE NOTICE '========================================================================================================';
        RAISE NOTICE 'Loading Bronze Layer is Completed';
        RAISE NOTICE '>> Total Batch Duration: % Seconds', batch_duration;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '=====================================================================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
            RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
            RAISE NOTICE 'ERROR MESSAGE: %', SQLERRM;
            RAISE NOTICE '=====================================================================================';
    END;
END;
$$;


-- To drop the procedure:
DROP PROCEDURE IF EXISTS bronze.load_bronze();

-- To execute the procedure:
CALL bronze.load_bronze();



