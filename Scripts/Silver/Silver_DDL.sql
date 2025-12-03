/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/


DROP TABLE IF EXISTS Silver.crm_cust_info;
CREATE TABLE Silver.crm_cust_info(
cst_id INT ,
cst_key VARCHAR(100) ,
cst_firstname VARCHAR(100),
cst_lastname VARCHAR(100),
cst_marital_status VARCHAR(10),
cst_gndr VARCHAR(10),
cst_create_date DATE,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

---------------------

DROP TABLE IF EXISTS Silver.crm_prd_info;
CREATE TABLE Silver.crm_prd_info(
prd_id INT ,
cat_id VARCHAR(50),
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost VARCHAR(50),
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
----------------------

DROP TABLE IF EXISTS Silver.crm_sales_details;
CREATE TABLE Silver.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT NOT NULL,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INTEGER,
sls_quantity INTEGER,
sls_price NUMERIC(10,2),
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT * FROM Bronze.crm_sales_details

DROP TABLE IF EXISTS Silver.erp_cus_az12;
CREATE TABLE Silver.erp_cus_az12(
CID VARCHAR(100),
BDATE DATE,
GEN VARCHAR(10),
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS Silver.erp_loc_a101;
CREATE TABLE Silver.erp_loc_a101(
CID VARCHAR(50),
CNTRY VARCHAR(50),
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS Silver.erp_px_cat_g1v2;
CREATE TABLE Silver.erp_px_cat_g1v2(
ID VARCHAR(10),
CAT VARCHAR(50),
SUBCAT VARCHAR(50),
MAINTENANCE VARCHAR(10),
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

















































































































