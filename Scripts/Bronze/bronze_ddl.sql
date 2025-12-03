/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

DROP TABLE IF EXISTS Bronze.crm_cust_info;

CREATE TABLE Bronze.crm_cust_info(
cst_id INT ,
cst_key VARCHAR(100) ,
cst_firstname VARCHAR(100),
cst_lastname VARCHAR(100),
cst_marital_status VARCHAR(10),
cst_gndr VARCHAR(10),
cst_create_date DATE
);

---------------------

DROP TABLE IF EXISTS Bronze.crm_prd_info;

CREATE TABLE Bronze.crm_prd_info(
prd_id INT ,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost VARCHAR(10),
prd_line VARCHAR(10),
prd_start_dt DATE,
prd_end_dt DATE
);
----------------------

DROP TABLE IF EXISTS Bronze.crm_sales_details;

CREATE TABLE Bronze.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT NOT NULL,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INTEGER,
sls_price NUMERIC(10,2)
);
------------------------

DROP TABLE IF EXISTS Bronze.erp_cust_az12;

CREATE TABLE Bronze.erp_cust_az12(
CID VARCHAR(100),
BDATE DATE,
GEN VARCHAR(10)
);
-------------------------

DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(
CID VARCHAR(50),
CNTRY VARCHAR(50)
);
-------------------------

DROP TABLE IF EXISTS Bronze.erp_px_cat_g1v2;

CREATE TABLE Bronze.erp_px_cat_g1v2(
ID VARCHAR(10),
CAT VARCHAR(50),
SUBCAT VARCHAR(50),
MAINTENANCE VARCHAR(10)
);



































