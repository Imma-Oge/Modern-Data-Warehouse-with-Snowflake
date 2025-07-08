
/*This DDL script creates stages which serve as a landing phase for the files
from where data is loaded into the first layer, bronze. also provuides ddl scripts for each table  */

/*creating internal staging area that serves as landing phase for each file */
create or replace stage bronze.erp_staging_area
create or replace stage bronze.crm_staging_area

list@bronze.crm_staging_area
list@bronze.erp_staging_area

  /*creating Tables for each file*/
create or replace table bronze.crm_cust_info (
cst_id varchar(50),
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(50),
cst_gndr varchar(50),
cst_create_date date
)

create or replace table bronze.crm_prd_info(
prd_id varchar(50),
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date
)

create or replace table bronze.crm_sales_details (
    sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id varchar(50),
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales float,
    sls_quantity int,
    sls_price float
)

create or replace table bronze.erp_CUST_AZ12 (
CID varchar(50),
BDATE DATE,
GEN varchar(50)
)

create or replace table bronze.erp_LOC_A101 (
CID varchar(50),
CNTRY varchar(50)
)

create or replace table bronze.erp_PX_CAT_GIV2 (
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTAINANCE varchar(50)
)

/*LOADING DATA INTO BRONZE TABLES FROM STAGING AREA- full load option*/
truncate table bronze.crm_cust_info
copy into bronze.crm_cust_info
from @bronze.crm_staging_area/cust_info.csv
file_format =(type ='csv' skip_header =1)

select * from bronze.crm_cust_info

truncate table bronze.crm_prd_info
copy into bronze.crm_prd_info
from @bronze.crm_staging_area/prd_info.csv
file_format =(type ='csv' skip_header =1)

select * from bronze.crm_prd_info

truncate table bronze.crm_sales_details
copy into bronze.crm_sales_details
from @bronze.crm_staging_area/sales_details.csv
file_format =(type ='csv' skip_header =1)

select * from bronze.crm_sales_details

truncate table bronze.erp_cust_az12
copy into bronze.erp_cust_az12
from @bronze.erp_staging_area/CUST_AZ12.csv
file_format =(type ='csv' skip_header =1)

select * from bronze.erp_cust_az12

truncate table bronze.erp_loc_a101
copy into bronze.erp_loc_a101
from @bronze.erp_staging_area/LOC_A101.csv
file_format =(type ='csv' skip_header =1)

select * from bronze.erp_loc_a101

truncate table  bronze.erp_px_cat_giv2
copy into bronze.erp_px_cat_giv2
from @bronze.erp_staging_area/PX_CAT_G1V2.csv
file_format =(type ='csv' skip_header =1)

select * from bronze.erp_px_cat_giv2

