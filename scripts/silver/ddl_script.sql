/* This script creates table for the silver layer, replaces table if they already exists. 
Run this to redefine the ddl structures of the bronze layer*/

--ddl for silver layer

create or replace table silver.crm_cust_info (
cst_id varchar(50),
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(50),
cst_gndr varchar(50),
cst_create_date date,
dwh_create_date datetime default getdate()
)
select * from silver.crm_cust_info

create or replace table silver.crm_prd_info(
prd_id varchar(50),
cat_id varchar(50),
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime default getdate()
)
select * from silver.crm_prd_info


create or replace table silver.crm_sales_details (
    sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id varchar(50),
    sls_order_dt date,
    sls_due_dt date,
    sls_ship_dt date,
    sls_sales float,
    sls_quantity int,
    sls_price float,
    dwh_create_date datetime default getdate()
)
select * from silver.crm_sales_details 


create or replace table silver.erp_CUST_AZ12 (
CID varchar(50),
BDATE DATE,
GEN varchar(50),
dwh_create_date datetime default getdate()
)
select * from  silver.erp_CUST_AZ12
  

create or replace table silver.erp_LOC_A101 (
CID varchar(50),
CNTRY varchar(50),
dwh_create_date datetime default getdate()
)
select * from silver.erp_LOC_A101

create or replace table silver.erp_PX_CAT_GIV2 (
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTAINANCE varchar(50),
dwh_create_date datetime default getdate()

)
select * from silver.erp_PX_CAT_GIV2
