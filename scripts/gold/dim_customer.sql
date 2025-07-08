/* This script builds the star schema model by creating both customer and product dimensions.
by joining the ERP customer and product info and CRM customer and product info.
it also creates the sales fact table by joining foreign keys from dimensions ensuring referential integrity */

--create the customer dimension

create view  gold.dim_customers as
select 
    row_number() over (order by cu.cst_id) as customerr_key,
    cu.cst_id as customer_id,
    cu.cst_key as customer_number,
    cu.cst_firstname as first_name,
    cu.cst_lastname as last_name,
    cu.cst_marital_status as marital_status,
    case
        when cu.cst_gndr != 'n/a' then cu.cst_gndr
      else coalesce(ca.gen,'n/a')
    end as gender,
     la.cntry as country,
    ca.bdate as birth_date,
    cu.cst_create_date as created_date 
from silver.crm_cust_info cu   
left join silver.erp_cust_az12 ca ---joining both erp_cust_az12 and erp_loc_a101 to crm_cust_info
on cu.cst_key = ca.cid
left join silver.erp_loc_a101 la
on cu.cst_key = la.cid
  
select * from gold.dim_customers
