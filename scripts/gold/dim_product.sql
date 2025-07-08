/* This script creates the product dimennsion by joining erp_px_cat_giv2 to the crm_prd_info.
creates foreign key for this dimension and ensures it is unique*/
create view gold.dim_products as 
select 
    row_number() over (order by  p.prd_start_dt, p.prd_key) as product_key,
    p.prd_id as product_id,
    p.prd_key as product_number,
    p.prd_nm as product_name,
    p.cat_id as category_id,
    c.cat as category,
   c.subcat as subcategory,
   c.maintainance as maintainance,
    p.prd_cost as cost,
    p.prd_line as product_line,
   p.prd_start_dt as start_date
from silver.crm_prd_info p
left join silver.erp_px_cat_giv2 c    ///filter out historical data
on p.cat_id = c.id
where p.prd_end_dt is null

select * from gold.dim_products
