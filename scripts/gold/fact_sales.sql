/*This scripts builds the sales fact table by integratin the foreiggn keys though joins and ensuring referential integrity*/
--building sales fact table
 create or replace view gold.fact_sales as 
 select 
    sls_ord_num as order_number,
    c.customerr_key,
    p.product_key,
    sls_order_dt as order_date,
    sls_due_dt as due_date,
    sls_ship_dt as ship_date,
    sls_sales as sales_amount,
    sls_quantity as quantity,
    sls_price as price
 from silver.crm_sales_details s
 left join gold.dim_customers c
 on s.sls_cust_id = c.customer_id
 left join gold.dim_products p
 on s.sls_prd_key = p.product_number

select * from gold.fact_sales
