/*This script tansforms and loads data from the bronze layer to the silver layer. data cleaning, transformation
is carried out in this script
**Load Type is full load */

--inserting into silver.crm_cust_info
truncate table silver.crm_cust_info

insert into silver.crm_cust_info
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status,cst_gndr,cst_create_date)
select 
    cst_id,
    trim(cst_key)cst_key,
    trim(cst_firstname)cst_firstname,
    trim(cst_lastname)cst_lastname,
    trim( case 
            when cst_marital_status =upper('M') then 'Married'
            when cst_marital_status = upper('S') then 'Single'
          else 'n/a'
          end)cst_marital_status,
     trim(case 
           when cst_gndr =upper('M') then 'Male'
           when cst_gndr = upper('F') then 'Female'
          else 'n/a'
          end)cst_gndr,
     trim(cst_create_date)cst_create_date
from (select row_number() over (partition by cst_id order by cst_create_date desc) as ranks, *
      from bronze.crm_cust_info
      where cst_id is not null)t
where ranks =1 

--inserting into silver.crm_prd_info
  Tuncate table silver.crm_prd_info
  
insert into silver.crm_prd_info
  (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt) 

select
    prd_id,
    replace(substring(prd_key,1,5),'-','_') as cat_id,
    substring(prd_key,7,length(prd_key)) as prd_key,
    trim(prd_nm)prd_nm,
    coalesce(prd_cost,0) as prd_cost,
    case upper(trim(prd_line))
     when  'M' then 'Mountain'
      when 'T' then  'Touring'
      when 'S' then  'Other sales'
       when 'R' then  'Road'
    else 'n/a'
end as prd_line,
    cast(prd_start_dt as date),
    cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
from bronze.crm_prd_info


--inserting into silver.crm_sales_details
truncate silver.crm_sales_details

insert into silver.crm_sales_details 
  ( sls_ord_num,   sls_prd_key,sls_cust_id,sls_order_dt,sls_due_dt,sls_ship_dt,sls_sales,sls_quantity,sls_price)
select 
    sls_ord_num, 
    sls_prd_key,
    sls_cust_id,
   case 
      when sls_order_dt <=0 
       or  length(sls_order_dt)!=8  or  sls_order_dt<19000101 or  sls_order_dt>20500101
        then null 
      else to_date (cast(sls_order_dt as varchar),'yyyymmdd')
    end as sls_order_dt, 
    case 
       when sls_due_dt <=0 
        or  length(sls_due_dt)!=8 or  sls_due_dt<19000101 or sls_due_dt>2050010
        then null 
        else to_date (cast(sls_due_dt as varchar),'yyyymmdd')
    end as sls_due_dt,
    case 
       when sls_ship_dt <=0 
        or  length(sls_ship_dt)!=8   or  sls_ship_dt<19000101 or sls_ship_dt>2050010
        then null 
        else to_date (cast(sls_ship_dt as varchar),'yyyymmdd')
    end as sls_ship_dt,
    case                  --recalculate sales if original value is missing or incorrect
       when sls_sales is null or sls_sales <=0 or sls_sales !=sls_quantity * abs(sls_price) 
        then sls_quantity * abs(sls_price)
        else sls_sales
    end as sls_sales,
    sls_quantity,
    case
       when sls_price  is null or sls_price <=0   
        then sls_sales / nullif(sls_quantity,0)
        else sls_price
    end as sls_price
from bronze.crm_sales_details


--inserting into silver.erp_cust_az12

 insert into silver.erp_cust_az12 
  (cid,bdate,gen)

 select 
    case 
        when cid like'NAS%' then substr(cid,4,length(cid)) //matching strings with cst_key from cust_info
         else cid
    end as cid ,
    case 
        when bdate >getdate() then null  //fixing out-of-range dates
         else bdate
    end as bdate,
    case 
       when trim(upper(gen )) in ('M','MALE') then 'Male'
        when trim(upper(gen )) in ('F','FEMALE') then 'Female' //standardizing low cardinality
      else 'n/a'
    end as gen
from bronze.erp_cust_az12


--inserting to erp_loc_a101
insert into silver.erp_loc_a101 (cid,cntry)

select 
    replace(trim(cid),'-','') as cid,
       case 
        when upper(trim(cntry)) in ('US','USA','UNITED STATES') then 'United States'
        when upper(trim(cntry)) in ('UK','UNITED KINGDOM') then 'United Kingdom'
        when upper(trim(cntry)) in ('GERMANY','DE') then 'Germany'
        when upper(trim(cntry)) is null  or upper(trim(cntry)) = '' then 'n/a'
        else trim(cntry)
      end as cntry
from bronze.erp_loc_a101

--inserting into silver.erp_px_cat_giv2
insert into silver.erp_px_cat_giv2 (id,cat,subcat,maintainance)

select
    id,
    trim(cat),
    trim(subcat),
    trim(maintainance)
from bronze.erp_px_cat_giv2
    

