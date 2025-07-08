/*
  ===================================================================================================================
  Creating Database and Schema
  ====================================================================================================================
  *Script Purpose**: This script creates or if already exists, replaces database and schema.
  The schemas to be used in this project are Bronze, Silver and Gold schemas, each representing a layer of the data flow
  
**Warning**: This script will replace the entire warehouse if it already exists,
therefore, caution mjust be applied before running scripts and ensure backup files.*/

---create warehouse and schemas
create or replace  database project_WH
use database project_WH

create schema bronze;
create schema silver;
create schema gold;

use schema bronze
