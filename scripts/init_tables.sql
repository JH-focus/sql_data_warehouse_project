/*
This Script is responsible for setting up the tables in the database
'datawarehouse'. It uses cust_info.csv, prod_info.csv and sales_details.csv
as Source Data
*/

CREATE TABLE bronze.crm_cust_info (
  cst_id INT,
  cst_key VARCHAR(50),
  cst_firstname VARCHAR(50),
  cst_lastname VARCHAR(50),
  cst_marital_status VARCHAR(1),
  cst_gndr VARCHAR(1),
  cst_create_date DATE
);
