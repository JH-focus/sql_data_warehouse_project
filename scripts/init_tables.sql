/*
This Script is responsible for setting up the tables in the database
'datawarehouse'. It uses cust_info.csv, prod_info.csv and sales_details.csv
as Source Data
*/

CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
  cst_id INT,
  cst_key VARCHAR(50),
  cst_firstname VARCHAR(50),
  cst_lastname VARCHAR(50),
  cst_marital_status VARCHAR(50),
  cst_gndr VARCHAR(50),
  cst_create_date DATE
);


CREATE TABLE IF NOT EXISTS bronze.crm_prod_info (
  prod_id INT,
  prod_key VARCHAR(50),
  prod_nm VARCHAR(50),
  prod_cost INT,
  prod_line VARCHAR(50),
  prod_start_date TIMESTAMP,
  prod_end_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
  sls_ord_num VARCHAR(50),
  sls_product_key VARCHAR(50),
  sls_cust_id INT,
  sls_order_date DATE,
  sls_ship_date DATE,
  sls_due_date DATE,
  sls_sales INT,
  sls_quantity INT,
  sls_price INT
);

CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
  erp_cust_id VARCHAR(50),
  erp_date_of_birth VARCHAR(50),
  erp_gender VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
  erp_cust_id VARCHAR(50),
  erp_cust_country VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
  erp_cust_id VARCHAR(50),
  erp_prod_category VARCHAR(50),
  erp_prod_sub_category VARCHAR(50),
  erp_prod_maintenance VARCHAR(50)
);
