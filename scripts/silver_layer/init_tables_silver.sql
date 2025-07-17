/*
This Script is responsible for setting up the tables of the silver layer in the database
'datawarehouse'. It uses the bronze layer tables as source data
crm_cust_info
crm_prod_info
crm_sales_details

erp_cust_az12
erp_loc_a101
erp_px_cat_g1v2
*/

CREATE TABLE IF NOT EXISTS silver.crm_cust_info (
  cst_id INT,
  cst_key VARCHAR(50),
  cst_firstname VARCHAR(50),
  cst_lastname VARCHAR(50),
  cst_marital_status VARCHAR(50),
  cst_gndr VARCHAR(50),
  cst_create_date DATE,
  dwh_create_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.crm_prod_info (
  prod_id INT,
  cat_id VARCHAR(50),
  prod_key VARCHAR(50),
  prod_nm VARCHAR(50),
  prod_cost INT,
  prod_line VARCHAR(50),
  prod_start_date DATE,
  prod_end_date DATE,
  dwh_create_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.crm_sales_details (
  sls_ord_num VARCHAR(50),
  sls_product_key VARCHAR(50),
  sls_cust_id INT,
  sls_order_date DATE,
  sls_ship_date DATE,
  sls_due_date DATE,
  sls_sales INT,
  sls_quantity INT,
  sls_price INT,
  dwh_create_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.erp_cust_az12 (
  erp_cust_id VARCHAR(50),
  erp_date_of_birth VARCHAR(50),
  erp_gender VARCHAR(50),
  dwh_create_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.erp_loc_a101 (
  erp_cust_id VARCHAR(50),
  erp_cust_country VARCHAR(50),
  dwh_create_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2 (
  erp_cust_id VARCHAR(50),
  erp_prod_category VARCHAR(50),
  erp_prod_sub_category VARCHAR(50),
  erp_prod_maintenance VARCHAR(50),
  dwh_create_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
