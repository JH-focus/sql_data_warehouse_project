CREATE VIEW gold.dim_customers AS 
SELECT 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.erp_cust_country AS country,
ci.cst_marital_status AS marital_status,
CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr --CRM is the Master for gender info
ELSE COALESCE(ca.erp_gender, 'Unknown') 
END AS gender,
ca.erp_date_of_birth AS birthdate,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT OUTER JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.erp_cust_id
LEFT OUTER JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.erp_cust_id;

