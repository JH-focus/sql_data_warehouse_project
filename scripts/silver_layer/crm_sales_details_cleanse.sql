INSERT INTO silver.crm_sales_details(	
	sls_ord_num,
	sls_product_key,
	sls_cust_id,
	sls_order_date,
	sls_ship_date,
	sls_due_date,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT
sls_ord_num,
sls_product_key,
sls_cust_id,
CASE WHEN sls_order_date = '1970-01-01' OR LENGTH(CAST(sls_order_date AS VARCHAR)) != 10 THEN NULL
	ELSE sls_order_date
END AS sls_order_date,
CASE WHEN sls_ship_date = '1970-01-01' OR LENGTH(CAST(sls_ship_date AS VARCHAR)) != 10 THEN NULL
	ELSE sls_ship_date
END AS sls_ship_date,
CASE WHEN sls_due_date = '1970-01-01' OR LENGTH(CAST(sls_due_date AS VARCHAR)) != 10 THEN NULL
	ELSE sls_due_date
END AS sls_due_date,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;

SELECT * FROM silver.crm_sales_details;
