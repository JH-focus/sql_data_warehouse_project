INSERT INTO silver.erp_loc_a101(
	erp_cust_id,
	erp_cust_country
)
SELECT
REPLACE(erp_cust_id, '-', '') AS erp_cust_id,
CASE WHEN TRIM(erp_cust_country) = 'DE' THEN 'Germany'
	WHEN TRIM(erp_cust_country) IN ('US', 'USA') THEN ' United States'
	WHEN TRIM(erp_cust_country) = '' OR erp_cust_country IS NULL THEN 'Unknown'
	ELSE TRIM(erp_cust_country)
END AS new_erp_cust_country
FROM bronze.erp_loc_a101;
