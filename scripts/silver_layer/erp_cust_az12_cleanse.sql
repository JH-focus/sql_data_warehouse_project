INSERT INTO silver.erp_cust_az12(
	erp_cust_id,
	erp_date_of_birth,
	erp_gender
)
SELECT 
CASE WHEN erp_cust_id LIKE 'NAS%' THEN SUBSTRING(erp_cust_id, 4, LENGTH(erp_cust_id))
	ELSE erp_cust_id
END AS erp_cust_id,
CASE WHEN CAST(erp_date_of_birth AS DATE) > NOW() THEN NULL
	ELSE erp_date_of_birth
END AS erp_date_of_birth,
CASE WHEN UPPER(TRIM(erp_gender)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(erp_gender)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'Unknown'
END AS erp_gender
FROM bronze.erp_cust_az12;