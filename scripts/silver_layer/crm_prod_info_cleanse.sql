INSERT INTO silver.crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_date,
prd_end_date
)

SELECT 
prod_id,
REPLACE(SUBSTRING(prod_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prod_key, 7, LENGTH(prod_key)) AS prod_key,
prod_nm,
COALESCE(prod_cost, 0) AS prod_cost,
CASE UPPER(TRIM(prod_line)) 
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'Unknown'
END AS prod_line,
CAST(prod_start_date AS DATE),
CAST(LEAD(prod_start_date) OVER (PARTITION BY prod_key ORDER BY prod_start_date)- interval '1 day' AS DATE) AS prod_end
FROM bronze.crm_prod_info;

