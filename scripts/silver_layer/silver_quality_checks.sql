SELECT COUNT(*) FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT COUNT (*) FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT COUNT(cst_id) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1 OR cst_id IS NULL

SELECT * FROM silver.crm_cust_info LIMIT 20
