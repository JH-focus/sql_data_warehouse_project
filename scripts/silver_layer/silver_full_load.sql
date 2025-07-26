/*
======================================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
======================================================================================================
Script Purpose:
	This Script executes the ETL process from the bronze layer to the silver layer. The raw data
	in the bronze layer is being trimmed, optimized, enriched, transformed and standardized to 
	better serve the purpose of analysing the data.
Execution style:
	The Script performs a full load, which means it truncates the tables of the silver layer
	and afterwords inserts all the transformed data from the bronze layer.
Parameters:
	This procedure does not accept any parameters.

CALL silver.load_silver();
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $body$
BEGIN
	
	DO $$
	BEGIN
		RAISE NOTICE '========================================';
		RAISE NOTICE '>> Truncating Table silver.crm_cust_info';
		RAISE NOTICE '========================================';
	END;
	$$;
	
	TRUNCATE TABLE silver.crm_cust_info;
	
	DO $$
	BEGIN
		RAISE NOTICE '-------------------------------------------------------------------';
		RAISE NOTICE '>> Inserting data from bronze.crm_cust_info to silver.crm_cust_info';
		RAISE NOTICE '-------------------------------------------------------------------';
	END;
	$$;
	
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr	
	)
	SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	ELSE 'Unknown'
	END AS cst_marital_status,
	
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'M'THEN 'Male'
	WHEN UPPER(TRIM(cst_gndr)) = 'F'THEN 'Female'
	ELSE 'Unknown'
	END AS cst_gndr
	FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info)
	WHERE flag_last = 1;
	
	DO $$
	BEGIN
		RAISE NOTICE '-------------------------------------------------------------------';
		RAISE NOTICE '>> Data inserted from bronze.crm_cust_info to silver.crm_cust_info';
		RAISE NOTICE '-------------------------------------------------------------------';
		RAISE NOTICE '========================================';
		RAISE NOTICE '>> Truncating table silver.crm_prd_info';
		RAISE NOTICE '========================================';
	END;
	$$;
	
	TRUNCATE TABLE silver.crm_prd_info;
	
	DO $$
	BEGIN
		RAISE NOTICE '-------------------------------------------------------------------';
		RAISE NOTICE '>>Inserting data from bronze.crm_prd_info into silver.crm_prd_info';
		RAISE NOTICE '-------------------------------------------------------------------';
	END;
	$$;
	
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
	
	DO $$
	BEGIN
		RAISE NOTICE '-------------------------------------------------------------------';
		RAISE NOTICE '>> Data inserted from bronze.crm_prd_info into silver.crm_prd_info';
		RAISE NOTICE '-------------------------------------------------------------------';
		RAISE NOTICE '===========================================';
		RAISE NOTICE '>>Truncating table silver.crm_sales_details';
		RAISE NOTICE '===========================================';
	END;
	$$;
	
	TRUNCATE TABLE silver.crm_sales_details;
	
	DO $$
	BEGIN
		RAISE NOTICE '---------------------------------------------------------------------------';
		RAISE NOTICE '>> Inserting data from bronze.crm_sales_details to silver.crm_sales_details';
		RAISE NOTICE '---------------------------------------------------------------------------';
	END;
	$$;
	
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
	
	DO $$
	BEGIN
		RAISE NOTICE '-----------------------------------------------------------------------------';
		RAISE NOTICE '>> Data inserted from bronze.crm_sales_details into silver.crm_sales_details';
		RAISE NOTICE '-----------------------------------------------------------------------------';
		RAISE NOTICE '==================================';
		RAISE NOTICE '>> Truncating table erp_cust_az12';
		RAISE NOTICE '==================================';
	END;
	$$;
	
	TRUNCATE TABLE silver.erp_cust_az12;
	
	DO $$
	BEGIN
		RAISE NOTICE '-------------------------------------------------------------------';
		RAISE NOTICE '>>vInserting data from bronze.erp_cust_az12 to silver.erp_cust_az12';
		RAISE NOTICE '-------------------------------------------------------------------';
	END;
	$$;
	
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
	
	DO $$
	BEGIN
		RAISE NOTICE '--------------------------------------------------------------------';
		RAISE NOTICE '>> Data inserted from bronze.erp_cust_az12 into silver.erp_cust_az12';
		RAISE NOTICE '--------------------------------------------------------------------';
		RAISE NOTICE '========================================';
		RAISE NOTICE '>> Truncating table silver.erp_loc_a101';
		RAISE NOTICE '========================================';		
	END;
	$$;
	
	TRUNCATE TABLE silver.erp_loc_a101;
	
	DO $$
	BEGIN
		RAISE NOTICE '-------------------------------------------------------------------';
		RAISE NOTICE '>> Inserting data from bronze.erp_loc_a101 to silver.erp_loc_a101';
		RAISE NOTICE '-------------------------------------------------------------------';
	END;
	$$;
	
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
	
	DO $$
	BEGIN
		RAISE NOTICE '-------------------------------------------------------------------';
		RAISE NOTICE '>> Data inserted from bronze.erp_loc_a101 into silver.erp_loc_a101';
		RAISE NOTICE '-------------------------------------------------------------------';
		RAISE NOTICE '==========================================';
		RAISE NOTICE '>> Truncating table silver.erp_px_cat_g1v2';
		RAISE NOTICE '==========================================';
	END;
	$$;
	
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	
	DO $$
	BEGIN
		RAISE NOTICE '-----------------------------------------------------------------------';
		RAISE NOTICE '>> Inserting data from bronze.erp_px_cat_g1v2 to silver.erp_px_cat_g1v2';
		RAISE NOTICE '-----------------------------------------------------------------------';
	END;
	$$;
	
	INSERT INTO silver.erp_px_cat_g1v2 (
		erp_cat_id,
		erp_prod_category,
		erp_prod_sub_category,
		erp_prod_maintenance
	)
	SELECT
	erp_cat_id,
	erp_prod_category,
	erp_prod_sub_category,
	erp_prod_maintenance
	FROM bronze.erp_px_cat_g1v2;
	
	DO $$
	BEGIN
		RAISE NOTICE '------------------------------------------------------------------------';
		RAISE NOTICE '>> Data inserted from bronze.erp_px_cat_g1v2 into silver.erp_px_cat_g1v2';
		RAISE NOTICE '------------------------------------------------------------------------';
		RAISE NOTICE '=============================================================';
		RAISE NOTICE '>> Data ingestion from bronze layer to silver layer complete';
		RAISE NOTICE '=============================================================';
	END;
	$$;
	
END;
$body$;
