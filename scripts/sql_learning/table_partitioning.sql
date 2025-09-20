--creation of a table to test on
SELECT *
INTO silver.partition_test
FROM silver.crm_sales_details;

SELECT DISTINCT
EXTRACT(YEAR FROM sls_order_date)
FROM silver.partition_test;

