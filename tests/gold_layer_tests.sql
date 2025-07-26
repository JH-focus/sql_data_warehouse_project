/*
This testing script checks whether the gold layer is in itself correct 
and can be linked to the two dimensional tables poducts and customers
*/

-- ========================================================
-- Checking if fact_sales can be connected to dim_products
-- ========================================================

SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL;

-- ========================================================
-- Checking if fact_sales can be connected to dim_customers
-- ========================================================

SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL;