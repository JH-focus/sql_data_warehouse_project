--SUM() OVER (PARTITION BY ...)
SELECT
order_number,
order_date,
SUM(sales_amount) OVER(PARTITION BY order_date) total_sales_by_products
FROM gold.fact_sales f;

--Find the total sales for each product
--additionally provide details such as order ID, order date
SELECT
f.order_number,
f.order_date,
f.product_key,
f.sales_amount,
p.category,
p.subcategory,
SUM(sales_amount) OVER() as total_sales,
SUM(sales_amount) OVER (PARTITION BY p.category) AS total_sales_by_products
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key;

--RANK() OVER (PARTITION BY)
SELECT
f.order_date,
f.order_number,
f.sales_amount,
p.category,
p.subcategory,
RANK() OVER (ORDER BY sales_amount ASC) AS rank_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key;

--window frame definition
--extremely useful when determining an average over the last three months, or years...
--important to consider that at the end of the table -> the subset 
SELECT
f.order_date,
f.order_number,
f.sales_amount,
p.category,
p.subcategory,
--the frame always has to be defined from "lowest to highest" -> meaning:
--ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
--ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
--ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
ROUND(AVG(f.sales_amount) OVER (ORDER BY f.order_date ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING), 2) AS avg_test_sales,
ROUND(SUM(f.sales_amount) OVER (ORDER BY f.order_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), 2) AS avg_test_sum,
ROUND(AVG(f.sales_amount) OVER (ORDER BY f.order_date ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS avg_test_sales,
ROUND(SUM(f.sales_amount) OVER (PARTITION BY DATE_TRUNC('YEAR', f.order_date) ORDER BY f.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 2) AS entire_window,
--Shortcut for preceding rows, works ONLY for preceding, not for following!
ROUND(AVG(f.sales_amount) OVER (ORDER BY f.order_date ROWS 1 PRECEDING), 2) AS avg_test_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key;

--RANK() function
--handles ties -> same rank possible with blank ranks, identical values will have the exact same rank
SELECT
f.customer_key,
SUM(f.sales_amount) AS total_sales,
RANK() OVER (ORDER BY SUM(f.sales_amount) DESC)
FROM gold.fact_sales f
GROUP BY f.customer_key;

--ROW_NUMBER() function
--not handling ties - same values will have different row number
SELECT
f.sales_amount,
p.category,
p.subcategory,
ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY f.sales_amount DESC) AS rn
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key;

--Top-N analysis
--Highest sales by product_key
SELECT * 
FROM(
SELECT
f.order_number,
f.product_key,
f.sales_amount,
ROW_NUMBER() OVER (PARTITION BY f.product_key ORDER BY f.sales_amount DESC) rank_by_product,
DENSE_RANK() OVER (PARTITION BY f.product_key ORDER BY f.sales_amount DESC) dense_rank_by_product
FROM gold.fact_sales f
)
WHERE rank_by_product <= 5;

--bottom-N analysis
--lowest 2 total_sales by customers
SELECT *
FROM(
SELECT
f.customer_key,
SUM(f.sales_amount),
ROW_NUMBER() OVER (ORDER BY SUM(f.sales_amount) ASC) AS rank_customers
FROM gold.fact_sales f
GROUP BY f.customer_key
)
WHERE rank_customers <= 2;

--assign unique IDs to rows of gold.fact_sales
SELECT
REPLACE('ID' || TO_CHAR(ROW_NUMBER() OVER (ORDER BY f.order_date), '00000'), ' ', '') AS unique_id,
LENGTH(REPLACE('ID' || TO_CHAR(ROW_NUMBER() OVER (ORDER BY f.order_date), '00000'), ' ', '')) AS length_id,
*
FROM gold.fact_sales f;

--clean duplicates using ROW_NUMBER()
SELECT *
FROM(
SELECT
f.order_number,
ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY order_date DESC) rn
FROM gold.fact_sales f
)
WHERE rn = 1;