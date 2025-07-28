-- Find Total customers by countries
SELECT
country,
COUNT(DISTINCT customer_key) AS customers_per_country
FROM gold.dim_customers
GROUP BY country
ORDER BY customers_per_country DESC;

-- Find total customers by gender
SELECT
gender,
COUNT(DISTINCT customer_key) AS customers_per_gender
FROM gold.dim_customers
GROUP BY gender
ORDER BY customers_per_gender DESC;

-- Find total products by category
SELECT
category,
COUNT(DISTINCT product_name) AS products_per_category
FROM gold.dim_products
GROUP BY category
ORDER BY products_per_category DESC;

-- Find average cost in each category
SELECT
category,
CAST(TO_CHAR(AVG(p.cost), 'FM9999.00') AS NUMERIC) AS cost_per_category
FROM gold.dim_products p
GROUP BY category
ORDER BY cost_per_category DESC;

-- Find total revenue for eacht category
SELECT
p.category,
CAST(TO_CHAR(SUM(f.sales_amount), 'FM9999999999.00') AS NUMERIC) AS revenue_per_category
FROM gold.fact_sales f
LEFT OUTER JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY revenue_per_category DESC;

-- Find total revenue by customer
SELECT
customer_key,
SUM(sales_amount) AS revenue_per_customer
FROM gold.fact_sales
GROUP BY customer_key
ORDER BY revenue_per_customer DESC;

-- Find distribution of sold items across countries
SELECT
c.country,
SUM(f.quantity) AS items_sold_per_country
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY items_sold_per_country DESC;