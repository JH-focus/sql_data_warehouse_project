-- Which 5 products generate the highest revenue?
SELECT
p.product_name,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 5;


-- What are the 5 worst-performing products in terms of sales?
SELECT
p.product_name,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue ASC
LIMIT 5;

-- What are the 5 worst-performing subcategories in terms of sales?
SELECT
p.subcategory,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.subcategory
ORDER BY total_revenue ASC
LIMIT 5;

-- More elaborate way of sorting the data and only showing the top or bottom X entries
SELECT *
FROM(
SELECT
p.product_name,
SUM(f.sales_amount) AS total_revenue,
RANK() OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_products
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name)
WHERE rank_products <= 5;


-- Find the top 10 customers who have generated the highest revenue
SELECT *
FROM
(SELECT
c.customer_key,
c.first_name,
c.last_name,
SUM(f.sales_amount) AS total_revenue,
ROW_NUMBER() OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_customers
FROM gold.fact_sales f
LEFT OUTER JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY
c.customer_key,
c.first_name,
c.last_name)
WHERE rank_customers <= 10;

-- Fnid the 3 customers with the fewest orders placed
SELECT *
FROM
(SELECT
c.customer_key,
c.first_name,
c.last_name,
COUNT(DISTINCT f.order_number) AS total_orders,
ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT f.order_number) ASC) AS rank_customers
FROM gold.fact_sales f
LEFT OUTER JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY
c.customer_key,
c.first_name,
c.last_name)
WHERE rank_customers <= 3;

SELECT
c.customer_key,
c.first_name,
c.last_name,
COUNT(DISTINCT f.order_number) AS total_orders
FROM gold.fact_sales f
LEFT OUTER JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY
c.customer_key,
c.first_name,
c.last_name
ORDER BY total_orders
LIMIT 3;

