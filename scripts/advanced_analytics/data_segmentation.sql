/*
Data segmentation is done with the CASE WHEN clause, that works like a switch function and defines
different checks in order to segregate the data into different groups.
segment products into cost ranges and count how many producs fall into each segment
*/

WITH product_segments AS(
SELECT
p.product_key,
p.product_name,
p.cost,
CASE WHEN p.cost <= 100 THEN '<100'
	WHEN p.cost BETWEEN 100 AND 500 THEN '100-500'
	WHEN p.cost BETWEEN 500 AND 1000 THEN '500-1000'
	ELSE '>1000'
END AS cost_range
FROM gold.dim_products p
)

SELECT
cost_range,
COUNT(DISTINCT product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;


/*
Group customers into three segments based on their spending behaviour:
	- VIP customers with at least 12 months of history and spending more tha 5.000 €
	- Regular customers with at least 12 months of history but spending 5000 € or less
	- new customers with a lifespan less than 12 months
And find the total number of customers by each group
*/

WITH customer_spending AS(
SELECT
c.customer_key,
SUM(f.sales_amount) AS total_sales,
MIN(f.order_date) AS first_order,
MAX(f.order_date) AS last_order,
EXTRACT(YEAR FROM AGE(MAX(f.order_date),MIN(f.order_date))) * 12 +
EXTRACT(MONTH FROM AGE(MAX(f.order_date),MIN(f.order_date))) AS lifespan
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY c.customer_key
)

SELECT
COUNT(customer_key) AS total_customers,
CASE WHEN lifespan >= 12 AND total_sales > 5000 THEN 'vip'
	WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'regular'
	ELSE 'new'
END AS customer_segment
FROM customer_spending
GROUP BY customer_segment
ORDER BY total_customers DESC;