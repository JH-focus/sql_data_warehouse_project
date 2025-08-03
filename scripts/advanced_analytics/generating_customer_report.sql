/*
=======================================================================================================
Customer report
=======================================================================================================
Purpose:
	- This report consolidates key customer metrics and behaviours

Highlights:
	1. Gathers essential fields such as names, ages and transaction details.
	2. Segments customers into categories (vip, regular, new) and age groups.
	3. Aggregates customer-level metrics:
		- total orders
		- total sales
		- total quantity purchased
		- total products
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last order)
		- average order value
		- average monthly spent
=======================================================================================================
*/

WITH base_query AS (
/*
1.) the base query selects the core columns from our fact and dim tables, while already creating the age-column
*/
SELECT
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
EXTRACT(YEAR FROM AGE(CURRENT_TIMESTAMP::DATE, c.birthdate)) as age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL
),

customer_aggregation AS(
SELECT
customer_key,
customer_number,
customer_name,
age,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT product_key) AS total_products,
MAX(order_date) AS last_order_date,
EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 +
EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan
FROM base_query
GROUP BY 
customer_key,
customer_number,
customer_name,
age
)

SELECT
customer_key,
customer_number,
customer_name,
age,
CASE WHEN age < 20 THEN 'under 20'
	WHEN age BETWEEN 20 AND 29 THEN '20-29'
	WHEN age BETWEEN 30 AND 39 THEN '30-39'
	WHEN age BETWEEN 40 AND 49 THEN '40-49'
	ELSE '50 and above'
END as age_group,
CASE WHEN lifespan >= 12 AND total_sales > 5000 THEN 'vip'
	WHEN lifespan >= 12 AND total_sales < 5000 THEN 'regular'
	ELSE 'new'
END AS customer_segment,
EXTRACT(YEAR FROM AGE(CURRENT_TIMESTAMP::DATE, last_order_date)) * 12 +
EXTRACT(MONTH FROM AGE(CURRENT_TIMESTAMP::DATE, last_order_date)) AS recency,
total_orders,
total_sales,
total_quantity,
total_products,
last_order_date,
lifespan,
--compute average order value
CASE WHEN total_orders = 0 THEN 0
	ELSE total_sales / total_orders
END AS avg_order_value,
--compute average monthly spent
CASE WHEN lifespan = 0 THEN total_sales
	ELSE ROUND(total_sales / lifespan, 2)
END AS avg_monthly_spent
FROM customer_aggregation;

