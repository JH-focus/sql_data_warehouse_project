/*
=======================================================================================================
Product report
=======================================================================================================
Purpose:
	- This report consolidates key product metrics and behaviours.

Highlights
	1. Gathers essential fields such as product name, category, subcategory, and cost.
	2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
	3. Aggregates product-level metrics:
		- total orders
		- total sales
		- total quantity sold
		- total customers (unique)
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last sale)
		- average order revenue (AOR)
		- average monthly revenue (AMR)
=======================================================================================================
*/
CREATE OR REPLACE VIEW gold.report_products AS(
WITH base_query AS (
/*
1.) base query: retrieves core columns from fact_sales and dim_products
*/
SELECT
f.order_number,
f.order_date,
f.sales_amount,
f.quantity,
f.customer_key,
p.product_key,
p.product_name,
p.category,
p.subcategory,
p.cost
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE order_date IS NOT NULL
),

product_aggregation AS(
/*
2.) product_aggregation creates the derived columns that we need in our view from the base query (e.g. lifespan, last_sale_date)
*/
SELECT
product_key,
product_name,
category,
subcategory,
b.cost,
EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 +
EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan,
MAX(order_date) AS last_sale_date,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT customer_key) AS total_customers,
ROUND(AVG(sales_amount/COALESCE(quantity, 0)), 2) AS avg_selling_price
FROM base_query b
GROUP BY
product_key,
product_name,
category,
subcategory,
b.cost
)

SELECT
/*
3.) last query takes the refined input from product_aggregation and enriches the data 
*/
product_key,
product_name,
category,
subcategory,
pa.cost,
last_sale_date,
EXTRACT(YEAR FROM AGE(CURRENT_TIMESTAMP, last_sale_date)) * 12 +
EXTRACT(MONTH FROM AGE(CURRENT_TIMESTAMP, last_sale_date)) AS recency_in_months,
CASE WHEN total_sales > 50000 THEN 'High-Performer'
	WHEN total_sales >= 10000 THEN 'Mid-Range'
	ELSE 'Low-Performer'
END AS product_segment,
lifespan,
total_orders,
total_sales,
total_quantity,
total_customers,
avg_selling_price,
CASE WHEN total_orders = 0 THEN 0
	ELSE ROUND(total_sales / total_orders, 2)
END AS avg_order_revenue,
CASE WHEN lifespan = 0 THEN total_sales
	ELSE ROUND(total_sales / lifespan, 2)
END AS avg_monthly_revenue
FROM product_aggregation pa
);

