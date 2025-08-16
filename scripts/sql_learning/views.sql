--views are effectively queries that are stored in the database (not data)
--every time a view is called, the associated query is executed and the result
--shown to the user -> if the underlying tables change - the view changes
CREATE OR REPLACE VIEW gold.monthly_summary AS(
	SELECT
	DATE_TRUNC('month', order_date)::date AS order_month,
	SUM(sales_amount) AS total_sales,
	COUNT(order_number) AS total_orders,
	SUM(quantity) AS total_quantities
	FROM gold.fact_sales
	GROUP BY DATE_TRUNC('month', order_date)::date
)

--via ordinary select-statements the data users can now access the already
--aggregated values and do not have to execute the aggregation themselves
SELECT
*
FROM gold.monthly_summary;

--when a view is not needed anymore, it can be dropped without dire consequences
--other than that the logic of the view may be lost.
DROP VIEW gold.monthly_summary;

--views can be used to join tables before a user gets access, thereby increasing readability
--and reducing technical complexity for the users.
CREATE OR REPLACE VIEW gold.combined AS(
	SELECT
	f.order_number,
	f.order_date,
	f.shipping_date,
	f.due_date,
	f.sales_amount,
	f.quantity,
	f.price,
	CONCAT(COALESCE(c.first_name,''), ' ', COALESCE(c.last_name, '')) AS customer_name,
	c.country,
	c.marital_status,
	c.gender,
	c.birthdate,
	c.create_date,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost,
	p.product_line,
	p.start_date
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON c. customer_key = f.customer_key
	LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key
	ORDER BY order_date
)

SELECT 
*
FROM gold.combined;

DROP VIEW gold.combined;

--views can be used to limit access for different roles
--create different views and grant access to the varying roles, in order to prevent data security issues

--e.g. combined view excluding all customers from the U.S.A.
CREATE OR REPLACE VIEW gold.combined_exclude_usa AS(
	SELECT
	f.order_number,
	f.order_date,
	f.shipping_date,
	f.due_date,
	f.sales_amount,
	f.quantity,
	f.price,
	CONCAT(COALESCE(c.first_name,''), ' ', COALESCE(c.last_name, '')) AS customer_name,
	c.country,
	c.marital_status,
	c.gender,
	c.birthdate,
	c.create_date,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost,
	p.product_line,
	p.start_date
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON c. customer_key = f.customer_key
	LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key
	ORDER BY order_date
	WHERE c.country !='United States'
)

SELECT
*
FROM gold.combined_exclude_usa;

DROP VIEW gold.combined_exclude_usa;


