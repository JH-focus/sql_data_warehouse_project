-- Find the total sales
SELECT 
EXTRACT(YEAR FROM order_date),
SUM(sales_amount) AS total_sales_per_year
FROM gold.fact_sales
GROUP BY EXTRACT(YEAR FROM order_date);

-- Find how many items are sold
SELECT
p.category,
p.subcategory,
SUM(f.quantity) AS total_items_sold_per_subcategory
FROM gold.fact_sales f
LEFT OUTER JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY 1,2;

-- Find the average selling price
SELECT
TO_CHAR(AVG(sales_amount/quantity), 'FM9999.00 L') AS average_selling_price
FROM gold.fact_sales;

-- Find the total number of orders
SELECT 
EXTRACT(YEAR FROM order_date),
EXTRACT(MONTH FROM order_date),
COUNT(DISTINCT order_number) AS total_orders_by_Year_and_month
FROM gold.fact_sales
GROUP BY 1,2;

-- Find the total number of products
SELECT 
COUNT(DISTINCT product_number) AS total_number_of_products
FROM gold.dim_products;

-- Find the total number of customers
SELECT
COUNT(DISTINCT customer_key) AS total_number_of_customers
FROM gold.dim_customers;

-- Find the total number of customers that have placed an order
SELECT
COUNT(DISTINCT customer_key) AS total_num_of_cust_with_order
FROM gold.fact_sales f;

--combining all the above
--Using UNION ALL is perfect in this case, becaus only the number of columns
--and the data types must be matching
SELECT 'Total Sales' as measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products', COUNT(DISTINCT product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers', COUNT(DISTINCT customer_key) FROM gold.dim_customers;