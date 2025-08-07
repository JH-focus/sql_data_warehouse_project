SELECT
order_number,
order_date,
SUM(sales_amount) OVER(PARTITION BY order_date) total_sales_by_products
FROM gold.fact_sales f;

--Find the total sales for each product
--additionally provide details such as order ID, order date
SELECT
order_number,
order_date,
product_key,
sales_amount,
SUM(sales_amount) OVER() as total_sales,
SUM(sales_amount) OVER (PARTITION BY product_key) AS total_sales_by_products
FROM gold.fact_sales;