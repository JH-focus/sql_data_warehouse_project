
--explore the possible values of a dimension, in order to grasp it's range
SELECT DISTINCT country FROM gold.dim_customers;


--explore connected dimensions and order by most generic to least generic
SELECT DISTINCT category, subcategory, product_name FROM gold.dim_products
ORDER BY 1,2,3;
