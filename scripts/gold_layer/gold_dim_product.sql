CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER() OVER (ORDER BY pn.prd_start_date, pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.erp_prod_category AS category,
pc.erp_prod_sub_category AS subcategory,
pc.erp_prod_maintenance AS maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_date As start_date
FROM silver.crm_prd_info pn
LEFT OUTER JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.erp_cat_id
WHERE prd_end_date IS NULL --Filter out historical data
;
