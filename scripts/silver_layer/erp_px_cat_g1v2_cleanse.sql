INSERT INTO silver.erp_px_cat_g1v2 (
	erp_cat_id,
	erp_prod_category,
	erp_prod_sub_category,
	erp_prod_maintenance
)
SELECT
erp_cat_id,
erp_prod_category,
erp_prod_sub_category,
erp_prod_maintenance
FROM bronze.erp_px_cat_g1v2;


