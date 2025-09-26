--trigger definition

SELECT * FROM silver.crm_cust_info LIMIT 10;

--creating trigger function, that will be used in the
--trigger -> this minimizes the 
CREATE OR REPLACE FUNCTION silver.log_new_cst()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	RAISE NOTICE 'New customer insterted: id=%, firstname=%, lastname=%, ts=%',
		NEW.cst_id,
		NEW.cst_firstname,
		NEW.cst_lastname,
		CURRENT_TIMESTAMP;
	RETURN NEW;
END;
$$;

--trigger creation together with the definition when it is going to execute
CREATE TRIGGER trg_after_insert_cust
AFTER INSERT ON silver.crm_cust_info
FOR EACH ROW
EXECUTE FUNCTION silver.log_new_cst();

--Test insertion
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date,
	dwh_create_date
)
VALUES (99000, 'AW00099000', 'Jeremias', 'Hirsch', 'Single', 'Male', '2025-09-26', CURRENT_TIMESTAMP);

--test with the silver.load_silver procedure (truncation and insertion of the entire table)
CALL silver.load_silver();