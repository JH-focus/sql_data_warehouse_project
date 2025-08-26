/*
============================================================================================================
1. regular index and clustured index
============================================================================================================
*/
--in postgres you can only create a non-clustered index -> you have to cluster afterwards, using 
--the index in order to create a clustered index manually
CREATE INDEX idx_crm_cust_info_cst_id ON bronze.crm_cust_info (cst_id);

CLUSTER bronze.crm_cust_info USING idx_crm_cust_info_cst_id;

DROP INDEX bronze.idx_crm_cust_info_cst_id;

--the clustering has to be executed every time new data is added!
--for that you usually set up a maintenance script that clusters the data post-load
--since clustering is resource intensive, make sure to do it at maintenance times only


/*
============================================================================================================
2. composite index
============================================================================================================
*/
--composite indexes use multiple columns and are especially helpful when the goal is to speed up
--queries that use multiple columns in the WHERE clause
--since composite indexes follow the leftmost prefix rule, you have to be careful about the 
--order of your columns both in the index creation and in the queries
--document this kind of index in a very detailed fashion in order to prevent the failed usage of the index
CREATE INDEX idx_crm_cust_info_cst_first_and_last_name ON bronze.crm_cust_info (cst_firstname, cst_lastname);

DROP INDEX bronze.idx_crm_cust_info_cst_first_and_last_name;

/*
============================================================================================================
3. columnstore index
============================================================================================================
*/
--a columnstore index is not natively supported by postgreSQL
--there are tools like "cstore_fdw" which enable columnar storage in postgreSQL.
--fdw stands for foreign data wrapper, that stores the data columnwise, making it
--much easier to query (faster read operations)

/*
============================================================================================================
4. unique index
============================================================================================================
*/
--unique indexes are indexes for a specific column, that contain unique values.

--when used on a column that has duplicate values, it causes an error
CREATE UNIQUE INDEX idx_crum_cust_info_cst_id ON bronze.crm_cust_info (cst_id);

--this column contains only unique values and therefore can be used to create
--a unique index
CREATE UNIQUE INDEX idx_crm_prod_info_prod_id ON bronze.crm_prod_info (prod_id);

SELECT * FROM bronze.crm_prod_info LIMIT 10;

--trying to insert a new value, that would produce a duplicate value causes
--an error (constraint violation)
INSERT INTO bronze.crm_prod_info (
prod_id,
prod_key,
prod_nm,
prod_cost,
prod_line,
prod_start_date,
prod_end_date
)
VALUES ('210', 'CO-RF-FR-R92B-58', 'FL Road Frame - Black- 58', '40', 'R', '2007-07-01 00:00:00', '2008-07-01 00:00:00');

/*
============================================================================================================
5. filtered index
============================================================================================================
*/
--a filtered index cannot be used on a table with a clustured index or a columnstore index.
--a filtered index is an index, that only contains a subset of rows that is most relevant 
--in order to optimize performance in the queries while reducing storage space


--this creates an index that contains only the customer ids of customers that are
--located in the united states
CREATE INDEX idx_erp_loc_a101_country ON silver.erp_loc_a101 (erp_cust_country)
WHERE erp_cust_country = 'United States';


