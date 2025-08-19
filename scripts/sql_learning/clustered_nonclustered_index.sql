--in postgres you can only create a non-clustered index -> you have to cluster afterwards, using 
--the index in order to create a clustered index manually
CREATE INDEX idx_crm_cust_info_cst_id ON bronze.crm_cust_info (cst_id);

CLUSTER bronze.crm_cust_info USING idx_crm_cust_info_cst_id;

--the clustering has to be executed every time new data is added!
--for that you usually set up a maintenance script that clusters the data post-load
--since clustering is resource intensive, make sure to do it at maintenance times only


--composite indexes use multiple columns and are especially helpful when the goal is to speed up
--queries that use multiple columns in the WHERE clause
--since composite indexes follow the leftmost prefix rule, you have to be careful about the 
--order of your columns both in the index creation and in the queries
--document this kind of index in a very detailed fashion in order to prevent the failed usage of the index
CREATE INDEX idx_crm_cust_info_cst_first_and_last_name ON bronze.crm_cust_info (cst_firstname, cst_lastname);