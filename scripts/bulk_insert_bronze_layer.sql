/*
This file is supposed to Insert the data from the cust_info.csv file into the bronze.crm_cust_info database. 
The method used is bulk insert which means the entire content of the csv file will be transferred into the 
database in one go - not row by row. 
This makes this method very fast but less flexible.
*/


\COPY bronze.crm_cust_info
FROM 'C:U/sers/gaukl/Downloads/sql-data-warehouse-project/datasets/cust_info'
DELIMITER ','
CSV HEADER;
