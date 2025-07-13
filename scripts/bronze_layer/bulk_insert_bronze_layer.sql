/*
This file is supposed to Insert the data from the cust_info.csv file into the bronze.crm_cust_info database. 
The method used is bulk insert which means the entire content of the csv file will be transferred into the 
database in one go - not row by row. 
This makes this method very fast but less flexible.
*/

DO $$
  BEGIN
    RAISE NOTICE '====================';
    RAISE NOTICE 'Loading bronze layer';
    RAISE NOTICE '====================';

    RAISE NOTICE '--------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '--------------------';


    RAISE NOTICE 'Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    
    RAISE NOTICE '>> Loading Table data: bronze.crm_cust_info';
    \COPY bronze.crm_cust_info FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' DELIMITER ',' HEADER CSV
    
    RAISE NOTICE 'Truncating Table: bronze.crm_prod_info';
    TRUNCATE TABLE bronze.crm_prod_info;
    
    RAISE NOTICE '>> Loading Table data: bronze.crm_prod_info';
    \COPY bronze.crm_prod_info FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' DELIMITER ',' HEADER CSV
    
    RAISE NOTICE 'Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    
    RAISE NOTICE '>> Loading Table data: bronze.crm_sales_details';
    \COPY bronze.crm_sales_details FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' DELIMITER ',' HEADER CSV
    
    RAISE NOTICE 'Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    
    RAISE NOTICE '>> Loading Table data: bronze.erp_cust_az12';
    \COPY bronze.erp_cust_az12 FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv' DELIMITER ',' HEADER CSV
    
    RAISE NOTICE 'Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    
    RAISE NOTICE '>> Loading Table data: bronze.erp_loc_a101';
    \COPY bronze.erp_loc_a101 FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv' DELIMITER ',' HEADER CSV
    
    RAISE NOTICE 'Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    
    RAISE NOTICE '>> Loading Table data: bronze.erp_px_cat_g1v2';
    \COPY bronze.erp_px_cat_g1v2 FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' DELIMITER ',' HEADER CSV
  END
$$;

