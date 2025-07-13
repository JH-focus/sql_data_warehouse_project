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
  END
$$;

DO $$
  BEGIN
    RAISE NOTICE 'Truncating Table: bronze.crm_cust_info';
  END
$$;
    TRUNCATE TABLE bronze.crm_cust_info;

DO $$
  BEGIN
    RAISE NOTICE '>> Loading Table data: bronze.crm_cust_info';
  END
$$;
    \COPY bronze.crm_cust_info FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' DELIMITER ',' HEADER CSV
      
DO $$
  BEGIN    
    RAISE NOTICE 'Truncating Table: bronze.crm_prod_info';
  END
$$;
    TRUNCATE TABLE bronze.crm_prod_info;

DO $$
  BEGIN    
    RAISE NOTICE '>> Loading Table data: bronze.crm_prod_info';
  END
$$;
    \COPY bronze.crm_prod_info FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' DELIMITER ',' HEADER CSV

DO $$
  BEGIN    
    RAISE NOTICE 'Truncating Table: bronze.crm_sales_details';
  END
$$;
    TRUNCATE TABLE bronze.crm_sales_details;

DO $$
  BEGIN    
    RAISE NOTICE '>> Loading Table data: bronze.crm_sales_details';
  END
$$;
    \COPY bronze.crm_sales_details FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' DELIMITER ',' HEADER CSV

DO $$
  BEGIN    
    RAISE NOTICE 'Truncating Table: bronze.erp_cust_az12';
  END
$$;
    TRUNCATE TABLE bronze.erp_cust_az12;

DO $$
  BEGIN    
    RAISE NOTICE '>> Loading Table data: bronze.erp_cust_az12';
  END
$$;
    \COPY bronze.erp_cust_az12 FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv' DELIMITER ',' HEADER CSV

DO $$
  BEGIN    
    RAISE NOTICE 'Truncating Table: bronze.erp_loc_a101';
  END
$$;
    TRUNCATE TABLE bronze.erp_loc_a101;

DO $$
  BEGIN    
    RAISE NOTICE '>> Loading Table data: bronze.erp_loc_a101';
  END
$$;
    \COPY bronze.erp_loc_a101 FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv' DELIMITER ',' HEADER CSV

DO $$
  BEGIN    
    RAISE NOTICE 'Truncating Table: bronze.erp_px_cat_g1v2';
  END
$$;
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

DO $$
  BEGIN    
    RAISE NOTICE '>> Loading Table data: bronze.erp_px_cat_g1v2';
  END
$$;
    \COPY bronze.erp_px_cat_g1v2 FROM 'C:/Users/gaukl/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' DELIMITER ',' HEADER CSV

