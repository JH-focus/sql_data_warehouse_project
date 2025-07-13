"""
This script is supposed to establish a connection to the postgreSQL Server and
bulk load the data from the given csv-files into the tables of the database.

used libraries: 
psycopg2 (postgreSQL) https://www.psycopg.org/docs/ (documentation)
time (built-in)
"""

import psycopg2 as pc2
import time as t

bronze_layer_tables = ['bronze.crm_cust_info', 
                       'bronze.crm_prod_info', 
                       'bronze.crm_sales_details', 
                       'bronze.erp_cust_az12', 
                       'bronze.erp_loc_a101',
                       'bronze.erp_px_cat_g1v2']

csv_files = [r'C:\Users\gaukl\Downloads\sql-data-warehouse-project\datasets\source_crm/cust_info.csv',
             r'C:\Users\gaukl\Downloads\sql-data-warehouse-project\datasets\source_crm/prd_info.csv',
             r'C:\Users\gaukl\Downloads\sql-data-warehouse-project\datasets\source_crm/sales_details.csv',
             r'C:\Users\gaukl\Downloads\sql-data-warehouse-project\datasets\source_erp/cust_az12.csv',
             r'C:\Users\gaukl\Downloads\sql-data-warehouse-project\datasets\source_erp/loc_a101.csv',
             r'C:\Users\gaukl\Downloads\sql-data-warehouse-project\datasets\source_erp/px_cat_g1v2.csv']

try:
    conn = pc2.connect(dbname="datawarehouse", user="postgres", password="1pbedi38tbiay!", host="localhost", port="5432")
except pc2.Error as e:
    print(f'an error occured: {e}, could not connect to the database')

try:
    cur = conn.cursor()
    batch_start = t.process_time()
    for table, file in zip(bronze_layer_tables, csv_files):
        start = t.process_time()
        print(f'Inserting data into {table} from {file}')
        with open(file, 'r') as file:
            cur.execute(f"TRUNCATE TABLE {table}")
            cur.copy_expert(f"""COPY {table} FROM STDIN WITH CSV HEADER DELIMITER ','""", file)
        end = t.process_time()
        print(f'Inserted data into {table} in {end - start} seconds')
        conn.commit()
    batch_end = t.process_time()
    print(f'Batch insert completed in {batch_end - batch_start} seconds')
    cur.close()
except pc2.Error as e:
    print(f'an error occurred: {e}, could not execute the query')
