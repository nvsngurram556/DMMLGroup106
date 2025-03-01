import sys
import os
sys.path.append(os.path.abspath("./Configurations"))
import dbConfig as db
import psycopg2
import pandas as pd
import logging

# Logging configuration
date_time = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
log_file = f'logs/rawDataStorage_{date_time}.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Database session establishment
try:
    connection = psycopg2.connect(**db.DB_PARAMS)
    cursor = connection.cursor()
    print("Connected to the database")
    logging.info("Connected to the database")
except:
    print("Error in connecting to the database")
    logging.error("Error in connecting to the database")

def read_tabledata(table_name):
    try:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        data = cursor.fetchall()
        return data
    except:
        print("Error in reading the table data")
        logging.error("Error in reading the table data")

def save_table_to_csv(table_name, csv_file_path):
    try:
        query = f"COPY (SELECT * FROM {table_name} WHERE ingestiondate = CURRENT_DATE) TO STDOUT WITH CSV HEADER"
        with open(csv_file_path, 'w') as f:
            cursor.copy_expert(query, f)
        print(f"Data from {table_name} saved to {csv_file_path}")
        logging.info(f"Data from {table_name} saved to {csv_file_path}")
    except Exception as e:
        print(f"Error in saving table data to CSV: {e}")
        logging.error(f"Error in saving table data to CSV: {e}")

if __name__ == "__main__":
    logging.info("------------------DataIngestion script started------------------")
    read_tabledata(db.tablename)
    file_name = f"Staging/IN/{db.tablename}_{date_time}.csv"
    save_table_to_csv(db.tablename, file_name)
    logging.info("------------------DataIngestion script completed------------------")