import sys
import os
sys.path.append(os.path.abspath("./Congifurations"))
import pandas as pd
import dbConfig as db
import psycopg2
import logging

# Logging configuration
date_time = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
log_file = f'logs/dataIngestion_{date_time}.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Read the dataset
dataset = pd.read_csv('Inputfiles/Telco_Customer_Churn.csv')
logging.info("Dataset is read successfully")

# Database session establishment
try:
    connection = psycopg2.connect(**db.DB_PARAMS)
    cursor = connection.cursor()
    print("Connected to the database")
    logging.info("Connected to the database")
except:
    print("Error in connecting to the database")
    logging.error("Error in connecting to the database")

# Check if the table exists
def check_table_exists(tablename):
    try:
        cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (tablename,))
        return cursor.fetchone()[0]
        logging.warning("Table exists")
    except:
        print("Error in checking the table")
        logging.error("Error in checking the table")

# Create the table
def create_table(tablename):
    try:
        query = f"""CREATE TABLE {tablename} (customerID varchar(50), gender varchar(10), SeniorCitizen varchar(50), Partner varchar(50), Dependents varchar(50), tenure varchar(50), PhoneService varchar(50), MultipleLines varchar(50), InternetService varchar(50), OnlineSecurity varchar(50), OnlineBackup varchar(50), DeviceProtection varchar(50), TechSupport varchar(50), StreamingTV varchar(50), StreamingMovies varchar(50), Contract varchar(50), PaperlessBilling varchar(50), PaymentMethod varchar(50), MonthlyCharges varchar(50), TotalCharges varchar(50), Churn varchar(50), IngestionDate Date DEFAULT CURRENT_TIMESTAMP);"""
        cursor.execute(query)
        connection.commit()
    except:
        print("Error in creating the table")
        logging.error("Error in creating the table")

# Insert the data into the table
def insert_data(tablename, dataset):
    try:
        for index, row in dataset.iterrows():
            insert_query = f"""
            INSERT INTO {tablename} (customerID, gender, SeniorCitizen, Partner, Dependents, tenure, PhoneService, MultipleLines, InternetService, OnlineSecurity, OnlineBackup, DeviceProtection, TechSupport, StreamingTV, StreamingMovies, Contract, PaperlessBilling, PaymentMethod, MonthlyCharges, TotalCharges, Churn)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (row['customerID'], row['gender'], row['SeniorCitizen'], row['Partner'], row['Dependents'], row['tenure'], row['PhoneService'], row['MultipleLines'], row['InternetService'], row['OnlineSecurity'], row['OnlineBackup'], row['DeviceProtection'], row['TechSupport'], row['StreamingTV'], row['StreamingMovies'], row['Contract'], row['PaperlessBilling'], row['PaymentMethod'], row['MonthlyCharges'], row['TotalCharges'], row['Churn']))
        connection.commit()
        print("Data is inserted into table!")
        logging.info("Data is inserted into table!")
    except:
        print("Error in inserting the data")
        logging.error("Error in inserting the data")

# Main function
if __name__ == "__main__":
    logging.info("------------------DataIngestion script started------------------")
    if not check_table_exists(db.tablename):
        create_table(db.tablename)
        print("Table created successfully")
        logging.info("Table created successfully")
    else:
        logging.warning("Table already exists")
    insert_data(db.tablename, dataset)
    cursor.close()
    connection.close()
    logging.info("------------------DataIngestion script completed------------------")
