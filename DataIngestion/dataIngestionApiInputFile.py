import os
import sys
sys.path.append(os.path.abspath("./Configurations"))
import dbConfig as db
import psycopg2
import pandas as pd
import logging
from kaggle.api.kaggle_api_extended import KaggleApi

# Set environment variables for Kaggle API credentials
os.environ['KAGGLE_USERNAME'] = db.kaggle_username
os.environ['KAGGLE_KEY'] = db.kaggle_key

# Logging configuration
date_time = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
log_file = f'logs/dataIngestion_{date_time}.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')


############################### PART 1 ###############################
# Download the dataset from Kaggle

def download_dataset():
    try:
        api = KaggleApi()
        api.authenticate()
        dataset = 'alfathterry/telco-customer-churn-11-1-3'
        path = 'Inputfiles'
        api.dataset_download_files(dataset, path=path, unzip=True)
        logging.info("Dataset is downloaded successfully")
    except:
        print("Error in downloading the dataset")
        logging.error("Error in downloading the dataset")

def file_columns_update():
    try:
        dataset = pd.read_csv('Inputfiles/telco.csv')
        dataset.columns = dataset.columns.str.replace(' ', '')
        dataset.to_csv('Inputfiles/telco.csv', index=False)
        print("Columns are updated successfully")
        logging.info("Columns are updated successfully")
    except:
        print("Error in reading the dataset")
        logging.error("Error in reading the dataset")


############################### PART 2 ###############################
# File to Database Ingestion

# Database session establishment
try:
    connection = psycopg2.connect(**db.DB_PARAMS)
    cursor = connection.cursor()
    print("Connected to the database")
    logging.info("Connected to the database")
except:
    print("Error in connecting to the database")
    logging.error("Error in connecting to the database")


def check_table_exists(tablename):
    try:
        cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (tablename,))
        return cursor.fetchone()[0]
        logging.warning("Table exists")
    except:
        print("Error in checking the table")
        logging.error("Error in checking the table")

def table_creation(tablename):
    try:
        query = f"""CREATE TABLE {tablename} (CustomerID varchar(50), Gender varchar(50), Age varchar(50), Under30 varchar(50), SeniorCitizen varchar(50), Married varchar(50), Dependents varchar(50), NumberofDependents varchar(50), Country varchar(50), State varchar(50), City varchar(50), ZipCode varchar(50), Latitude varchar(50), Longitude varchar(50), Population varchar(50), Quarter varchar(50), ReferredaFriend varchar(50), NumberofReferrals varchar(50), TenureinMonths varchar(50), Offer varchar(50), PhoneService varchar(50), AvgMonthlyLongDistanceCharges varchar(50), MultipleLines varchar(50), InternetService varchar(50), InternetType varchar(50), AvgMonthlyGBDownload varchar(50), OnlineSecurity varchar(50), OnlineBackup varchar(50), DeviceProtectionPlan varchar(50), PremiumTechSupport varchar(50), StreamingTV varchar(50), StreamingMovies varchar(50), StreamingMusic varchar(50), UnlimitedData varchar(50), Contract varchar(50), PaperlessBilling varchar(50), PaymentMethod varchar(50), MonthlyCharge varchar(50), TotalCharges varchar(50), TotalRefunds varchar(50), TotalExtraDataCharges varchar(50), TotalLongDistanceCharges varchar(50), TotalRevenue varchar(50), SatisfactionScore varchar(50), CustomerStatus varchar(50), ChurnLabel varchar(50), ChurnScore varchar(50), CLTV varchar(50), ChurnCategory varchar(50), ChurnReason varchar(50), IngestionDate Date DEFAULT CURRENT_TIMESTAMP);"""
        cursor.execute(query)
        connection.commit()
        print("Table is created successfully")
        logging.info("Table is created successfully")
    except:
        print("Error in creating the table")
        logging.error("Error in creating the table")

def insert_data(tablename, dataset):
    try:
        for index, row in dataset.iterrows():
            insert_query = f"""
            INSERT INTO {tablename} (CustomerID,Gender,Age,Under30,SeniorCitizen,Married,Dependents,NumberofDependents,Country,State,City,ZipCode,Latitude,Longitude,Population,Quarter,ReferredaFriend,NumberofReferrals,TenureinMonths,Offer,PhoneService,AvgMonthlyLongDistanceCharges,MultipleLines,InternetService,InternetType,AvgMonthlyGBDownload,OnlineSecurity,OnlineBackup,DeviceProtectionPlan,PremiumTechSupport,StreamingTV,StreamingMovies,StreamingMusic,UnlimitedData,Contract,PaperlessBilling,PaymentMethod,MonthlyCharge,TotalCharges,TotalRefunds,TotalExtraDataCharges,TotalLongDistanceCharges,TotalRevenue,SatisfactionScore,CustomerStatus,ChurnLabel,ChurnScore,CLTV,ChurnCategory,ChurnReason)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """
            cursor.execute(insert_query, (row['CustomerID'], row['Gender'], row['Age'], row['Under30'], row['SeniorCitizen'], row['Married'], row['Dependents'], row['NumberofDependents'], row['Country'], row['State'], row['City'],row['ZipCode'], row['Latitude'], row['Longitude'], row['Population'], row['Quarter'],row['ReferredaFriend'], row['NumberofReferrals'], row['TenureinMonths'], row['Offer'],row['PhoneService'], row['AvgMonthlyLongDistanceCharges'], row['MultipleLines'], row['InternetService'], row['InternetType'], row['AvgMonthlyGBDownload'],row['OnlineSecurity'], row['OnlineBackup'], row['DeviceProtectionPlan'],row['PremiumTechSupport'], row['StreamingTV'], row['StreamingMovies'],row['StreamingMusic'], row['UnlimitedData'], row['Contract'], row['PaperlessBilling'], row['PaymentMethod'], row['MonthlyCharge'], row['TotalCharges'], row['TotalRefunds'], row['TotalExtraDataCharges'], row['TotalLongDistanceCharges'], row['TotalRevenue'],row['SatisfactionScore'], row['CustomerStatus'], row['ChurnLabel'], row['ChurnScore'], row['CLTV'], row['ChurnCategory'], row['ChurnReason']))
        connection.commit()
        print("Data is inserted into table!")
        logging.info("Data is inserted into table!")
    except:
        print("Error in inserting the data")
        logging.error("Error in inserting the data")

if __name__ == "__main__":
    logging.info("------------------DataIngestion script started------------------")
    logging.info("------------------Part 1 DataIngestion started------------------")
    download_dataset()
    file_columns_update()
    logging.info("------------------Part 1 DataIngestion completed------------------")
    logging.info("------------------Part 2 DataIngestion started------------------")
    if not check_table_exists(db.tablename):
        table_creation(db.tablename)
        print("Table created successfully")
        logging.info("Table created successfully")
    else:
        logging.warning("Table already exists")
    dataset = pd.read_csv('Inputfiles/telco.csv')
    insert_data(db.tablename, dataset)
    logging.info("------------------Part 2 DataIngestion completed------------------")
    cursor.close()
    connection.close()
    logging.info("------------------DataIngestion script completed------------------")
