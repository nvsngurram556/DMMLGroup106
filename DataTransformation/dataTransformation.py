import os
import sys
import pandas as pd
import glob
import logging
sys.path.append(os.path.abspath("./Configurations"))
import dbConfig as db
import psycopg2
from dateutil.relativedelta import relativedelta
from sklearn.preprocessing import StandardScaler, MinMaxScaler

# Configure logging
date_time = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
log_file = f'logs/dataTransforamtion_{date_time}.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')



def read_all_csv_files(directory_path):
    try:
        os.makedirs(directory_path, exist_ok=True)
        all_files = glob.glob(os.path.join(directory_path, "*.csv"))
        df_list = []
        for file in all_files:
            df = pd.read_csv(file)
            df_list.append(df)
        combined_df = pd.concat(df_list, ignore_index=True)
        return combined_df
    except:
        print("Error in creating the directory")


# Load dataset
dataset = read_all_csv_files("Staging/OUT")
# Calculate the customer tenure
dataset['customer_tenure_months'] = dataset.apply(lambda row: (pd.to_datetime(row['ingestiondate']).year - pd.to_datetime(row['customer_joined_date']).year) * 12 + (pd.to_datetime(row['ingestiondate']).month - pd.to_datetime(row['customer_joined_date']).month), axis=1)
# Create a column 'customers_all_type_services' based on the condition
dataset['customers_all_type_services'] = dataset.apply(lambda row: row['phoneservice'] == 'Yes' and row['internetservice'] == 'Yes' and row['streamingtv'] == 'Yes', axis=1)
dataset['total_spent_bycustomer_yearly'] = (dataset['monthlycharge'] + dataset['avgmonthlylongdistancecharges']) * 12

num_cols = dataset.select_dtypes(include=["number"]).columns.tolist()
cat_cols = dataset.select_dtypes(include=["object", "category"]).columns.tolist()

scalar = StandardScaler()
normalization = MinMaxScaler()
dataset[num_cols] = scalar.fit_transform(dataset[num_cols])
dataset[num_cols] = normalization.fit_transform(dataset[num_cols])

# Save intermediate result

dataset.to_csv(f"Staging/Cleansed_data/Transformed_data_{date_time}.csv", index=False)

# Database session establishment
try:
    connection = psycopg2.connect(**db.DB_PARAMS)
    cursor = connection.cursor()
    print("Connected to the database")
    logging.info("Connected to the database")
except:
    print("Error in connecting to the database")
    logging.error("Error in connecting to the database")

def insert_data(tablename, dataset):
    try:
        for index, row in dataset.iterrows():
            insert_query = f"""
            INSERT INTO {tablename} (customerid, gender, age, under30, seniorcitizen, married, dependents, numberofdependents, country, state, city, zipcode, latitude, longitude, population, quarter, referredafriend, numberofreferrals, offer, phoneservice, avgmonthlylongdistancecharges, multiplelines, internetservice, internettype, avgmonthlygbdownload, onlinesecurity, onlinebackup, deviceprotectionplan, premiumtechsupport, streamingtv, streamingmovies, streamingmusic, unlimiteddata, contract, paperlessbilling, paymentmethod, monthlycharge, satisfactionscore, customerstatus, churnlabel, churnscore, cltv, churncategory, churnreason, ingestiondate, customer_joined_date, customer_tenure_months, customers_all_type_services, total_spent_bycustomer_yearly) 
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (row['customerid'], row['gender'], row['age'], row['under30'], row['seniorcitizen'], row['married'], row['dependents'], row['numberofdependents'], row['country'], row['state'], row['city'], row['zipcode'], row['latitude'], row['longitude'], row['population'], row['quarter'], row['referredafriend'], row['numberofreferrals'], row['offer'], row['phoneservice'], row['avgmonthlylongdistancecharges'], row['multiplelines'], row['internetservice'], row['internettype'], row['avgmonthlygbdownload'], row['onlinesecurity'], row['onlinebackup'], row['deviceprotectionplan'], row['premiumtechsupport'], row['streamingtv'], row['streamingmovies'], row['streamingmusic'], row['unlimiteddata'], row['contract'], row['paperlessbilling'], row['paymentmethod'], row['monthlycharge'], row['satisfactionscore'], row['customerstatus'], row['churnlabel'], row['churnscore'], row['cltv'], row['churncategory'], row['churnreason'], row['ingestiondate'], row['customer_joined_date'], row['customer_tenure_months'], row['customers_all_type_services'], row['total_spent_bycustomer_yearly']))
        connection.commit()
        print("Data is inserted into table!")
        logging.info("Data is inserted into table!")
    except Exception as e:
        print(f"Error in inserting the data: {e}")
        logging.error(f"Error in inserting the data: {e}")

insert_data(db.dw_tablename, dataset)
logging.info("------------------Part 2 DataIngestion completed------------------")
cursor.close()
connection.close()
