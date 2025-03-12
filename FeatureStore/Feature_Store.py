import subprocess
import pandas as pd
import os
import logging
import sys
from feast import FeatureStore
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

# Logging configuration
date_time = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
log_file = f'logs/feature_Store_{date_time}.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

def initiate_feature_store():
    # Ensure Feast is installed and initialized before running this script
    subprocess.run(["feast", "version"])
    # Initialize Feast repository
    subprocess.run(["feast", "init", "-m", "feature_repo"], cwd=".")   
    logging.info("Feature Store initiated")  
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

def getTransformedData(data):
    # Load the transformed data
    predictors_df = data.loc[:, data.columns != 'churnreason']
    target_df = data['churnreason']
     
    timestamps = pd.date_range(end=pd.Timestamp.now(), periods=len(data), freq='D').to_frame(name='event_timestamp', index=False)
    predictors_df = pd.concat(objs=[predictors_df, timestamps], axis=1)
    target_df = pd.concat(objs=[target_df, timestamps], axis=1)
     
    datalen = len(data)
    idsLst = list(range(datalen))
    customer_ids = pd.DataFrame(data=idsLst, columns=['customer_ids'])
    predictors_df = pd.concat(objs=[predictors_df, customer_ids], axis=1)
    target_df = pd.concat(objs=[target_df, customer_ids], axis=1)
     
    predictors_df.to_parquet(path='FeatureStore/feature_repo/feature_repo/data/predictors_df.parquet')
    target_df.to_parquet(path='FeatureStore/feature_repo/feature_repo/data/target_df.parquet')
    
    #print(predictors_df.describe())
    logging.info("Transformed data loaded")  
 
 
def historicalFeaturesFromFeatureStore():
    # Initialize FeatureStore
    store = FeatureStore(repo_path='FeatureStore/feature_repo/feature_repo')
    # Read entity DataFrame from a Parquet file
    entity_df = pd.read_parquet(path='FeatureStore/feature_repo/feature_repo/data/target_df.parquet')
    # Get historical features from the FeatureStore
    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "customer_df_feature_view:gender",
            "customer_df_feature_view:age",
            "customer_df_feature_view:satisfactionscore",
            "customer_df_feature_view:monthlycharge",
            "customer_df_feature_view:avgmonthlygbdownload",
            "customer_df_feature_view:churnlabel",
            "customer_df_feature_view:avgmonthlylongdistancecharges",
            "customer_df_feature_view:customerid",
            "customer_df_feature_view:customer_tenure_months",
            "customer_df_feature_view:customers_all_type_services",
            "customer_df_feature_view:total_spent_bycustomer_yearly",
        ]
    ).to_df()
    #dataset = store.create_saved_dataset(from_=training_data, name = "customer_churn_dataset", storage=SavedDataFileStorage('data/customer_churn_dataset.parquet'))
    #training_data
    training_df.to_csv(f'Outputfiles/customer_churn_dataset_{date_time}.csv')
    print("Dataset created")
    logging.info("Historical features created")  

if __name__=='__main__':
    #initiate_feature_store()
    if os.makedirs('FeatureStore/feature_repo', exist_ok=True):
        print("Directory created")
    else:
        print("Directory not created")
        initiate_feature_store()
    dataset = read_all_csv_files('Staging/Cleansed_data')
    getTransformedData(dataset)
    # Apply Feast configuration
    subprocess.run(["feast", "apply"], cwd="FeatureStore/feature_repo/feature_repo")
    logging.info("Applied Feast configuration")
    historicalFeaturesFromFeatureStore()

