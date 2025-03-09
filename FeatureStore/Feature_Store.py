import subprocess
import pandas as pd
from feast import FeatureStore
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

def initiate_feature_store():
    # Ensure Feast is installed and initialized before running this script
    subprocess.run(["feast", "version"])
     
    # Initialize Feast repository
    subprocess.run(["feast", "init", "-m", "feature_repo"], cwd=".")
     
    

def getTransformedData():
    # Load the transformed data
    data = pd.read_csv('../Staging/Transformed_data.csv')
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
     
    predictors_df.to_parquet(path='feature_repo/feature_repo/data/predictors_df.parquet')
    target_df.to_parquet(path='feature_repo/feature_repo/data/target_df.parquet')
    
    #print(predictors_df.describe())
 
 
def historicalFeaturesFromFeatureStore():
    
    # Initialize FeatureStore
    store = FeatureStore(repo_path='feature_repo/feature_repo')
     
    # Read entity DataFrame from a Parquet file
    entity_df = pd.read_parquet(path='feature_repo/feature_repo/data/target_df.parquet')
     
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
            
        ]
    ).to_df()
    
    #dataset = store.create_saved_dataset(from_=training_data, name = "customer_churn_dataset", storage=SavedDataFileStorage('data/customer_churn_dataset.parquet'))
    #training_data
     
    training_df.to_csv('feature_repo/feature_repo/data/customer_churn_dataset.csv')
    print("Dataset created")
    
if __name__=='__main__':
    #initiate_feature_store()
    
    #getTransformedData()
    
    # Apply Feast configuration
    #subprocess.run(["feast", "apply"], cwd="feature_repo/feature_repo")
    
    historicalFeaturesFromFeatureStore()

