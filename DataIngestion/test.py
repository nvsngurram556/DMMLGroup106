import os
import pandas as pd
import glob
from dateutil.relativedelta import relativedelta
from sklearn.preprocessing import StandardScaler, MinMaxScaler

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
dataset['customer_tenure_months'] = dataset.apply(lambda row: relativedelta(pd.to_datetime(row['ingestiondate']), pd.to_datetime(row['customer_joined_date'])).months + relativedelta(pd.to_datetime(row['ingestiondate']), pd.to_datetime(row['customer_joined_date'])).years * 12, axis=1)

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

dataset.to_csv("Staging/Transformed_data.csv", index=False)

print(dataset.info())