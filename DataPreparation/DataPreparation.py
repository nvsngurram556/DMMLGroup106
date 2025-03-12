import os
import pandas as pd
import numpy as np
import glob
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from scipy import stats
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, MinMaxScaler, OneHotEncoder, LabelEncoder
from dateutil.relativedelta import relativedelta

# Configure logging
date_time = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
log_file = f'logs/dataPreparation_{date_time}.log'
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
        logging.info("All CSV files read successfully")
        return combined_df
    except:
        logging.error("Error in creating the directory")


# Load dataset
dataset = read_all_csv_files("Staging/IN")
logging.info("Datasets are merged into singel file loaded successfully.")
dataset['tenureinmonths'] = dataset['tenureinmonths'].astype(int)
# Calculate the customer joined date
dataset['customer_joined_date'] = pd.to_datetime(dataset['ingestiondate']) - dataset['tenureinmonths'].apply(lambda x: relativedelta(months=x))
drop_columns = ['tenureinmonths', 'totalcharges', 'totalrefunds', 'totalextradatacharges', 'totallongdistancecharges', 'totalrevenue']
dataset = dataset.drop(columns=drop_columns)
dataset_no_duplicates = dataset.drop_duplicates()

# Save the resulting DataFrame to a new CSV file
output_file_path = f"Staging/OUT/telecom_customer_cleaned_dataset_{date_time}.csv"
dataset_no_duplicates.to_csv(output_file_path, index=False)
logging.info(f"Data with duplicates removed saved as '{output_file_path}'.")

# Identify numerical and categorical columns
num_cols = dataset.select_dtypes(include=["number"]).columns.tolist()
cat_cols = dataset.select_dtypes(include=["object", "category"]).columns.tolist()

# Handle Missing Values
num_imputer = SimpleImputer(strategy="median")
dataset[num_cols] = num_imputer.fit_transform(dataset[num_cols])
logging.info("Missing values in numerical columns handled using median imputation.")

cat_imputer = SimpleImputer(strategy="most_frequent")
dataset[cat_cols] = cat_imputer.fit_transform(dataset[cat_cols])
logging.info("Missing values in categorical columns handled using mode imputation.")

# Save intermediate result
dataset[num_cols].to_csv("DataPreparation/Visualizations/handling_numerical_data.csv", index=False)
dataset[cat_cols].to_csv("DataPreparation/Visualizations/handling_categorical_data.csv", index=False)
logging.info("Imputed data saved as 'handling_numerical_data.csv' and 'handling_categorical_data.csv'.")
logging.info("Imputation completed successfully.")

# Standardize or Normalize Numerical Attributes
scaler = StandardScaler()  # or MinMaxScaler()
dataset[num_cols] = scaler.fit_transform(dataset[num_cols])
logging.info("Numerical attributes standardized using StandardScaler.")

# Save intermediate result
dataset.to_csv("DataPreparation/Visualizations/scaled_data.csv", index=False)
logging.info("Scaled data saved as 'scaled_data.csv'.")
logging.info("Standardization completed successfully.")

# Encode Categorical Variables
# One-hot encoding
dataset = pd.get_dummies(dataset, columns=cat_cols, drop_first=True)
logging.info("Categorical variables one-hot encoded.")

# Label encoding for ordinal categories (if applicable)
if 'ordinal_col' in dataset.columns:
    le = LabelEncoder()
    dataset['ordinal_col'] = le.fit_transform(dataset['ordinal_col'])
    logging.info("Ordinal column label encoded.")
    dataset['ordinal_col'].to_csv("DataPreparation/Visualizations/encoded_data.csv", index=False)
    logging.info("Encoded data saved as 'encoded_data.csv'.")

dataset_outliers = dataset.copy()
# Detect Outliers using Z-score and IQR
outliers = {}
for col in num_cols:
    # Z-score method
    z_scores = np.abs(stats.zscore(dataset_outliers[col]))
    dataset_outliers[f'{col}_outlier_z'] = z_scores > 3
    
    # IQR method
    Q1 = dataset_outliers[col].quantile(0.25)
    Q3 = dataset_outliers[col].quantile(0.75)
    IQR = Q3 - Q1
    dataset_outliers[f'{col}_outlier_iqr'] = (dataset_outliers[col] < (Q1 - 1.5 * IQR)) | (dataset_outliers[col] > (Q3 + 1.5 * IQR))
    
    outliers[col] = {
        'z_score_outliers': dataset_outliers[f'{col}_outlier_z'].sum(),
        'iqr_outliers': dataset_outliers[f'{col}_outlier_iqr'].sum()
    }

logging.info(f"Outlier detection completed: {outliers}")



# Exploratory Data Analysis (EDA)

histogram_dir = "DataPreparation/Visualizations/Histograms"
os.makedirs(histogram_dir, exist_ok=True)
for col in num_cols:
    plt.figure()
    dataset_no_duplicates[col].hist(figsize=(10, 8), bins=30)
    plt.title(f'Histogram of {col}')
    hist_path = os.path.join(histogram_dir, f'{col}_histogram.png')
    plt.savefig(hist_path)
    plt.close()
    logging.info(f"Histogram for {col} saved at {hist_path}")


boxplot_dir = "DataPreparation/Visualizations/Boxplots"
os.makedirs(boxplot_dir, exist_ok=True)

for col in num_cols:
    plt.figure()
    sns.boxplot(data=dataset_no_duplicates[[col]])
    plt.title(f'Box plot of {col}')
    boxplot_path = os.path.join(boxplot_dir, f'{col}_boxplot.png')
    plt.savefig(boxplot_path)
    plt.close()
    logging.info(f"Box plot for {col} saved at {boxplot_path}")

logging.info("Data preprocessing, outlier detection, and EDA completed successfully.")
