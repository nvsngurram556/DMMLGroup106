import pandas as pd
import logging
import os
import glob

# === Configuration ===

# Logging configuration
date_time = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
log_file = f'logs/dataValidation_{date_time}.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s') 

report_output = f'DataValidation/validation_report_{date_time}.csv'  

# Expected schema: column -> expected type
expected_schema = {
    'customerid': 'object', 'gender': 'object', 'age': 'int64', 
    'under30': 'object', 'seniorcitizen': 'object', 'married': 'object', 
    'dependents': 'object', 'numberofdependents': 'int64', 
    'country': 'object', 'state': 'object', 'city': 'object', 
    'zipcode': 'object', 'latitude': 'float64', 'longitude': 'float64', 
    'population': 'int64', 'quarter': 'object', 'referredafriend': 'object', 
    'numberofreferrals': 'int64', 'tenureinmonths': 'int64', 
    'offer': 'object', 'phoneservice': 'object', 
    'avgmonthlylongdistancecharges': 'float64', 'multiplelines': 'object', 
    'internetservice': 'object', 'internettype': 'object', 
    'avgmonthlygbdownload': 'float64', 'onlinesecurity': 'object', 
    'onlinebackup': 'object', 'deviceprotectionplan': 'object', 
    'premiumtechsupport': 'object', 'streamingtv': 'object', 
    'streamingmovies': 'object', 'streamingmusic': 'object', 
    'unlimiteddata': 'object', 'contract': 'object', 
    'paperlessbilling': 'object', 'paymentmethod': 'object', 
    'monthlycharge': 'float64', 'totalcharges': 'float64', 
    'totalrefunds': 'float64', 'totalextradatacharges': 'float64', 
    'totallongdistancecharges': 'float64', 'totalrevenue': 'float64', 
    'satisfactionscore': 'int64', 'customerstatus': 'object', 
    'churnlabel': 'object', 'churnscore': 'int64', 'cltv': 'int64', 
    'churncategory': 'object', 'churnreason': 'object', 
    'ingestiondate': 'object'
}




def detect_format(column):
    if column.dtype == 'object':
      format_detected = 'text'
    else:
      format_detected = 'numeric'
    return format_detected


# === Schema Validation Logic ===
def validation(df, expected_schema, report_output):
    for column, expected_type in expected_schema.items():
        if column in df.columns:
            actual_type = df[column].dtype  # Get actual data type
            match_status = 'Match' if actual_type == expected_type else 'Mismatch'
            missing = df[column].isna().sum()
            percent_missing = (missing / len(df)) * 100
            sample_values = df[column].dropna().unique()[:3]
            inconsistant = 0
            if detect_format(df[column]) == 'numeric':
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
                inconsistant = len(outliers)
            else:
                actual_type = 'N/A'  # Column not found
                match_status = 'Missing'
        
            report.append({
                'Column': column,
                'Expected Type': expected_type,
                'Actual Type': actual_type,
                'Status': match_status
            })
            report.append({
                'Column': column,
                'Data Type': str(df[column].dtype),
                'Detected Format': actual_type,
                'Expected Format': expected_type,
                'Match Status': match_status,
                'Missing Values': missing,
                'Percent Missing': f"{percent_missing:.2f}%",
                'Inconsistent Values': inconsistant,
            })
            logging.info(f"Column {column} validation results are added as a row into the report {report_output}")


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

def identify_duplicates(df):
    try:
        duplicates = df[df.duplicated()]
        logging.info(f"Duplicate records:\n{duplicates}")
        return duplicates
    except:
        logging.error("Error in identifying duplicates")


# === Load CSV ===
df = read_all_csv_files('Staging/IN/')
logging.info("All staging files are loaded into dataframe for validation\n")
report = []
validation(df, expected_schema, report_output)
identify_duplicates(df)
report_df = pd.DataFrame(report)
report_df.to_csv(report_output, index=False)
logging.info(f"Report is exported to {report_output}")