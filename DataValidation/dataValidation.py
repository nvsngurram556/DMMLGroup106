import pandas as pd
import glob
import os
import logging

# Logging configuration
date_time = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
log_file = f'logs/dataValidation_{date_time}.log'
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
    

def check_missing_data(df):
    try:
        missing_data = df.isnull().sum()
        logging.info(f"Missing data:\n{missing_data}")
        return missing_data
    except:
        logging.error("Error in checking missing data")

def check_inconsistent_data(df):
    try:
        inconsistent_data = df[df.duplicated()]
        logging.info(f"Inconsistent data:\n{inconsistent_data}")
        return inconsistent_data
    except:
        logging.error("Error in checking inconsistent data")

def validate_data_types(df, expected_types):
    try:
        type_errors = {}
        for column, expected_type in expected_types.items():
            if not df[column].dtype == expected_type:
                type_errors[column] = df[column].dtype
        logging.info(f"Data type errors:\n{type_errors}")
        return type_errors
    except:
        logging.error("Error in validating data types")

def validate_data_formats(df, column, format_func):
    try:
        format_errors = df[~df[column].apply(format_func)]
        logging.info(f"Data format errors in column {column}:\n{format_errors}")
        return format_errors
    except:
        logging.error("Error in validating data formats")

def identify_duplicates(df):
    try:
        duplicates = df[df.duplicated()]
        logging.info(f"Duplicate records:\n{duplicates}")
        return duplicates
    except:
        logging.error("Error in identifying duplicates")

def generate_data_quality_report(df, expected_types, format_validations):
    try:
        report = {}
        logging.info("Missing Data") 
        report['missing_data'] = check_missing_data(df)
        logging.info("Inconsistent Data") 
        report['inconsistent_data'] = check_inconsistent_data(df)
        logging.info("Type errors") 
        report['type_errors'] = validate_data_types(df, expected_types)
        logging.info("format errors")
        for column, format_func in format_validations.items():
            report[f'format_errors_{column}'] = validate_data_formats(df, column, format_func)
        logging.info("Duplicates Data") 
        report['duplicates'] = identify_duplicates(df)
        return report
    except:
        logging.error("Error in generating data quality report")

if __name__ == "__main__":
    # Example usage
    df = read_all_csv_files('Staging')
    expected_types = {'CustomerID': 'object', 'Gender': 'object', 'Age': 'int64', 'Under30': 'object', 'SeniorCitizen': 'object', 'Married': 'object', 'Dependents': 'object', 'NumberofDependents': 'int64', 'Country': 'object', 'State': 'object','City': 'object', 'ZipCode': 'int64', 'Latitude': 'float64', 'Longitude': 'float64', 'Population': 'int64', 'Quarter': 'object', 'ReferredaFriend': 'object', 'NumberofReferrals': 'int64', 'TenureinMonths': 'int64', 'Offer': 'object', 'PhoneService': 'object', 'AvgMonthlyLongDistanceCharges': 'int64', 'MultipleLines': 'object', 'InternetService': 'object', 'InternetType': 'object', 'AvgMonthlyGBDownload': 'int64', 'OnlineSecurity': 'object', 'OnlineBackup': 'object', 'DeviceProtectionPlan': 'object', 'PremiumTechSupport': 'object', 'StreamingTV': 'object', 'StreamingMovies': 'object', 'StreamingMusic': 'object', 'UnlimitedData': 'object', 'Contract': 'object', 'PaperlessBilling': 'object', 'PaymentMethod': 'object', 'MonthlyCharge': 'float64', 'TotalCharges': 'float64', 'TotalRefunds': 'float64', 'TotalExtraDataCharges': 'int64', 'TotalLongDistanceCharges': 'float64', 'TotalRevenue': 'float64', 'SatisfactionScore': 'int64', 'CustomerStatus': 'object', 'ChurnLabel': 'object', 'ChurnScore': 'int64', 'CLTV': 'int64', 'ChurnCategory': 'object', 'ChurnReason': 'object', 'ingestiondate': 'date'}
    format_validations = {'CustomerID': lambda x: x.str.len().max(), 'Gender': lambda x: x.str.len().max(), 'Age': lambda x: len(str(x.max())), 'Under30': lambda x: x.str.len().max(), 'SeniorCitizen': lambda x: x.str.len().max(), 'Married': lambda x: x.str.len().max(), 'Dependents': lambda x: x.str.len().max(), 'NumberofDependents': lambda x: len(str(x.max())), 'Country': lambda x: x.str.len().max(), 'State': lambda x: x.str.len().max(),'City': lambda x: x.str.len().max(), 'ZipCode': lambda x: len(str(x.max())), 'Latitude': lambda x: len(str(x.max())), 'Longitude': lambda x: len(str(x.max())), 'Population': lambda x: len(str(x.max())), 'Quarter': lambda x: x.str.len().max(), 'ReferredaFriend': lambda x: x.str.len().max(), 'NumberofReferrals': lambda x: len(str(x.max())), 'TenureinMonths': lambda x: len(str(x.max())), 'Offer': lambda x: x.str.len().max(), 'PhoneService': lambda x: x.str.len().max(), 'AvgMonthlyLongDistanceCharges': lambda x: len(str(x.max())), 'MultipleLines': lambda x: x.str.len().max(), 'InternetService': lambda x: x.str.len().max(), 'InternetType': lambda x: x.str.len().max(), 'AvgMonthlyGBDownload': lambda x: len(str(x.max())), 'OnlineSecurity': lambda x: x.str.len().max(), 'OnlineBackup': lambda x: x.str.len().max(), 'DeviceProtectionPlan': lambda x: x.str.len().max(), 'PremiumTechSupport': lambda x: x.str.len().max(), 'StreamingTV': lambda x: x.str.len().max(), 'StreamingMovies': lambda x: x.str.len().max(), 'StreamingMusic': lambda x: x.str.len().max(), 'UnlimitedData': lambda x: x.str.len().max(), 'Contract': lambda x: x.str.len().max(), 'PaperlessBilling': lambda x: x.str.len().max(), 'PaymentMethod': lambda x: x.str.len().max(), 'MonthlyCharge': lambda x: len(str(x.max())), 'TotalCharges': lambda x: len(str(x.max())), 'TotalRefunds': lambda x: len(str(x.max())), 'TotalExtraDataCharges': lambda x: len(str(x.max())), 'TotalLongDistanceCharges': lambda x: len(str(x.max())), 'TotalRevenue': lambda x: len(str(x.max())), 'SatisfactionScore': lambda x: len(str(x.max())), 'CustomerStatus': lambda x: x.str.len().max(), 'ChurnLabel': lambda x: x.str.len().max(), 'ChurnScore': lambda x: len(str(x.max())), 'CLTV': lambda x: len(str(x.max())), 'ChurnCategory': lambda x: x.str.len().max(), 'ChurnReason': lambda x: x.str.len().max(), 'ingestiondate': lambda x: x.str.len().max()} # Example format validation
    report = generate_data_quality_report(df, expected_types, format_validations)
    logging.info(f"Data Quality Report:\n{report}")