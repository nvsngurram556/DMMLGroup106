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
            if column in df.columns:
                if df[column].dtype != expected_type:
                    type_errors[column] = df[column].dtype
            else:
                type_errors[column] = 'Column not found'
        if type_errors:
            logging.info(f"Data type errors:\n{type_errors}")
        else:
            logging.info("No data type errors found.")
        return type_errors
    except Exception as e:
        logging.error(f"Error in validating data types: {e}")
        return None

def validate_data_formats(df, column, format_func):
    try:
        format_errors = df[~df[column].apply(format_func)]
        if not format_errors.empty:
            logging.info(f"Data format errors in column {column}:\n{format_errors}")
        return format_errors
    except Exception as e:
        logging.error(f"Error in validating data formats: {e}")
        return None

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
        logging.info("Format errors")
        for column, format_func in format_validations.items():
            report[f'format_errors_{column}'] = validate_data_formats(df, column, format_func)
        logging.info("Duplicates Data")
        report['duplicates'] = identify_duplicates(df)
        return report
    except Exception as e:
        logging.error(f"Error in generating data quality report: {e}")
        return None

if __name__ == "__main__":
    # Example usage
    df = read_all_csv_files('Staging/IN')
    expected_types = { 'customerid': 'object', 'gender': 'object', 'age': 'int64', 'under30': 'object', 'seniorcitizen': 'object', 'married': 'object', 'dependents': 'object', 'numberofdependents': 'int64', 'country': 'object', 'state': 'object', 'city': 'object', 'zipcode': 'object', 'latitude': 'float64', 'longitude': 'float64', 'population': 'int64', 'quarter': 'object', 'referredafriend': 'object', 'numberofreferrals': 'int64', 'tenureinmonths': 'int64', 'offer': 'object', 'phoneservice': 'object', 'avgmonthlylongdistancecharges': 'float64', 'multiplelines': 'object', 'internetservice': 'object', 'internettype': 'object', 'avgmonthlygbdownload': 'float64', 'onlinesecurity': 'object', 'onlinebackup': 'object', 'deviceprotectionplan': 'object', 'premiumtechsupport': 'object', 'streamingtv': 'object', 'streamingmovies': 'object', 'streamingmusic': 'object', 'unlimiteddata': 'object', 'contract': 'object', 'paperlessbilling': 'object', 'paymentmethod': 'object', 'monthlycharge': 'float64', 'totalcharges': 'float64', 'totalrefunds': 'float64', 'totalextradatacharges': 'float64', 'totallongdistancecharges': 'float64', 'totalrevenue': 'float64', 'satisfactionscore': 'int64', 'customerstatus': 'object', 'churnlabel': 'object', 'churnscore': 'int64', 'cltv': 'int64', 'churncategory': 'object', 'churnreason': 'object', 'ingestiondate': 'object' }
    format_validations = {'CustomerID': lambda x: x.str.len() == 6, 'Gender': lambda x: x.isin(['Male', 'Female']), 'Age': lambda x: x.between(0, 120), 'Under30': lambda x: x.isin(['Yes', 'No']), 'SeniorCitizen': lambda x: x.isin(['Yes', 'No']), 'Married': lambda x: x.isin(['Yes', 'No']), 'Dependents': lambda x: x.isin(['Yes', 'No']), 'NumberofDependents': lambda x: x.between(0, 10), 'Country': lambda x: x.str.len() > 0, 'State': lambda x: x.str.len() > 0, 'City': lambda x: x.str.len() > 0, 'ZipCode': lambda x: x.str.len() == 5, 'Latitude': lambda x: x.between(-90, 90), 'Longitude': lambda x: x.between(-180, 180), 'Population': lambda x: x > 0, 'Quarter': lambda x: x.str.len() > 0, 'ReferredaFriend': lambda x: x.isin(['Yes', 'No']), 'NumberofReferrals': lambda x: x >= 0, 'TenureinMonths': lambda x: x >= 0, 'Offer': lambda x: x.str.len() > 0, 'PhoneService': lambda x: x.isin(['Yes', 'No']), 'AvgMonthlyLongDistanceCharges': lambda x: x >= 0, 'MultipleLines': lambda x: x.isin(['Yes', 'No']), 'InternetService': lambda x: x.str.len() > 0, 'InternetType': lambda x: x.str.len() > 0, 'AvgMonthlyGBDownload': lambda x: x >= 0, 'OnlineSecurity': lambda x: x.isin(['Yes', 'No']), 'OnlineBackup': lambda x: x.isin(['Yes', 'No']), 'DeviceProtectionPlan': lambda x: x.isin(['Yes', 'No']), 'PremiumTechSupport': lambda x: x.isin(['Yes', 'No']), 'StreamingTV': lambda x: x.isin(['Yes', 'No']), 'StreamingMovies': lambda x: x.isin(['Yes', 'No']), 'StreamingMusic': lambda x: x.isin(['Yes', 'No']), 'UnlimitedData': lambda x: x.isin(['Yes', 'No']), 'Contract': lambda x: x.str.len() > 0, 'PaperlessBilling': lambda x: x.isin(['Yes', 'No']), 'PaymentMethod': lambda x: x.str.len() > 0, 'MonthlyCharge': lambda x: x >= 0, 'TotalCharges': lambda x: x >= 0, 'TotalRefunds': lambda x: x >= 0, 'TotalExtraDataCharges': lambda x: x >= 0, 'TotalLongDistanceCharges': lambda x: x >= 0, 'TotalRevenue': lambda x: x >= 0, 'SatisfactionScore': lambda x: x.between(1, 5), 'CustomerStatus': lambda x: x.str.len() > 0, 'ChurnLabel': lambda x: x.str.len() > 0, 'ChurnScore': lambda x: x.between(0, 100), 'CLTV': lambda x: x >= 0, 'ChurnCategory': lambda x: x.str.len() > 0, 'ChurnReason': lambda x: x.str.len() > 0, 'ingestiondate': lambda x: pd.to_datetime(x, errors='coerce').notnull()} # Example format validation
    report = generate_data_quality_report(df, expected_types, format_validations)
    logging.info(f"Data Quality Report:\n{report}")