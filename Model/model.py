import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import joblib
import os
import logging
import glob
from datetime import datetime

# Logging configuration
date_time = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
log_file = f'logs/model_{date_time}.log'
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

# 1. Load preprocessed dataset
df = read_all_csv_files("Outputfiles")
logging.info(f"Dataset loaded with {df.shape[0]} rows and {df.shape[1]} columns")

# 2. Separate features and target
if "churnlabel" not in df.columns:
    logging.warning("Target column 'churn' not found in the dataset")
else:
    X = df.drop(columns=["churnlabel"])
    logging.info("Target column 'churn' found in the dataset")

y = df["churnlabel"]


# Identify categorical features
categorical_features = X.select_dtypes(include=['object']).columns.tolist()

# Create a column transformer with one-hot encoding for categorical features
preprocessor = ColumnTransformer(
    transformers=[
        ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
    ],
    remainder='passthrough'
)


# 3. Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
logging.info(f"Train-test split: {X_train.shape[0]} train rows, {X_test.shape[0]} test rows")
# 4. Initialize models
models = {
    "LogisticRegression": Pipeline(steps=[('preprocessor', preprocessor), ('classifier', LogisticRegression(max_iter=1000))]),
    "RandomForest": Pipeline(steps=[('preprocessor', preprocessor), ('classifier', RandomForestClassifier(n_estimators=100))])
}

model_performance = {}

# 5. Train and evaluate models
for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, pos_label='Yes')
    recall = recall_score(y_test, y_pred, pos_label='Yes')
    f1 = f1_score(y_test, y_pred, pos_label='Yes')

    model_performance[name] = {
        "model": model,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1
    }

    logging.info(f"\n {name} Performance:")
    logging.info(f"  Accuracy : {accuracy:.4f}")
    logging.info(f"  Precision: {precision:.4f}")
    logging.info(f"  Recall   : {recall:.4f}")
    logging.info(f"  F1 Score : {f1:.4f}")

# 6. Select and save best model
best_model_name = max(model_performance, key=lambda k: model_performance[k]["f1_score"])
logging.info(f"Best model selected: {best_model_name}")
best_model = model_performance[best_model_name]["model"]
logging.info(f"Best model performance: {model_performance[best_model_name]}")

# Create versioned model filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
model_dir = "Model"
os.makedirs(model_dir, exist_ok=True)
model_filename = f"{model_dir}/churn_model_{best_model_name}_{timestamp}.joblib"
logging.info(f"Saving best model as: {model_filename}")

joblib.dump(best_model, model_filename)
logging.info(f"\n Best model ({best_model_name}) saved as: {model_filename}")
logging.info("Model training completed")