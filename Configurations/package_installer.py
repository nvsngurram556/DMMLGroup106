import subprocess

subprocess.check_call(["pip", "install", "pandas", "numpy", "requests", "schedule", "psycopg2", "kagglehub", "kaggle","kagglehub[pandas-datasets]", "airflow", "irflow.operators.dummy_operator", "airflow.operators.python_operator"])