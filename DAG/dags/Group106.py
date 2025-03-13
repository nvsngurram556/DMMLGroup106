import subprocess
import datetime
from prefect import task, flow

@task
def run_DataIngestion():
    dataIngestion_result = subprocess.run(["python", "DataIngestion/dataIngestionApiInputFile.py"])
    return dataIngestion_result.stdout, dataIngestion_result.stderr

@task
def run_RawDataStorage():
    rawDataStorage = subprocess.run(["python", "RawDataStorage/rawDataStorage.py"])
    return rawDataStorage.stdout, rawDataStorage.stderr

@task
def run_DataValidation():
    dataValidation = subprocess.run(["python", "DataValidation/dataValidation.py"])
    return dataValidation.stdout, dataValidation.stderr

@task
def run_DataPreparation():
    dataPreparation = subprocess.run(["python", "DataPreparation/dataPreparation.py"])
    return dataPreparation.stdout, dataPreparation.stderr

@task
def run_DataTransformation():
    dataTransformation = subprocess.run(["python", "DataTransformation/dataTransformation.py"])
    return dataTransformation.stdout, dataTransformation.stderr

@task
def run_FeatureStore():
    dataFeatureStore = subprocess.run(["python", "FeatureStore/featureStore.py"])
    return dataFeatureStore.stdout, dataFeatureStore.stderr

@task
def run_Model():
    dataModel = subprocess.run(["python", "Model/model.py"])
    return dataModel.stdout, dataModel.stderr


@flow
def DMMLGroup106() -> str:
    run_DataIngestion()
    run_RawDataStorage()
    run_DataValidation()
    run_DataPreparation()
    run_DataTransformation()
    run_FeatureStore()
    run_Model()
    return "Done"

if __name__ == "__main__":
    deployment = DMMLGroup106.serve(
        name="group106deployment",
        cron="0 * * * *"
        )
    deployment.apply()
    print("Deployment created with daily schedule")