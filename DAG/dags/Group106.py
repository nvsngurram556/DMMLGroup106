import subprocess
#import datetime
from prefect import task, flow
#from prefect.schedules import IntervalSchedule

@task
def run_DataIngestion():
    dataIngestion_result = subprocess.run(["python", "DataIngestion/dataIngestionApiInputFile.py"])
    #schedule = IntervalSchedule(interval=datetime.timedelta(hours=1))
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
    dataPreparation = subprocess.run(["python", "DataIngestion/dataPreparation.py"])
    return dataPreparation.stdout, dataPreparation.stderr

@task
def run_DataTransformation():
    dataTransformation = subprocess.run(["python", "DataIngestion/dataTransformation.py"])
    return dataTransformation.stdout, dataTransformation.stderr

@flow
def my_flow() -> str:
    run_DataIngestion()
    run_RawDataStorage()
    run_DataValidation()
    run_DataPreparation()
    run_DataTransformation()
    return "Done"

if __name__ == "__main__":
    print(my_flow())