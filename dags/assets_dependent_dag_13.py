from airflow.sdk import dag,task,asset
from pendulum import datetime
import os
from assets_dag_12 import fetch_data

@asset(
    schedule = fetch_data,
    #This is where data will be store and is option but good to include 
    #for clarity about assets location.
    uri = "/opt/airflow/logs/data/data_processed.txt",
    name="process_data"
)
def process_data(self):
    data = "This is the data processed from the source"
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    with open(self.uri, "w") as f:
        f.write(data)
    
    print(f"Data Written to location: {self.uri}")
    