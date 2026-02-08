from airflow.sdk import dag,task, asset
from pendulum import datetime
import os

@asset(
    schedule = "@daily",
    #This is where data will be store and is option but good to include 
    #for clarity about assets location.
    uri = "/opt/airflow/logs/data/data_extract.txt",
    name="fetch_data"
)
def fetch_data(self):
    data = "This is the data extracted from the source"
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    with open(self.uri, "w") as f:
        f.write(data)
    
    print(f"Data Written to location: {self.uri}")
    