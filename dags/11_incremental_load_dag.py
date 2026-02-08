from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    dag_id='incremental_load_dag',
    schedule=CronDataIntervalTimetable("@daily",timezone="Asia/Kolkata"),
    start_date=datetime(year=2026,month=1,day=1,tz="Asia/Kolkata"),
    catchup=True,
)
def incremental_load_dag():

    @task.python
    def incremental_data_fetch(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Extracting data from {date_interval_start} to {date_interval_end}")

    @task.bash
    def incremental_data_process():
        return "echo 'Processing incremental data from {date_interval_start} to {date_interval_end}'"

    fetch = incremental_data_fetch()
    process = incremental_data_process()

    fetch >> process

incremental_load_dag()