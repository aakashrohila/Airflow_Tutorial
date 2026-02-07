from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    dag_id='schedule_cron_dag',
    schedule=CronDataIntervalTimetable('45 18 * * *',timezone='Asia/Kolkata'),
    start_date=datetime(year=2026,month=1,day=1,tz="Asia/Kolkata"),
    catchup=False
)
def schedule_cron_dag():
    @task.bash
    def first_task():
        print("Initiating First Task")
        return "echo First Task"
    
    first = first_task()

    first
    
schedule_cron_dag()