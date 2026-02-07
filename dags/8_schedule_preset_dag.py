from airflow.sdk import dag,task
from pendulum import datetime

@dag(
    dag_id='schedule_preset_dag',
    schedule='@daily',
    start_date=datetime(year=2026,month=1,day=1,tz="Asia/Kolkata"),
    catchup=False,
    is_paused_upon_creation=False
)
def schedule_preset_dag():
    @task.bash
    def first_task():
        print("Initiating First Task")
        return "echo First Task"
    
    first = first_task()

    first
    
schedule_preset_dag()