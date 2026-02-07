from airflow.sdk import dag,task
from airflow.operators.bash import BashOperator

@dag(
    dag_id="operators_dag",
    # start_date=datetime(2023,1,1),
    # schedule_interval="@daily",
    # catchup=False
)
def operators_dag():
    
    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.bash
    def modern_bash():
        print("This is the modern bash task")
        return "echo Hello from modern bash"

    def old_bash():
        print("This is the old bash task")
        return BashOperator(
            task_id="old_bash",
            bash_command="echo Hello from old bash"
        )

    first = first_task()
    second = second_task()
    third = modern_bash()
    forth = old_bash()

    first >> second >> third >> forth

operators_dag()