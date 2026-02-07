from airflow.sdk import dag,task

@dag(
    dag_id="versioned_dag",    
)
def versioned_dag():

    @task.python
    def first_dag():
        print("This is the versioned dag")

    @task.python
    def second_dag():
        print("This is the second task")

    @task.python
    def third_dag():
        print("This is the third task")

    @task.python
    def forth_dag():
        print("This is the forth task")

    first = first_dag()
    second = second_dag()
    third = third_dag()
    forth = forth_dag()

    first >> second >> third >> forth

versioned_dag()