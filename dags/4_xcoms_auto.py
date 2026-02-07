from airflow.sdk import dag,task

@dag(
    dag_id="xcoms_dag_auto",
    # start_date=datetime(2023,1,1),
    # schedule_interval="@daily",
    # catchup=False
)
def xcoms_dag_auto():
    
    @task.python
    def first_task():
        print("Extracting Data...This is the first task")
        fetched_data = {"data":[1,2,3,4,5]}
        return fetched_data

    @task.python
    def second_task(data:dict):
        fetched_data = data['data']
        transformed_data = fetched_data*2
        transformed_data_dict = {"tran_data":transformed_data}
        return transformed_data_dict

    @task.python
    def third_task(data:dict):
        load_data = data
        return load_data

    first = first_task()
    second = second_task(first)
    third = third_task(second)

    #This order is not required as Airflow 3.0 is smart enough to understand the dependencies 
    # first >> second >> third >> forth

xcoms_dag_auto()