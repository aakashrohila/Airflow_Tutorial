from airflow.sdk import dag,task

@dag(
    dag_id="xcoms_dag_kwargs"
)
def xcoms_dag_kwargs():
    
    @task.python
    def first_task(**kwargs):
        print("Initiating First Task...")
        raw_data = [1,2,3,4,5]
        #ti : task instance
        ti = kwargs['ti']
        ti.xcom_push(key="first_task_val",value=raw_data)

    @task.python
    def second_task(**kwargs):
        print("Initiating second Task.")
        ti = kwargs['ti']
        first_task_val = ti.xcom_pull(task_ids='first_task',key='first_task_val')
        transformed_even_data = [i for i in first_task_val if i%2==0]
        print("Even: ",transformed_even_data)

        ti.xcom_push(key="second_task_val",value=transformed_even_data)
        

    @task.python
    def third_task(**kwargs):
        print("Initiating third Task.")
        ti = kwargs['ti']
        first_task_val = ti.xcom_pull(task_ids='first_task',key='first_task_val')
        transformed_odd_data = [i for i in first_task_val if i%2!=0]
        print("Odd: ",transformed_odd_data)


    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

xcoms_dag_kwargs()