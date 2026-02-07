from airflow.sdk import dag,task

@dag(
    dag_id='branches_dag'
)
def branches_dag():

    @task.python
    def first_task(**kwargs):
        print("Initiating First Task")
        ti = kwargs['ti']
        ti.xcom_push(key="raw_data_api",value=[1,2,3])
        ti.xcom_push(key="raw_data_db",value=[4,5,6])
        ti.xcom_push(key="raw_data_s3",value=[7,8,9])
        ti.xcom_push(key="weekend_flag",value="false")

    @task.python
    def second_task(**kwargs):
        print("Initiating Second Task")
        ti = kwargs['ti']
        raw_data_api = ti.xcom_pull(task_ids='first_task',key='raw_data_api')           
        transformed_data = [i*2 for i in raw_data_api]
        print("Transformed Data: ",transformed_data)
        ti.xcom_push(key="transformed_data_api",value=transformed_data)

    @task.python
    def third_task(**kwargs):
        print("Initiating Third Task")
        ti = kwargs['ti']
        raw_data_db = ti.xcom_pull(task_ids='first_task',key='raw_data_db')
        transformed_data = [i*2 for i in raw_data_db]
        print("Transformed Data: ",transformed_data)
        ti.xcom_push(key="transformed_data_db",value=transformed_data)

    @task.python
    def forth_task(**kwargs):
        print("Initiating Forth Task")
        ti = kwargs['ti']
        raw_data_s3 = ti.xcom_pull(task_ids='first_task',key='raw_data_s3')
        transformed_data = [i*2 for i in raw_data_s3]
        print("Transformed Data: ",transformed_data)
        ti.xcom_push(key="transformed_data_s3",value=transformed_data)

    @task.python
    def fifth_task(**kwargs):
        print("Initiating Fifth Task")
        ti = kwargs['ti']
        transformed_data_api = ti.xcom_pull(task_ids='second_task',key='transformed_data_api')
        transformed_data_db = ti.xcom_pull(task_ids='third_task',key='transformed_data_db')
        transformed_data_s3 = ti.xcom_pull(task_ids='forth_task',key='transformed_data_s3')
        final_data = transformed_data_api + transformed_data_db + transformed_data_s3
        print("Final Data: ",final_data)
        ti.xcom_push(key="final_data",value=final_data)

    @task.bash
    def sixth_task(**kwargs):
        print("Initiating Sixth Task")
        ti = kwargs['ti']
        final_data = ti.xcom_pull(task_ids='fifth_task',key='final_data')
        print("Final Data: ",final_data)
        return "echo Final Data: "+str(final_data)

    @task.branch
    def decider(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(task_ids='first_task',key='weekend_flag')
        if weekend_flag == "true":
            return "no_load_task"
        else:
            return "fifth_task"

    @task.bash
    def no_load_task(**kwargs):
        print("No Load Task on Weekends")
        return "echo No Load Task"

    first = first_task()
    second = second_task()
    third = third_task()
    forth = forth_task()
    no_load = no_load_task()
    fifth = fifth_task()
    sixth = sixth_task()
    
    #Only fifth and sixth to execute in serial manner if its not a weekend
    #if its a weekend, then only no_load task to execute
    dec = decider()

    # Unified chain
    first >> [second,third,forth] >> dec
    
    # Branch 1: Not Weekend
    dec >> fifth >> sixth

    # Branch 2: Weekend
    dec >> no_load

            
branches_dag()
