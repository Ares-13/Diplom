from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance

#Это словарь с настройками, которые автоматически применятся 
# ко всем задачам внутри DAG
default_args = {
    #если задача упадет с ошибкой, Airflow попытается 
    # перезапустить её еще 5 раз.
    'retries': 5,
    #Между попытками перезапуска будет пауза в 2 минуты
    'retry_delay': timedelta(minutes=2)
}


def hello(**context):
    ti: TaskInstance = context["ti"]
    xcom_str1 = ti.xcom_pull(task_ids='get_hello_str_xcom', key='xcom_str1')
    xcom_str2 = ti.xcom_pull(task_ids='get_hello_str_xcom', key='xcom_str2')
    xcom_number = ti.xcom_pull(task_ids='get_number_xcom', key='xcom_number')
    print(f"{xcom_str1},{xcom_str2} add_number is {xcom_number}")


def get_hello_str_xcom(**context):
    ti: TaskInstance = context["ti"]
    ti.xcom_push(key='xcom_str1', value= "Hello, i am xcom string!")
    ti.xcom_push(key='xcom_str2', value= "i am another xcom string!")


def get_number_xcom(**context):
    ti: TaskInstance = context["ti"]
    ti.xcom_push(key="xcom_number", value= 21)

#Создание DAG (контейнера):
with DAG(
    dag_id='xcom_dag_dag_v01',
    default_args=default_args,
    description='This is xcom_dag',
    start_date=datetime(2025,1,1),#datetime нужен, чтобы указать, когда начать запускать наш граф (дату старта)
    schedule_interval='@daily',
    #Если поставить True, Airflow попытается запустить этот граф за все 
    #пропущенные дни с 1 января 2025 года до сегодня. False означает "забудь про прошлое, 
    #запускай только новые интервалы с момента включения"
    catchup=False
) as dag:
    #создаем конкретную задачу (экземпляр оператора)
    task1 = PythonOperator(
        task_id = 'python_task',
        python_callable=hello,
        op_kwargs={"add_number": 5},
    )
    
    task2 = PythonOperator(
        task_id = 'get_hello_str_xcom',
        python_callable=get_hello_str_xcom,
    )

    task3 = PythonOperator(
        task_id = 'get_number_xcom',
        python_callable=get_number_xcom,
    )

    [task3, task2] >> task1 