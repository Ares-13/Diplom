from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

#Это словарь с настройками, которые автоматически применятся 
# ко всем задачам внутри DAG
default_args = {
    #если задача упадет с ошибкой, Airflow попытается 
    # перезапустить её еще 5 раз.
    'retries': 5,
    #Между попытками перезапуска будет пауза в 2 минуты
    'retry_delay': timedelta(minutes=2)
}


def hello(add_str: str, add_num: int):
    print(f"Hi, i am python operator, STR: {add_str} and NUM: {add_num}")

#Создание DAG (контейнера):
with DAG(
    dag_id='python_operator_dag_v2',
    default_args=default_args,
    description='this is python_operator_dag_v2',
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
        op_kwargs={"add_str": "GOOD", "add_num": 5},
    )
    
    task1 