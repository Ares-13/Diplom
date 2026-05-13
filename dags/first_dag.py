from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

#Это словарь с настройками, которые автоматически применятся 
# ко всем задачам внутри DAG
default_args = {
    #если задача упадет с ошибкой, Airflow попытается 
    # перезапустить её еще 5 раз.
    'retries': 5,
    #Между попытками перезапуска будет пауза в 2 минуты
    'retry_delay': timedelta(minutes=2)
}

#Создание DAG (контейнера):
with DAG(
    dag_id='first_example',
    default_args=default_args,
    description='this is first DAG',
    start_date=datetime(2025,1,1),#datetime нужен, чтобы указать, когда начать запускать наш граф (дату старта)
    schedule_interval='@daily',
    #Если поставить True, Airflow попытается запустить этот граф за все 
    #пропущенные дни с 1 января 2025 года до сегодня. False означает "забудь про прошлое, 
    #запускай только новые интервалы с момента включения"
    catchup=False
) as dag:
    #создаем конкретную задачу (экземпляр оператора)
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command='echo it is a first DAG'
    )
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command='echo it is a 2nd task'
    )
    task3 = BashOperator(
        task_id = 'third_task',
        bash_command='echo it is a 3rd task'
    )
    task4 = BashOperator(
        task_id = 'fourth_task',
        bash_command='echo it is a 4th task'
    )
    ## Сначала task1 -> потом ПАРАЛЛЕЛЬНО task2 и task3 -> в конце task4
    task1 >> [task2, task3] >> task4