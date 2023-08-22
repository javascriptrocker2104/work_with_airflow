from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import psycopg2
from psycopg2 import sql

default_args = {
    "owner": "airflow",
    'start_date': datetime(2023,8,9)
}

dag=DAG(
    dag_id="first_dag",
    default_args=default_args,
    schedule_interval='*/10****',
    catchup=False
)

def get_rate(
url = 'https://api.exchangerate.host/latest?base=BTC&symbols=RUB'
response = requests.get(url)
data = response.json()

#созданием словари
lst = []
lst.append((data['date'], data['rates']['RUB']))

#устанавливаем соединение
conn = psycopg2.connect(dbname='database', 
                              user='postgres', 
                              password='get_rate', 
                              host='db',
                              port=5430)

with conn.cursor() as cursor:
    conn.autocommit = True
    #загрузка словарей в бд PostgresSQL
    insert = sql.SQL('INSERT INTO rate (rate_date, rate_amount) VALUES {}').format(sql.SQL(',').join(map(sql.Literal, lst)))
    cursor.execute(insert)
conn.commit()
#закрываем соединение
conn.close()
)

task1 = BashOperator(
    task_id = 'bash_task',
    bash_command = "echo 'Good morning my diggers!'"
    dag=dag)

task2=PythonOperator(
    task_id="exchange rate",
    python_callable=get_rate,
    dag=dag
)

task1>>task2
