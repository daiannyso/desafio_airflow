from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta, date
from textwrap import dedent
from airflow.operators.python import PythonOperator
from airflow import DAG
import pandas as pd
import sqlite3
import random
import string

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['daianny.oliveira@indicium.tech'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Função para carregar Northwind_small.sqlite para csv
def load_sqlite_to_csv():
    conn = sqlite3.connect('data/Northwind_small.sqlite', isolation_level=None, detect_types=sqlite3.PARSE_COLNAMES)
    db_df = pd.read_sql("SELECT * FROM 'Order'", conn)
    db_df.to_csv('output_orders.csv', index=False)

# Função para extrair count para txt
def count_to_txt():
    conn = sqlite3.connect('data/Northwind_small.sqlite', isolation_level=None, detect_types=sqlite3.PARSE_COLNAMES)
    df = pd.read_sql("SELECT * FROM 'OrderDetail'", conn)
    df_2 = pd.read_csv('output_orders.csv')
    df_join = pd.merge(df, df_2, left_on='OrderId', right_on='Id', how='left')
    df_queried = df_join.query('ShipCity == "Rio de Janeiro"')
    total = df_queried['Quantity'].sum()
    f = open("count.txt", "w")
    print(str(total), file=f)
    f.close()

# Task para gerar arquivo final_output.txt
def export_final_output():
    res = ''.join(random.choices(string.ascii_letters,k=7))
    f = open("final_output.txt", "w")
    print(str(res), file=f)
    f.close()

with DAG(
    'NorthwindELT',
    default_args=default_args,
    description='A ELT dag for the Northwind ECommerceData',
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 10, 10),
    catchup=False,
    tags=[],
) as dag:
    dag.doc_md = """
        ELT Diária do banco de dados de ecommerce Northwind,
        começando em 2024-10-10. 
    """
    # Task para carregar Northwind_small.sqlite para csv
    load_sqlite_to_csv_task = PythonOperator(
        task_id='load_sqlite_to_csv',
        python_callable=load_sqlite_to_csv,
    )

    # Task para extrair count para txt
    count_to_txt_task = PythonOperator(
        task_id='count_to_txt',
        python_callable=count_to_txt,
    )

    # Task para gerar arquivo final_output.txt
    export_final_output_task = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_output,
    )

    # Dependências
    load_sqlite_to_csv_task >> count_to_txt_task >> export_final_output_task