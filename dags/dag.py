from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from census import *


#timedelta(seconds=60)
default_args = {
    'start_date': datetime(2022, 6, 5)
}

with DAG(dag_id='census',
         default_args=default_args,
         schedule_interval=timedelta(seconds=10),
         catchup=False,
         tags=['currency']
         ) as dag:

    with TaskGroup('extract_tasks') as extract_tasks:
    # busca dados arquivo data

        t1 = PythonOperator(
            task_id='extract_data',
            python_callable=extract_data1,
            do_xcom_push=False,
            dag=dag,
        )

        # busca dados arquivo test
        t2 = PythonOperator(
            task_id='extract_test',
            python_callable=extract_data2,
            do_xcom_push=False,
            dag=dag,
        )

    # aplicar transformações na tabela
    t3 = PythonOperator(
        task_id='transforma_tabela',
        python_callable=transform_data,
        do_xcom_push=False,
        dag=dag,
    )

    # cria tabela
    t4 = PostgresOperator(
        task_id='criar_table',
        postgres_conn_id='postgres_default',
        sql=r"""
            CREATE TABLE IF NOT EXISTS census (
                id_census             SERIAL PRIMARY KEY,
                age                   INT,
                workclass             VARCHAR(20),
                fnlwgt                FLOAT,
                education             VARCHAR(20),
                education_num         INT,
                marital_status        VARCHAR(30),
                occupation            VARCHAR(20),
                relationship          VARCHAR(30),
                race                  VARCHAR(30),
                sex                   VARCHAR(10),
                capital_gain          FLOAT,
                capital_loss          FLOAT,
                hours_per_week        INT,
                native_country        VARCHAR(30),
                class                 VARCHAR(10)
                )
        """,
        dag=dag,
    )

    t5 = PythonOperator(
        
        task_id='populate_table',
        python_callable=populate_table,
        do_xcom_push=False,
        dag=dag,
    )

    # dependências entre as tarefas
    extract_tasks >> t3 >> t4 >> t5
