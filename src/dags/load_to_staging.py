from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

import vertica_python
import pendulum
import pandas as pd


vertica_connection_config = Variable.get("vertica_connection_config", deserialize_json=True)

conn_info = {"host": vertica_connection_config["host"],
            "port": vertica_connection_config["port"],
            "user": vertica_connection_config["user"],
            "password": vertica_connection_config["password"],
            "database": vertica_connection_config["database"],
            "autocommit": True
            }


# conn_info = {"host": "51.250.75.20",
#             "port": "5433",
#             "user": "EVGENIYAREFEVYANDEXRU",
#             "password": "fSX2ERi7fkz9AVb",
#             "database": "dwh",
#             "autocommit": True
#             }

vertica_conn = vertica_python.connect(**conn_info)

def load_from_csv_to_vertice(connection, data):
    df = pd.read_csv(f"/data/{data}.csv")
    
    if data == 'group_log':
        columns = 'group_id,user_id,user_id_from,event,event_datetime'
    else:
        columns = ','.join(df.columns)

    with connection as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            truncate table EVGENIYAREFEVYANDEXRU__STAGING.{data};
            """
        )
        cur.execute(
            f"""
            copy EVGENIYAREFEVYANDEXRU__STAGING.{data} ({columns})
            from local '/data/{data}.csv'
            delimiter ','
            ;
            """
        )
    conn.close()


@dag(
    schedule_interval=None, 
    start_date=pendulum.parse('2023-01-01')
)
def vertica_load_to_staging():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    files = ('users', 'groups', 'dialogs', 'group_log')
    fetch_tasks = [
        PythonOperator(
            task_id = f'load_{file_name}',
            python_callable = load_from_csv_to_vertice,
            op_kwargs = {
                'connection': vertica_conn,
                'data': file_name
            }
        ) for file_name in files
    ]

    start >> fetch_tasks >> end
    

    fetch_tasks

_ = vertica_load_to_staging()