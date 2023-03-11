import sqlalchemy as sql
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def load_data_to_db():
    # Скачиваем данные в pandas Dataframe
    url_to_parse = "https://random-data-api.com/api/cannabis/random_cannabis?size=10"
    df = pd.read_json(url_to_parse, orient='records')

    # Указываем credentials для БД
    host_name = 'some_host'
    db_name = 'some_dbname'
    login = 'some_login'
    password = 'some_password'

    # Запрос для создания таблицы в базе, если её ещё нет
    create_table_query = '''CREATE TABLE IF NOT EXISTS cannabis_data (
                                id INTEGER,
                                uid VARCHAR(128),
                                strain VARCHAR,
                                cannabinoid_abbreviation VARCHAR,
                                cannabinoid VARCHAR,
                                terpene VARCHAR,
                                medical_use VARCHAR,
                                health_benefit VARCHAR,
                                category VARCHAR,
                                type VARCHAR,
                                buzzword VARCHAR,
                                brand VARCHAR
                            );'''

    # Создаём подключение к базе (я использую Postgres),
    # выполняем скрипт по созданию таблицы и загрузку данных из Dataframe
    engine = sql.create_engine(f'postgresql://{login}:{password}@{host_name}/{db_name}')
    with engine.connect() as connection:
        connection.execute(create_table_query)
        df.to_sql('cannabis_data', connection, if_exists='append', index=False)
    print("New data has been loaded to DB")


default_args = {
    'owner': 'Roman',
    'depends_on_past': False
}

# Создаём DAG для работы с airflow,
# указываем необходимые дату и время начала его выполнения и интервал (каждые 12 часов)
with DAG(
    'elt_process',
    default_args=default_args,
    start_date=datetime(2023, 3, 11, 0, 0, 0),
    schedule_interval='* */12 * * *',
    catchup=False
) as dag:
    tr_aggr = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_data_to_db,
        dag=dag
    )
