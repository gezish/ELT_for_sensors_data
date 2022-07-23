from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime as dt
from datetime import timedelta


default_args = {
    'owner': 'gezahegne',
    'depends_on_past': False,
    'email': ['enggezahegn.w@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': dt(2022, 7, 22),
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'create_tables',
    default_args=default_args,
    description='An Airflow DAG to create tables',
    schedule_interval='@once',
)

create_allstation_table = MySqlOperator(
    task_id='create_table_allstation',
    mysql_conn_id='mysql_conn_id',
    sql='/dbt/models/staging/stg_sample.sql',
    dag=dag,
)

email = EmailOperator(task_id='send_email',
                      to='enggezahegn.w@gmail.com',
                      subject='Daily report generated',
                      html_content=""" <h1>Congratulations! The tables are created.</h1> """,
                      dag=dag,
                      )


create_allstation_table# >> email
