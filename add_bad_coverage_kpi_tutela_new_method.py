## Author : Faiz Abdussalam
## Last Edit: 30 Jan 2023
## Note :

import logging
import shutil
import time
import json
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta, TH


from airflow import DAG
from airflow.decorators import task
from airflow.macros import ds_format, ds_add
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


log = logging.getLogger(__name__)


with DAG(
    dag_id='add_bad_coverage_kpi_tutela_new_method',
    schedule=None,
    start_date=datetime(2023, 2, 7),
    catchup=False,
    tags=['covmo','weekly','bad_coverage','tutela'],
    max_active_runs=1
) as dag:
    

    def yearweekNow():
        todayte = date.today() - timedelta(days=7)
        src = PostgresHook(postgres_conn_id='production_44')
        connection = src.get_conn()
        cursor = connection.cursor()
        cursor.execute("select yearcalendarweek from suropati.dimdate where date = '"+ todayte.strftime("%Y-%m-%d") +"'")
        dt = cursor.fetchone()
        yearweek = dt[0]
        return yearweek

    insert_tutela_grid_for_bad_coverage_new_method = PostgresOperator(
        task_id='insert_tutela_grid_for_bad_coverage_new_method',
        postgres_conn_id='235_dataset',
        sql='sql/covmo_bad_coverage/insert_tutela_grid_for_bad_coverage_new_method.sql',
        params={'yearweek':yearweekNow()}
    )

    update_kpi_tutela_covmo_bad_coverage_badtype_new_method = PostgresOperator(
        task_id='update_kpi_tutela_covmo_bad_coverage_badtype_new_method',
        postgres_conn_id='235_dataset',
        sql='sql/covmo_bad_coverage/update_kpi_tutela_covmo_bad_coverage_badtype_new_method.sql',
        params={'yearweek':yearweekNow()}
    )

    insert_tutela_grid_for_bad_coverage_new_method >> update_kpi_tutela_covmo_bad_coverage_badtype_new_method