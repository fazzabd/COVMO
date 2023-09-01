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

default_args = {
    'retries': 1,
    'retry_delay':timedelta(hours = 6)
}

with DAG(
    dag_id='covmo_weekly_all_process_transpose_clickhouse',
    schedule=None,
    start_date=datetime(2023, 2, 7),
    catchup=False,
    tags=['report','covmo','weekly'],
    max_active_runs=1
) as dag:
    
    todayte = date.today() - timedelta(days=7)
    src = PostgresHook(postgres_conn_id='production_44')
    connection = src.get_conn()
    cursor = connection.cursor()
    cursor.execute("select right(yearcalendarweek::varchar,2) from suropati.dimdate where date = '"+ todayte.strftime("%Y-%m-%d") +"'")
    dt = cursor.fetchone()
    week = dt[0]

    # week =52
    transpose_data = SSHOperator(
        task_id='transpose_data',
        ssh_conn_id='ssh_50',
        # ssh_conn_id='ssh_53',
        cmd_timeout=5000,
        command='bash /data01/covmo_automations/covmo_tile_parser_transpose_clickhouse_weekly.sh {{params.week}}',
        # command='bash /home/ndm/nsem/scripts/covmo_tile_parser_transpose_clickhouse_weekly.sh {{params.week}}',
        params={'week':week}
        )
    transpose_weeknonbusyhour = SSHOperator(
        task_id='transpose_weeknonbusyhour',
        ssh_conn_id='ssh_50',
        # ssh_conn_id='ssh_53',
        cmd_timeout=3600,
        command='bash /data01/covmo_automations/covmo_tile_parser_transpose_clickhouse_weekly_nonbusyhour.sh {{params.week}}',
        # command='bash /home/ndm/nsem/scripts/covmo_tile_parser_transpose_clickhouse_weekly_nonbusyhour.sh {{params.week}}',
        params={'week':week}
        )

    process_all_weekly_covmo = SSHOperator(
        task_id='process_all_weekly_covmo',
        ssh_conn_id='ssh_204',
        cmd_timeout=15000,
        command='sh /data/covmo_new/automation/main_airflow.sh {{params.week}}',
        params={'week':week}
        )

    transpose_data >> transpose_weeknonbusyhour >> process_all_weekly_covmo
