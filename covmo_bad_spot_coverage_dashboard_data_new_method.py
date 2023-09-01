## Author : Faiz Abdussalam
## Last Edit: 1 SEP 2023
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
from airflow.operators.bash import BashOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
import pytz

log = logging.getLogger(__name__)

def send_fail_notif(context):
    tele_kwargs = {}
    tele_hook = TelegramHook(telegram_conn_id='telegram')
    wib=pytz.timezone("Asia/Jakarta")
    datetime = context.get('task_instance').start_date.astimezone(wib)
    tele_kwargs['text'] = "\U0001f621 Task has failed. \nDag : {} \nTask : {} \nError :  {} \nStart time : {}".format(context.get('task_instance').dag_id,context.get('task_instance').task_id,str(context.get('exception')),datetime)
    tele_hook.send_message(tele_kwargs)

with DAG(
    dag_id='covmo_bad_spot_coverage_dashboard_data_new_method',
    schedule=None,
    start_date=datetime(2023, 2, 7),
    catchup=False,
    tags=['testing','poi','covmo'],
    max_active_runs=1,
    default_args={"on_failure_callback":send_fail_notif}
) as dag:
    todayte = date.today() - timedelta(days=7)
    src = PostgresHook(postgres_conn_id='144_nsem')
    connection = src.get_conn()
    cursor = connection.cursor()
    cursor.execute("select yearcalendarweek from reff.dimdate where date = '"+ todayte.strftime("%Y-%m-%d") +"'")
    dt = cursor.fetchone()
    yearweek = dt[0]
    # yearweek = 202301

    cleansing_data_dashboard_postgre_new_method = PostgresOperator(
        task_id='cleansing_data_dashboard_postgre_new_method',
        postgres_conn_id='235_dataset',
        sql='sql/covmo_bad_coverage/cleansing_data_dashboard_postgre_new_method.sql',
        params={'yearweek':yearweek}
    )


    # ingest_data_dashboard_to_54 = PostgresOperator(
    #     task_id='ingest_data_dashboard_to_54',
    #     postgres_conn_id='235_dataset',
    #     sql='sql/covmo_bad_coverage/ingest_data_dashboard_to_54.sql',
    #     params={'yearweek':yearweek}
    # )

    insert_cei_cell_weekly = PostgresOperator(task_id='insert_cei_cell_weekly',
        postgres_conn_id='235_dataset',
        sql='sql/insert_cei_cell_weekly.sql',
        params={'yearweek':yearweek}
    )

    # update_coverage_avg_rsrp_kecamatan_dashboard = PostgresOperator(
    #     task_id='update_coverage_avg_rsrp_kecamatan_dashboard',
    #     postgres_conn_id='235_dataset',
    #     sql='sql/covmo_bad_coverage/update_coverage_avg_rsrp_kecamatan_dashboard.sql',
    #     params={'yearweek':yearweek}
    # )


    insert_bad_coverage_overall_dashboard_new_method = PostgresOperator(
        task_id='insert_bad_coverage_overall_dashboard_new_method',
        postgres_conn_id='235_dataset',
        sql='sql/covmo_bad_coverage/insert_bad_coverage_overall_dashboard_new_method.sql',
        params={'yearweek':yearweek}
    )

    cleansing_data_dashboard_postgre_new_method >> insert_bad_coverage_overall_dashboard_new_method 
    cleansing_data_dashboard_postgre_new_method >> insert_cei_cell_weekly 

    
