## Author : Faiz Abdussalam
## Last Edit: 1 September 2023
## Note : UPDATE COVMO NEW CALCULATION METHOD

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
    dag_id='covmo_bad_coverage_daily_agg_to_week_v2',
    schedule=None,
    start_date=datetime(2023, 2, 7),
    catchup=False,
    tags=['testing','poi','covmo'],
    #max_active_runs=1,
    default_args={"on_failure_callback":send_fail_notif}
    
) as dag:
    # yearweek = 202301
    todayte = date.today() - timedelta(days=7)
    src = PostgresHook(postgres_conn_id='production_44')
    connection = src.get_conn()
    cursor = connection.cursor()
    cursor.execute("select yearcalendarweek from suropati.dimdate where date = '"+ todayte.strftime("%Y-%m-%d") +"'")
    dt = cursor.fetchone()
    yearweek = dt[0]
    # yearweek = 202301
    cursor.execute("select to_char(to_date(date,'YYYY-MM-DD'),'YYYYMMDD') from suropati.dimdate where yearcalendarweek = "+ str(yearweek))
    dt = cursor.fetchall()
    date_list = [row[0] for row in dt]

    cleansing_sqream_preparation = SSHOperator(
        task_id='cleansing_sqream_preparation',
        ssh_conn_id='ssh_204',
        cmd_timeout=2000,
        command='sh /data/covmo_new/covmo_bad_coverage/query/cleansing_sqream_preparation.sh {{params.yearweek}}',
        params={'yearweek':yearweek}
        )

    cleansing_postgre_144 = PostgresOperator(
        task_id='cleansing_postgre_144',
        postgres_conn_id='144_nsem',
        sql='sql/covmo_bad_coverage/cleansing_postgre_144.sql',
        params={'yearweek':yearweek}
    )

    sqream_query_covmo_bad_tile_new_method = SSHOperator(
        task_id='sqream_query_covmo_bad_tile_new_method',
        ssh_conn_id='ssh_204',
        cmd_timeout=2000,
        command='sh /data/covmo_new/covmo_bad_coverage/query/sqream_query_covmo_bad_tile_new_method.sh {{params.yearweek}}',
        params={'yearweek':yearweek}
        )

    ingest_covmo_bad_tile_to_postgres_new_method = SSHOperator(
        task_id='ingest_covmo_bad_tile_to_postgres_new_method',
        ssh_conn_id='ssh_204',
        cmd_timeout=2000,
        command='sh /data/covmo_new/covmo_bad_coverage/query/ingest_covmo_bad_tile_to_postgres_new_method.sh {{params.yearweek}}',
        params={'yearweek':yearweek}
        )

    query_covmo_bad_coverage_nearest_site_postgre_144_new_method = PostgresOperator(
        task_id='query_covmo_bad_coverage_nearest_site_postgre_144_new_method',
        postgres_conn_id='144_nsem',
        sql='sql/covmo_bad_coverage/query_covmo_bad_coverage_nearest_site_postgre_144_new_method.sql',
        params={'yearweek':yearweek}
    )

    ingest_final_to_dest_new_method = PostgresOperator(
        task_id='ingest_final_to_dest_new_method',
        postgres_conn_id='235_dataset',
        sql='sql/covmo_bad_coverage/ingest_final_to_dest_new_method.sql',
        params={'yearweek':yearweek}
    )

    cleansing_sqream = SSHOperator(
        task_id='cleansing_sqream',
        ssh_conn_id='ssh_204',
        cmd_timeout=2000,
        command='sh /data/covmo_new/covmo_bad_coverage/query/cleansing_sqream.sh {{params.yearweek}}',
        params={'yearweek':yearweek}
        )

    cleansing_postgre_144_after = PostgresOperator(
        task_id='cleansing_postgre_144_after',
        postgres_conn_id='144_nsem',
        sql='sql/covmo_bad_coverage/cleansing_postgre_144_after.sql',
        params={'yearweek':yearweek}
    )

    get_raw_data1 = SSHOperator(
        task_id="get_raw_data_1",
        ssh_conn_id='ssh_204',
        cmd_timeout=2000,
        command='sh /data/covmo_new/covmo_bad_coverage/query/get_unzip_and_ingest_sqream_daily_covmo.sh {{params.date1}}',
        params={'date1':date_list[0]}
    )
    get_raw_data2 = SSHOperator(
        task_id="get_raw_data_2",
        ssh_conn_id='ssh_204',
        cmd_timeout=2000,
        command='sh /data/covmo_new/covmo_bad_coverage/query/get_unzip_and_ingest_sqream_daily_covmo.sh {{params.date2}}',
        params={'date2':date_list[1]}
    )
    get_raw_data3 = SSHOperator(
        task_id="get_raw_data_3",
        ssh_conn_id='ssh_204',
        cmd_timeout=2000,
        command='sh /data/covmo_new/covmo_bad_coverage/query/get_unzip_and_ingest_sqream_daily_covmo.sh {{params.date3}}',
        params={'date3':date_list[2]}
    )
    get_raw_data4 = SSHOperator(
        task_id="get_raw_data_4",
        ssh_conn_id='ssh_204',
        cmd_timeout=2000,
        command='sh /data/covmo_new/covmo_bad_coverage/query/get_unzip_and_ingest_sqream_daily_covmo.sh {{params.date4}}',
        params={'date4':date_list[3]}
    )
    get_raw_data5 = SSHOperator(
        task_id="get_raw_data_5",
        ssh_conn_id='ssh_204',
        cmd_timeout=2000,
        command='sh /data/covmo_new/covmo_bad_coverage/query/get_unzip_and_ingest_sqream_daily_covmo.sh {{params.date5}}',
        params={'date5':date_list[4]}
    )
    get_raw_data6 = SSHOperator(
        task_id="get_raw_data_6",
        ssh_conn_id='ssh_204',
        cmd_timeout=2000,
        command='sh /data/covmo_new/covmo_bad_coverage/query/get_unzip_and_ingest_sqream_daily_covmo.sh {{params.date6}}',
        params={'date6':date_list[5]}
    )
    get_raw_data7 = SSHOperator(
        task_id="get_raw_data_7",
        ssh_conn_id='ssh_204',
        cmd_timeout=2000,
        command='sh /data/covmo_new/covmo_bad_coverage/query/get_unzip_and_ingest_sqream_daily_covmo.sh {{params.date7}}',
        params={'date7':date_list[6]}
    )

    cleansing_sqream_preparation >> cleansing_postgre_144
    cleansing_postgre_144 >> get_raw_data1
    cleansing_postgre_144 >> get_raw_data2
    cleansing_postgre_144 >> get_raw_data3
    cleansing_postgre_144 >> get_raw_data4
    cleansing_postgre_144 >> get_raw_data5
    cleansing_postgre_144 >> get_raw_data6
    cleansing_postgre_144 >> get_raw_data7

    get_raw_data1 >> sqream_query_covmo_bad_tile_new_method
    get_raw_data2 >> sqream_query_covmo_bad_tile_new_method
    get_raw_data3 >> sqream_query_covmo_bad_tile_new_method
    get_raw_data4 >> sqream_query_covmo_bad_tile_new_method
    get_raw_data5 >> sqream_query_covmo_bad_tile_new_method
    get_raw_data6 >> sqream_query_covmo_bad_tile_new_method
    get_raw_data7 >> sqream_query_covmo_bad_tile_new_method

    sqream_query_covmo_bad_tile_new_method >> ingest_covmo_bad_tile_to_postgres_new_method >> query_covmo_bad_coverage_nearest_site_postgre_144_new_method 
    query_covmo_bad_coverage_nearest_site_postgre_144_new_method >> ingest_final_to_dest_new_method
    ingest_final_to_dest_new_method >> cleansing_sqream >> cleansing_postgre_144_after
    # cleansing_sqream_preparation >> cleansing_postgre_144 >> get_raw_data

