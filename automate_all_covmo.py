## Author : Faiz Abdussalam
## Last Edit: 30 March 2023
## Note : This DAG use to job All Covmo automation

import logging
import shutil
import time
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.macros import ds_format, ds_add
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
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

args={
    'retries':4,
    'retry_delay':timedelta(hours=2)
    ,'on_failure_callback':send_fail_notif
    }

with DAG(
    dag_id='automate_all_covmo_weekly',
    schedule='15 22 * * 0',
    start_date=datetime(2023, 2, 7),
    default_args=args,
    catchup=False,
    tags=['covmo','automation','cronjob'],
) as dag:

    def yearNow():
        temp_yw = yearweekNow()
        year = int(str(temp_yw)[:4])
        return year

    def weekNow():
        temp_yw = yearweekNow()
        week = int(str(temp_yw)[-2:])
        return week
        # todayte = date.today() - timedelta(days=7)
        # src = PostgresHook(postgres_conn_id="production_44")
        # connection = src.get_conn()
        # cursor = connection.cursor()
        # cursor.execute("select right(yearcalendarweek::varchar,2) from suropati.dimdate where date = '"+ todayte.strftime("%Y-%m-%d") +"'")
        # dt = cursor.fetchone()
        # week = dt[0]
        # return week

    def yearweekNow():
        todayte = date.today() - timedelta(days=7)
        src = PostgresHook(postgres_conn_id='production_44')
        connection = src.get_conn()
        cursor = connection.cursor()
        cursor.execute("select yearcalendarweek from suropati.dimdate where date = '"+ todayte.strftime("%Y-%m-%d") +"'")
        dt = cursor.fetchone()
        yearweek = dt[0]
        return yearweek

    def datelistWeekNow():
        todayte = date.today() - timedelta(days=7)
        src = PostgresHook(postgres_conn_id='production_44')
        connection = src.get_conn()
        cursor = connection.cursor()
        cursor.execute("select to_char(to_date(date,'YYYY-MM-DD'),'YYYYMMDD') from suropati.dimdate where yearcalendarweek = "+ str(yearweekNow()))
        dt = cursor.fetchall()
        date_list = [row[0] for row in dt]
        return date_list

    covmo_week =TriggerDagRunOperator(task_id='load_covmo_weekly_transpose_ch',
        trigger_dag_id='covmo_weekly_all_process_transpose_clickhouse',
        conf={"week":weekNow()},wait_for_completion=True)

    tutela_top_cell = TriggerDagRunOperator(task_id='load_tutela_month_topcell_join_covmo',
        trigger_dag_id='tutela_month_top_cell_join_covmo',
        conf={"yearweek":yearweekNow()},wait_for_completion=True)

    date_list = datelistWeekNow()
    
    covmo_bad_coverage = TriggerDagRunOperator(task_id='load_covmo_bad_coverage_daily_agg_to_week_v2',
            trigger_dag_id='covmo_bad_coverage_daily_agg_to_week_v2',
            conf={"yearweek":yearweekNow()
            ,"date1":date_list[0]
            ,"date2":date_list[1]
            ,"date3":date_list[2]
            ,"date4":date_list[3]
            ,"date5":date_list[4]
            ,"date6":date_list[5]
            ,"date7":date_list[6]}
            ,wait_for_completion=True)

    tutela_top_cell_3x3 = TriggerDagRunOperator(task_id='load_tutela_3x3_celltop_join_covmo',
        trigger_dag_id='tutela_3x3_celltop_join_covmo',
        conf={"yearweek":yearweekNow()},wait_for_completion=True)
        

    ingest_raw_transpose_covmo = SSHOperator(
        task_id='ingest_raw_transpose_covmo',
        ssh_conn_id='ssh_204',
        cmd_timeout=30000,
        command='sh /data/covmo_new/automation/query/ingest_raw_transpose_to_pg.sh {{params.week}}',
        params={'week':weekNow()}
        )

    covmo_bad_spot_coverage_dashboard_data  = TriggerDagRunOperator(task_id='load_covmo_bad_spot_coverage_dashboard_data',
        trigger_dag_id='covmo_bad_spot_coverage_dashboard_data',
        conf={"yearweek":yearweekNow()},wait_for_completion=True)
    
    cleantopcell = PostgresOperator(
        task_id='cleantopcell',
        postgres_conn_id='144_nsem',
        sql='delete FROM tutela_prod.top_cell_by_geohash_covmo where week = {{params.week}}; delete FROM tutela_prod.top_cell_by_grid9 where weeknum = {{params.week}};',
        params={'week':weekNow()}
    )

    #tutela_join_gq_gp_vn = TriggerDagRunOperator(task_id='load_tutela_join_gq_gp_vn',
    #    trigger_dag_id='tutela_join_gq_gp_vn',
    #    conf={"yearweek":yearweekNow()},wait_for_completion=True)
    
    tutela_demarcation_bayu_v2 = TriggerDagRunOperator(task_id='load_tutela_demarcation_bayu_v2',
        trigger_dag_id='tutela_demarcation_bayu_v2',
        conf={"yearweek":yearweekNow()},wait_for_completion=True)

    xmap_coverage_p50_weekly_joinml = TriggerDagRunOperator(task_id='load_xmap_coverage_p50_weekly_joinml',
        trigger_dag_id='xmap_coverage_p50_weekly_joinml',
        conf={"year":yearNow(),"week":weekNow()},wait_for_completion=True)

    update_covmo_bad_coverage_sector = TriggerDagRunOperator(task_id='load_update_covmo_bad_coverage_sector',
        trigger_dag_id='update_covmo_bad_coverage_sector',
        conf={"year":yearNow(),"week":weekNow(),"yearweek":yearweekNow()},wait_for_completion=True)


    covmo_week >> tutela_top_cell
    covmo_week >> covmo_bad_coverage
    covmo_week >> ingest_raw_transpose_covmo
    
    tutela_top_cell >> tutela_top_cell_3x3

    covmo_bad_coverage >> update_covmo_bad_coverage_sector >> covmo_bad_spot_coverage_dashboard_data
    
    tutela_top_cell_3x3 >> tutela_demarcation_bayu_v2 >>cleantopcell
    
    covmo_bad_spot_coverage_dashboard_data >> cleantopcell

    cleantopcell >> xmap_coverage_p50_weekly_joinml


