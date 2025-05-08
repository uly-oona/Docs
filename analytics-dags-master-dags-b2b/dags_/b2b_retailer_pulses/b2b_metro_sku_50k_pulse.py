import pandas as pd
import numpy as np
import mysql.connector
from gspread_pandas import Spread
import psycopg2 as pg
from time import sleep
import sqlalchemy as sa
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

import smtplib, ssl
import os.path as op
import os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
import sys
import subprocess
from datetime import timedelta, date, datetime
import json
import logging

from libs.mailer import send_mail
import libs.nextcloud as NextCloud

ch_hook = ClickHouseHook(clickhouse_conn_id='clickhous_analytics')
mysql_hook = MySqlHook(mysql_conn_id='mysql_instamart')

from libs.dwh_ch import ch_pd_read_sql_query, DwhChCheckQuerySensor, ch_dataframe_to_clickhouse
from libs.airflow_connections import CH_ANALYTICS__MARKETING_B2B__CONN_ID

def from_next_cloud_to_dataframe(path, **kwargs):
    conn_obj = NextCloud.SpreadNextCloud() # creating connector object to NextCloud
    df = conn_obj.download_file_to_df(path, **kwargs) # path = absolute or relative path. Eg: '/home/user/file.csv' or 'file.csv'
    return df

def from_dataframe_to_next_cloud(df, path, **kwargs):
    conn_obj = NextCloud.SpreadNextCloud() # creating connector object to NextCloud
    conn_obj.upload_df_to_nextcloud(df=df, remote_path=path, **kwargs) # this uploads your df to nextcloud for the provided remote_path
    return True


def SQL_tables():

    SQL = mysql_hook.get_pandas_df( 
    """ 
  select distinct
    s.shipment_number shipment_number,
    s.completed_at completed_at,
    li.offer_id offer_id,
    li.price price,
    li.quantity quantity,
    li.price*li.quantity sku_total,
    CONVERT(sc.key_number,char) key_number,
    co.company_id company_id
    ,case when (payment_state in ('paid','overpaid') ) or (payment_method_id in (4,11,12,20)) or (payment_state = 'balance_due' and bd.id <> 0) 
            then 1 
            else 0 end as pay_flg
from 
    (   select distinct number shipment_number, li.shipment_id shipment_id, s.store_id store_id, 
    o.completed_at completed_at, pp.payment_method_id payment_method_id, o.payment_state  payment_state, s.order_id order_id
    
        from (
                select distinct 
                    id, 
                    order_id, 
                    number,
                    store_id
                from spree_shipments 
                where item_total >= 50000 
                    and retailer_id = 1
                    and id > 300000000
                    and deleted_at is null 
                ) s 
            join spree_line_items li on s.id = li.shipment_id
            join (
                select id, completed_at,payment_state
                from spree_orders
                where completed_at > now() - INTERVAL 5 DAY
                    and state in ('complete', 'resumed')
                ) o on o.id = s.order_id
            join spree_payments pp on pp.order_id = o.id
        where li.retailer_id = 1 
            and li.deleted_at is null
            and price*quantity >= 50000
            and o.id not in (select distinct order_id from b2b_unpaid_orders where export_lock_state = 'locked')
    ) s 
    join spree_line_items li on s.shipment_id = li.shipment_id
    join (
        select 
            sc.store_id store_id,
            sc.key_number key_number
        from stores s
            join (
                select 
                    substr(import_key, locate('-', import_key)+1,15) key_number,
                    import_key,
                    store_id 
                from store_configs
                ) sc on s.id = sc.store_id
        where s.retailer_id = 1 
        ) sc on sc.store_id = s.store_id
    join companies_orders co on co.order_id = s.order_id /*тут доработка понадобится, кроме заказов на компаний еще сделать заказы на галочек/без менеджеров*/
    -- left join (select distinct order_id from companies_orders co join companies c on c.id = co.company_id and (c.postpay or c.deposit) and not c.contract_suspended ) cco on cco.order_id = s.order_id
    left join (
        select 
            o.id id,
            sum(s.payment_total) summ_paid,
            count(distinct s.payment_total) cnt_pay,
            count(s.payment_total) cnt_ords,
            sum(s.total) summ_total
            ,if(count(distinct s.payment_total) = 1, sum(s.payment_total)/count(s.payment_total),sum(s.payment_total))summ_paid_t
        from (select * from spree_shipments where retailer_id = 1 and id > 300000000) s 
        join (select * from spree_orders where state = 'complete' and payment_state = 'balance_due' and completed_at > now() - INTERVAL 5 DAY)  o  on s.order_id = o.id
        where 1 
        group by 1
        having summ_total - summ_paid_t <= 1000
    ) bd on bd.id = s.order_id
                        
where li.retailer_id = 1 
    and li.deleted_at is null
    -- and (s.payment_state in ('paid','overpaid') or cco.order_id <> 0 or s.payment_method_id in (4,11,12,20) or (s.payment_state = 'balance_due' and bd.id <> 0))
order by s.shipment_number
    """
    )
    return SQL


def SQL_managers():
        SQL = """
        with cm_users as (
        select distinct
            cm.company_id company_id, 
            cm.user_id user_id,
            u.firstname||' '||u.lastname user_fio,
            u.email user_email
        from int_spree_users u 
        join (select * from gp_rep.rep__company_managers where deleted_at is null ) cm on cm.user_id = u.id 
        ),

        phones as (
        select user_id, p.value phone
        from int_spree_phones p 
        where user_id in (select distinct user_id from cm_users)
        )

        select 
        c.company_id company_id
        , c.inn "ИНН"
        , c.name "Название компании"
        , ifNull(u.user_email,'zabotab2b@sbermarket.ru') as "Почта ответственного менеджера"
        ,REPLACE(REPLACE(REPLACE(toString(groupUniqArray(if(p.phone = '','78005509411',p.phone))), '[', ''), ']', ''),'''',' ') as "Телефон ответственного менеджера"
        , ifNull(u.user_fio,'Группа поддержки b2b') as "ФИО ответственного менеджера"
        , con_flag
        from (select id company_id, inn, name, if(postpay_flg or deposit_flg, 'postpay', 'another') con_flag from gp_rep.rep__companies where mt_src_cd = 'B2B_COMPANY') c 
        left join cm_users u on u.company_id = c.company_id
        left join phones p on p.user_id = u.user_id
        where 1 
        group by 1,2,3,4,6,7
            """
        return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)


def SQL_producthub():

        SQL = """
    with    x_prices as (
            select
                offer_id,
                argMax(price,dt) price,
                argMax(discount,dt) discount,
                argMax(discount_starts_at,dt) discount_starts_at,
                argMax(discount_ends_at,dt) discount_ends_at
            from ods.product_hub__product_price
            where 1 
                and published = 1 
                and retailer_id = 1
            group by offer_id
                ), 

            x_offers as (
                select
                    distinct 
                    id,
                    name as "Наименование Товара",
                    sku,
                    retailer_sku as "Артикул Ритейлера",
                    store_id,
                    retailer_id,
                    stock,
                    max_stock,
                    updated_at_offer,
                    updated_at_stock,
                    vat_rate
                from ods.product_hub__offer_x_stock o
                where 1
                    and deleted_at is null 
                    and published = 1
                    and status = 1
                    and retailer_id = 1
                    )
            select 
                x_offers.id offer_id,
                x_offers."Наименование Товара" "Наименование Товара",
                x_offers."Артикул Ритейлера" "Артикул Ритейлера"
            from x_offers 
                left join x_prices on x_prices.offer_id = x_offers.id
            where 1 

            """
        return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def pushed_df():

    SQL =   """
            select distinct shipment_number from cdm.b2b__pulses_logs where process_name = 'metro_sku_50k_pulse'
            """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def get_directors():
    pre_df = from_next_cloud_to_dataframe('b2b_analytics/Почты магазинов Метро.xlsx')
    pre_df = pre_df.drop_duplicates()    
    return pre_df

def send_mail_dag():

    df = SQL_tables()
    pushed = pushed_df()
    df = df[~df.shipment_number.isin(pushed.shipment_number)]
  
    if len(df) >0:
        
        directors = get_directors()
        directors.director_email = directors.director_email.apply(lambda x: x.lower())
        directors.zamdirector_email = directors.zamdirector_email.apply(lambda x: x.lower())
        directors.key_number = directors.key_number.astype('str')

        managers_df = SQL_managers()
        producthub_df = SQL_producthub()

        df = df.merge(managers_df,how = 'left',on = 'company_id')

        df = df[(df.pay_flg==1) | (df.con_flag=='postpay')]

        df["Почта ответственного менеджера"] = df["Почта ответственного менеджера"].fillna('zabotab2b@sbermarket.ru')

        df = df.merge(producthub_df,on = 'offer_id')
        df = df.drop(columns = ['offer_id','company_id','pay_flg','con_flag'])
        df = df.drop_duplicates()

        for i in np.unique(df['shipment_number'].values):
            pmdf = df[df['shipment_number'] == i]
            PATH = f'Sbermarket_orders_{i}.xlsx'
            email = list(pmdf['Почта ответственного менеджера'].drop_duplicates().values)
            email = email + list(pmdf.merge(directors, on = 'key_number')['director_email'].drop_duplicates().values) + list(pmdf.merge(directors, on = 'key_number')['zamdirector_email'].drop_duplicates().values)
            email = email + ['dmitry.smertin@metro-cc.ru','olga.a.orlova@sbermarket.ru','aleksandra.pekhoto@sbermarket.ru']
            email_to_text = str.replace(str.replace(str.replace(str.replace(str(email),'[',''),']',''),"'",""),',',';')
            pmdf = pmdf.drop(columns = 'key_number')
            pmdf.to_excel(PATH, index=False)
            send_mail(to = email ,
                      subject = f"Заказ {i} Сбермаркет METRO",
                      text = f"""Заказ {i} Сбермаркет METRO
                      При ответе на это письмо, добавляйте в копию {email_to_text}
                      """,
                      file_paths = [PATH]
                      )

        
        log_df = df[['shipment_number']].drop_duplicates()
        log_df['process_name'] = 'metro_sku_50k_pulse'
        log_df['shipment_logged_at'] = pd.datetime.now() + timedelta(hours = 3)

        ch_dataframe_to_clickhouse(log_df.drop_duplicates(), "cdm.b2b__pulses_logs", ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)
    else:
        logging.info('No Data Received')
        
    

with DAG(

        dag_id='b2b_metro_sku_50k_pulse',
        description='b2b_metro_sku_50k_pulse',
        schedule_interval='0/7 * * * *',
        start_date=datetime(2021, 7, 18, 10, 0, 0, 0),
        max_active_runs=1,
        tags=['b2b'],
        catchup=False

) as dag:

    send = PythonOperator(
        task_id='to_send',
        python_callable=send_mail_dag
    )

    send
