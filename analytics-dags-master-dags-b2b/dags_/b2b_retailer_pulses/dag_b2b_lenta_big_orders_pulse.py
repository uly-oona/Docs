import pandas as pd
import numpy as np
import mysql.connector
from gspread_pandas import Spread
import psycopg2 as pg
from time import sleep
import sqlalchemy as sa

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

mysql_hook = MySqlHook(mysql_conn_id='mysql_instamart')

def from_next_cloud_to_dataframe(path, **kwargs):
    conn_obj = NextCloud.SpreadNextCloud() # creating connector object to NextCloud
    df = conn_obj.download_file_to_df(path, **kwargs) # path = absolute or relative path. Eg: '/home/user/file.csv' or 'file.csv'
    return df

from libs.dwh_ch import ch_pd_read_sql_query, DwhChCheckQuerySensor, ch_dataframe_to_clickhouse
from libs.airflow_connections import CH_ANALYTICS__MARKETING_B2B__CONN_ID

def scan_big_orders():

    SQL = mysql_hook.get_pandas_df( """
    with orders_info as (
    select distinct
        s.number as shipment_number
        , co.company_id company_id
        , substr(sc.import_key, locate('-', sc.import_key)+1,15) import_key
        , oz.name city_name 
        , o.payment_state payment_state
        , s.total total
        , o.id order_id
        , pp.payment_method_id payment_method_id
    from 
        (
        select id as shipment_id, order_id,number,delivery_window_id,store_id,total
        from spree_shipments 
        where retailer_id in (8,23939) and delivery_window_id is not null
        and deleted_at is null
        and item_total >= 50000
        ) s
    join spree_orders o on o.id = s.order_id and date(convert_tz(o.completed_at, 'UTC', 'Europe/Moscow'))  = date(now())
    join companies_orders co on co.order_id = o.id 
    join stores st on st.id = s.store_id 
    join operational_zones oz on oz.id = st.operational_zone_id
    join store_configs sc on sc.store_id = st.id 
    join spree_line_items li on li.shipment_id = s.shipment_id and li.deleted_at is null
    join spree_payments pp on pp.order_id = o.id 
    where 1=1
        and o.state in ('complete','resumed')
        and o.id not in (select distinct order_id from b2b_unpaid_orders where export_lock_state = 'locked')
        and (li.quantity* li.price) >= 50000
    ),

    balance_due_ords as (
    select distinct order_id
    from orders_info
    where payment_state = 'balance_due'
    ),

    balance_due_suitable_orders as (
    select distinct order_id
    from (
        select 
            s.order_id order_id, 
            number,
            case when 
                    (dense_rank() over (partition by order_id order by payment_total) + dense_rank() over (partition by order_id order by payment_total desc) - 1) >1  
                then  
                    sum(payment_total) over (partition by order_id)
                else 
                    payment_total
                end as payment_total_corrected,
            sum(total) over (partition by order_id)  sum_total
        from balance_due_ords bd 
        join spree_shipments s on s.order_id = bd.order_id and s.deleted_at is null 
        ) a 
    where sum_total - payment_total_corrected <= 1000
    )

    select 
        shipment_number,
        company_id,
        total,
        CONVERT(import_key,char) import_key,
        city_name,
        li.offer_id offer_id,
        li.price price,
        li.quantity quantity
        ,case when (payment_state in ('paid','overpaid') ) or (payment_method_id in (4,11,12,20)) or (payment_state = 'balance_due' and bd.order_id <> 0) 
                    then 1 
                    else 0 
        end as pay_flg
    from orders_info o 
    join spree_line_items li on li.order_id = o.order_id and li.deleted_at is null
    left join balance_due_suitable_orders bd on o.order_id = bd.order_id
    """)

    return SQL


def scan_limited_orders():

    SQL = mysql_hook.get_pandas_df( """
        select distinct
            s.number as shipment_number
            , co.company_id company_id
            , li.offer_id offer_id
            , substr(sc.import_key, locate('-', sc.import_key)+1,15) import_key
            , oz.name city_name 
            , li.quantity as quantity
            , li.price as price
            , case when (o.payment_state in ('paid','overpaid') )
                    then 1 
                    else 0 end as pay_flg
        from
            (
            select id as shipment_id, order_id,number,delivery_window_id,store_id
            from spree_shipments 
            where retailer_id in (8,23939) and delivery_window_id is not null
            and deleted_at is null
            ) s
        join spree_orders o on o.id = s.order_id
        join companies_orders co on co.order_id = o.id
        join stores st on st.id = s.store_id 
        join operational_zones oz on oz.id = st.operational_zone_id
        join store_configs sc on sc.store_id = st.id 
        join spree_line_items li on li.shipment_id = s.shipment_id and li.deleted_at is null
        -- left join (select distinct co.order_id order_id from companies_orders co join companies c on c.id = co.company_id and (c.postpay or c.deposit) and not c.contract_suspended) coo on coo.order_id = o.id
        where 1=1
        and o.state = 'complete'
        -- and ((o.payment_state in ('paid','overpaid') or coo.order_id <> 0) or (o.payment_state = 'balance_due'))
        and o.id not in (select distinct order_id from b2b_unpaid_orders where export_lock_state = 'locked')
        and date(convert_tz(o.completed_at, 'UTC', 'Europe/Moscow')) = date(now())

    """)
    
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
            , REPLACE(REPLACE(REPLACE(toString(groupUniqArray(if(p.phone = '','78005509411',p.phone))), '[', ''), ']', ''),'''',' ') as "Телефон ответственного менеджера"
            , ifNull(u.user_fio,'Группа поддержки b2b') as "ФИО ответственного менеджера"
            , con_flag
        from (select id company_id, inn, name, if(postpay_flg or deposit_flg, 'postpay', 'another') con_flag from gp_rep.rep__companies where mt_src_cd = 'B2B_COMPANY') c 
        left join cm_users u on u.company_id = c.company_id
        left join phones p on p.user_id = u.user_id
        where 1 
        group by 1,2,3,4,6,7
        """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def SQL_producthub(column = None):

    if column is None:
        SQL = f"""
                select distinct id offer_id, name product_name, retailer_sku
                from ods.product_hub__offer_x_stock
                where 1
                    and deleted_at is null 
                    and published = 1
                    and status = 1
                    and retailer_id in (8,23939)
            """
    else:
        SQL = f"""
                select distinct id offer_id, name product_name, retailer_sku
                from ods.product_hub__offer_x_stock
                where 1
                    and deleted_at is null 
                    and published = 1
                    and status = 1
                    and retailer_id in (8,23939)
                    and retailer_sku in {tuple(column.values)}
            """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)


def pushed_df():

    SQL = """
            select distinct shipment_number from cdm.b2b__pulses_logs where process_name = 'lenta_pulse'
            """

    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def scaned_df():

    SQL = """
            select distinct shipment_number from cdm.b2b__pulses_logs where process_name = 'lenta_pulse_scanned'
            """
    
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)



def send_more50k_orders():

    df = scan_big_orders()
    pushed = pushed_df()
    df = df[~df.shipment_number.isin(pushed.shipment_number)]
    
    if len(df) >0:

        managers_df = SQL_managers()
        producthub_df = SQL_producthub(column = None)

        contacts = from_next_cloud_to_dataframe('b2b_analytics/b2b_lenta_offers_limits.xlsx',sheet_name = 'contacts' , dtype={'import_key': str})
        contacts = contacts.drop_duplicates()
        contacts.import_key = contacts.import_key.astype('str')

        df = df.merge(managers_df,on = 'company_id')
        df = df.merge(producthub_df,on = 'offer_id')
        df = df.merge(contacts, how ='left', on = 'import_key')

        df = df[(df.pay_flg==1) | (df.con_flag=='postpay')]

        log_df = df[['shipment_number']].drop_duplicates()
        log_df['process_name'] = 'lenta_pulse'
        log_df['shipment_logged_at'] = pd.datetime.now() + timedelta(hours = 3)

        ch_dataframe_to_clickhouse(log_df.drop_duplicates(), "cdm.b2b__pulses_logs", ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

        df = df.drop(columns = ['company_id','offer_id','con_flag','pay_flg'])
        df = df.drop_duplicates()

        for i in np.unique(df['shipment_number'].values):
            pmdf = df[df['shipment_number'] == i]
            PATH = f'Sbermarket_lenta_order_{i}.xlsx'
            email = list(pmdf['Почта ответственного менеджера'].drop_duplicates().values)
            email = email + ['marat.shaimardanov@lenta.com','andrey.vostrikov@lenta.com',
                            'ekaterina.marten@lenta.com','tatiana.pushkina@lenta.com',
                            'natalya.strigacheva@lenta.com','maria.khlebnikova@lenta.com',
                            'AllLentaProDM@lenta.com', 'AllRecipientsLentaPRO@lenta.com',
                            'olga.a.orlova@sbermarket.ru','aleksandra.pekhoto@sbermarket.ru']
            email_to_text = str.replace(str.replace(str.replace(str.replace(str(email),'[',''),']',''),"'",""),',',';')
            import_key = pmdf['import_key'].drop_duplicates().values[0]
            city_name = pmdf['city_name'].drop_duplicates().values[0]
            pmdf.to_excel(PATH, index=False)
            send_mail(to = email ,
                      subject = f"Заказ {i} свыше 50т.р. город {city_name} ключ магазина {import_key} ",
                      text = f"""Заказ {i} свыше 50т.р. город {city_name} ключ магазина {import_key}
                      При ответе на это письмо, добавляйте в копию {email_to_text}
                      """,
                      file_paths = [PATH]
                      )
    else:
        logging.info('No Data Received')


def send_limit_orders():

    df = scan_limited_orders()
    scaned = scaned_df()
    df = df[~df.shipment_number.isin(scaned.shipment_number)]

    if len(df) >0:

        log_df = df[['shipment_number']].drop_duplicates()
        log_df['process_name'] = 'lenta_pulse_scanned'
        log_df['shipment_logged_at'] = pd.datetime.now() + timedelta(hours = 3)

        ch_dataframe_to_clickhouse(log_df.drop_duplicates(), "cdm.b2b__pulses_logs", ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)
        
        limits = from_next_cloud_to_dataframe('b2b_analytics/b2b_lenta_offers_limits.xlsx',sheet_name = 'limits')
        limits = limits.drop_duplicates()
        limits = limits[~limits.limit.isna()]
        limits = limits[~limits.retailer_sku.isna()]
        limits.retailer_sku = limits.retailer_sku.astype('str')
        limits.limit = limits.limit.astype('int')

        contacts = from_next_cloud_to_dataframe('b2b_analytics/b2b_lenta_offers_limits.xlsx',sheet_name = 'contacts' , dtype={'import_key': str})
        contacts = contacts.drop_duplicates()
        contacts.import_key = contacts.import_key.astype('str')

        producthub_df = SQL_producthub(limits.retailer_sku)
        producthub_df.retailer_sku = producthub_df.retailer_sku.astype('str')

        df = df.merge(producthub_df, on = 'offer_id')
        df = df.merge(limits, on = 'retailer_sku')
        df = df[df.quantity > df.limit]
        df = df.merge(contacts, how ='left', on = 'import_key')
       
        df = df.drop(columns = ['offer_id'])
        df = df.drop_duplicates()

        if len(df) > 0:
            managers_df = SQL_managers()
            df = df.merge(managers_df,on = 'company_id')

            df = df[(df.pay_flg==1) | (df.con_flag=='postpay')]

            df = df.drop(columns = ['company_id','con_flag','pay_flg'])

            log_df = df[['shipment_number']].drop_duplicates()
            log_df['process_name'] = 'lenta_overlimit_pulse'
            log_df['shipment_logged_at'] = pd.datetime.now() + timedelta(hours = 3)

            ch_dataframe_to_clickhouse(log_df.drop_duplicates(), "cdm.b2b__pulses_logs", ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

            for i in np.unique(df['shipment_number'].values):
                pmdf = df[df['shipment_number'] == i]
                PATH = f'Over_Limit_Sbermarket_Lenta_Order_{i}.xlsx'
                email = list(pmdf['Почта ответственного менеджера'].drop_duplicates().values) + list(pmdf['lenta_manager_email'].drop_duplicates().values) + ['olga.a.orlova@sbermarket.ru','daniil.mazakin@sbermarket.ru']
                email_to_text = str.replace(str.replace(str.replace(str.replace(str(email),'[',''),']',''),"'",""),',',';')
                import_key = pmdf['import_key'].drop_duplicates().values[0]
                city_name = pmdf['city_name'].drop_duplicates().values[0]
                pmdf.to_excel(PATH, index=False)
                send_mail(to = email ,
                        subject = f"Заказ {i} превышено ограничение город {city_name} ключ магазина {import_key} ",
                        text = f"""Заказ {i} превышено ограничение город {city_name} ключ магазина {import_key}
                        При ответе на это письмо, добавляйте в копию {email_to_text}
                        """,
                        file_paths = [PATH]
                        )
        else:
            logging.info('No Data Recieved')
    else:
        logging.info('No New Data Scanned')


with DAG(

        dag_id='dag_b2b_lenta_big_orders_pulse',
        description='lenta_pulse_big_orders',
        schedule_interval='0/10 * * * *',
        start_date=datetime(2022, 12, 21, 5, 0, 0, 0),
        max_active_runs=1,
        tags=['b2b'],
        catchup=False

) as dag:

    send_50k = PythonOperator(
        task_id='to_send',
        python_callable=send_more50k_orders
    )

    send_lim = PythonOperator(
        task_id='to_send_limit',
        python_callable=send_limit_orders
    )

    send_50k >> send_lim
