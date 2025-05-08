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
from libs.helpers import get_clickhouse_connection
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import sys
import subprocess
from datetime import timedelta, date, datetime
import json
import logging

from libs.mailer import send_mail

mysql_hook = MySqlHook(mysql_conn_id='mysql_instamart')

from libs.dwh_ch import ch_pd_read_sql_query, DwhChCheckQuerySensor, ch_dataframe_to_clickhouse
from libs.airflow_connections import CH_ANALYTICS__MARKETING_B2B__CONN_ID

def SQL_tables():

    SQL = mysql_hook.get_pandas_df( """ 

    select distinct
        convert_tz(o.completed_at, 'UTC', 'Europe/Moscow') as 'Дата и время заказа'
        , convert_tz(dw.ends_at, 'UTC', 'Europe/Moscow') as 'Дата и время доставки'
        , o.state as 'Статус заказа'
        , a.full_address as 'Адрес доставки'
        , s.number as 'Номер заказа Сбермаркет'
        , li.quantity as 'Количество товара'
        , li.price as 'Цена товара'
        , coalesce(u.pending_email, u.email) as 'Почта клиента'
        , concat('+',a.phone) as 'Телефон клиента'
        , li.offer_id offer_id
        , co.company_id company_id
        , s.number as shipment_number
    from
        (
        select order_id,delivery_window_id, number
        from spree_shipments 
        where 1=1 
        and retailer_id = 25354
        and delivery_window_id is not null
        and deleted_at is null 
        ) s
    join delivery_windows dw on dw.id = s.delivery_window_id
    join spree_orders o on o.id = s.order_id
    join companies_orders co on co.order_id = o.id
    join spree_line_items li on li.order_id = s.order_id
    join spree_users u on u.id = o.user_id 
    left join spree_addresses a on a.id = o.ship_address_id
    where 1=1
        and o.state in ('complete')
        and li.deleted_at is null
        and li.retailer_id = 25354
        and convert_tz(o.completed_at, 'UTC', 'Europe/Moscow') > convert_tz(now() , 'UTC', 'Europe/Moscow') - INTERVAL 4 DAY
    order by o.number

 """)
    return SQL

def SQL_producthub():

    SQL = """
    with x_prices as (
        select
            offer_id,
            argMax(price,dt) price,
            argMax(discount,dt) discount,
            argMax(discount_starts_at,dt) discount_starts_at,
            argMax(discount_ends_at,dt) discount_ends_at
        from ods.product_hub__product_price
        where 1 
            and published = 1 
            and retailer_id = 25354
        group by offer_id
            ), 

        x_offers as (
            select
                distinct 
                id,
                name as "Наименование Товара",
                sku as "Артикул",
                retailer_sku as "Артикул Ритейлера",
                store_id,
                retailer_id,
                stock,
                max_stock,
                updated_at_offer,
                updated_at_stock,
                vat_rate
            from ods.product_hub__offer_x_stock
            where 1
                and deleted_at is null 
                and published = 1
                and status = 1
                and retailer_id = 25354
                )
        select 
            x_offers.id offer_id,
            "Наименование Товара",
            "Артикул Ритейлера"
        from x_offers
            left join x_prices on x_prices.offer_id = x_offers.id
        """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

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
        , c.kpp "КПП"
        , c.name "Название компании"
        , ifNull(u.user_email,'zabotab2b@sbermarket.ru') as "Почта ответственного менеджера"
        ,REPLACE(REPLACE(REPLACE(toString(groupUniqArray(if(p.phone = '','78005509411',p.phone))), '[', ''), ']', ''),'''',' ') as "Телефон ответственного менеджера"
        , ifNull(u.user_fio,'Группа поддержки b2b') as "ФИО ответственного менеджера"
        , con_flag
        from (select id company_id, inn, kpp, name, if(postpay_flg or deposit_flg, 'postpay', 'another') con_flag from gp_rep.rep__companies where mt_src_cd = 'B2B_COMPANY') c 
        left join cm_users u on u.company_id = c.company_id
        left join phones p on p.user_id = u.user_id
        where 1 
        group by 1,2,3,4,5,7,8
        """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def pushed_df():

    SQL = """
            select distinct shipment_number from cdm.b2b__pulses_logs where process_name = 'umniy_retailer_pulse'
            """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

email = ['b2b@samokat.ru','aleksandra.pekhoto@sbermarket.ru', 'olga.a.orlova@sbermarket.ru']

PATH = 'Umniy_Retailer_Completed_Orders.xlsx'

def send_mail_dag():

    df = SQL_tables()
    df.company_id = df.company_id.astype('str')

    pushed = pushed_df()
    df = df[~df.shipment_number.isin(pushed.shipment_number)]

    if len(df) >0:
        managers_df = SQL_managers()
        managers_df.company_id = managers_df.company_id.astype('str')

        producthub_df = SQL_producthub()

        df = df.merge(producthub_df,on = 'offer_id')
        df = df.merge(managers_df,how = 'left',on = 'company_id')
        
        df["Почта ответственного менеджера"] = df["Почта ответственного менеджера"].fillna('viktoria.samonaeva@sbermarket.ru')
        df["Телефон ответственного менеджера"] = df["Телефон ответственного менеджера"].fillna('79103036925')
        df["ФИО ответственного менеджера"] = df["ФИО ответственного менеджера"].fillna('Самонаева Виктория')

        log_df = df[['shipment_number']].drop_duplicates()
        log_df['process_name'] = 'umniy_retailer_pulse'
        log_df['shipment_logged_at'] = pd.datetime.now() + timedelta(hours = 3)
        
        ch_dataframe_to_clickhouse(log_df.drop_duplicates(), "cdm.b2b__pulses_logs", ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

        df = df.drop(columns = ['offer_id','company_id','shipment_number'])
        df = df.drop_duplicates()

        df = df[["Дата и время заказа","Дата и время доставки","Статус заказа","Адрес доставки","Номер заказа Сбермаркет"
                ,"Почта клиента","Телефон клиента","ИНН","КПП","Название компании","Почта ответственного менеджера","Телефон ответственного менеджера","ФИО ответственного менеджера"
                ,"Наименование Товара","Артикул Ритейлера","Количество товара","Цена товара"]]

        df.to_excel(PATH, index=False)
        
        send_mail(to= email ,
          subject="Оформленыые заказы Сбермаркет на подтверждение",
          text="Оформленыые заказы Сбермаркет на подтверждение",
          file_paths=[PATH])
    else:
        logging.info('No Data Received')

with DAG(

        dag_id='b2b_umniy_retailer_pulse',
        description='b2b_umniy_retailer_pulse',
        schedule_interval='0/10 * * * *',
        start_date=datetime(2024, 1, 18, 10, 0, 0, 0),
        max_active_runs=1,
        tags=['b2b'],
        catchup=False

) as dag:

    send = PythonOperator(
        task_id='to_send',
        python_callable=send_mail_dag
    )

    send
