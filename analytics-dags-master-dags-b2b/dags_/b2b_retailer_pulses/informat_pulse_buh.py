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

mysql_hook = MySqlHook(mysql_conn_id='mysql_instamart')

from libs.dwh_ch import ch_pd_read_sql_query, DwhChCheckQuerySensor, ch_dataframe_to_clickhouse
from libs.airflow_connections import CH_ANALYTICS__MARKETING_B2B__CONN_ID

email = ['svolkova@informat.ru', 'kkostylev@informat.ru', 'artem.subbotin@sbermarket.ru','spb.vva@informat.ru','spb.vee@informat.ru','spb.iov@informat.ru','ufa.ak@informat.ru','ufa.lg@informat.ru','ufa.lz@informat.ru', 'nikolai.romanov@sbermarket.ru','daniil.mazakin@sbermarket.ru']
PATH = 'Buhgalteria.xlsx'


def SQL_tables():

    

    SQL = mysql_hook.get_pandas_df( """ 

    select distinct
    convert_tz(o.completed_at, 'UTC', 'Europe/Moscow') as 'Дата и время заказа'
    , convert_tz(dw.ends_at, 'UTC', 'Europe/Moscow') as 'Дата и время доставки'
    , a.full_address as 'Адрес доставки'
    , s.number as 'Номер заказа Сбермаркет'
    , li.quantity as 'Количество'
    , li.price as 'Цена'
    , li.offer_id offer_id
    , co.company_id company_id
    , o.state as 'Статус заказа'
    from
        (
        select order_id,delivery_window_id,number
        from spree_shipments 
        where retailer_id = 211 and delivery_window_id is not null
        ) s
    join delivery_windows dw on dw.id = s.delivery_window_id
    join spree_orders o on o.id = s.order_id
    join companies_orders co on co.order_id = o.id 
    join spree_line_items li on li.order_id = s.order_id
    left join spree_addresses a on a.id = o.ship_address_id
    where 1=1
    and o.state in('complete', 'canceled')
    and li.deleted_at is null
    and li.retailer_id = 211
    and case when DAYOFWEEK(convert_tz(now() , 'UTC', 'Europe/Moscow')) = 2 and HOUR(convert_tz(now() , 'UTC', 'Europe/Moscow')) = 0 then convert_tz(o.completed_at, 'UTC', 'Europe/Moscow') > convert_tz(now() , 'UTC', 'Europe/Moscow') - INTERVAL 2 DAY - INTERVAL 1 HOUR
                                                        else convert_tz(o.completed_at, 'UTC', 'Europe/Moscow') > convert_tz(now() , 'UTC', 'Europe/Moscow') - INTERVAL 1 HOUR
        end
    order by s.number
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
            and retailer_id = 211
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
                and retailer_id = 211
                )

        select 
            x_offers.id offer_id,
            "Наименование Товара",
            "Артикул Ритейлера"
        from x_offers
            left join x_prices on x_prices.offer_id = x_offers.id
        """
    return  ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

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
        from (select id company_id, name, inn from gp_rep.rep__companies where mt_src_cd = 'B2B_COMPANY') c 
        left join cm_users u on u.company_id = c.company_id
        left join phones p on p.user_id = u.user_id
        where 1 
        group by 1,2,3,4,6
        """
    return  ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def send_mail_dag():

    df = SQL_tables()
    managers_df = SQL_managers()
    producthub_df = SQL_producthub()
    df = df.merge(producthub_df,on = 'offer_id')
    df = df.merge(managers_df,how = 'left',on = 'company_id')
    df["Почта ответственного менеджера"] = df["Почта ответственного менеджера"].fillna('viktoria.samonaeva@sbermarket.ru')
    df["Телефон ответственного менеджера"] = df["Телефон ответственного менеджера"].fillna('79103036925')
    df["ФИО ответственного менеджера"] = df["ФИО ответственного менеджера"].fillna('Самонаева Виктория')

    df = df.drop(columns = ['offer_id','company_id'])
    df = df.drop_duplicates()
    df.to_excel(PATH, index=False)

    if len(df) >0:
        send_mail(to= email ,
          subject="Заказы Сбермаркет на подтверждение (для бухгалтерии inФОРМАТ)",
          text="Заказы Сбермаркет на подтверждение (для бухгалтерии inФОРМАТ)",
          file_paths=[PATH])
    else:
        logging.info('No Data Received')


with DAG(

        dag_id='informat_buh',
        description='informat_buh',
        schedule_interval='02 */1 * * 1-5',
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
