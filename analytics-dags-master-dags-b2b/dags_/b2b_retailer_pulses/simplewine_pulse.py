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

from libs.dwh_ch import ch_pd_read_sql_query, DwhChCheckQuerySensor, ch_dataframe_to_clickhouse
from libs.airflow_connections import CH_ANALYTICS__MARKETING_B2B__CONN_ID

email = ['oavdeeva@simple.ru', 'olga.a.orlova@sbermarket.ru','aprihodko@simple.ru','aleksandra.pekhoto@sbermarket.ru']


def SQL_tables():

    SQL = mysql_hook.get_pandas_df( 
    """ 
select distinct
        convert_tz(o.completed_at, 'UTC', 'Europe/Moscow') as 'Дата и время заказа'
        , convert_tz(dw.ends_at, 'UTC', 'Europe/Moscow') as 'Ожидаемые дата и время доставки'
        , s.number as shipment_number
        , o.special_instructions as "Комментарий к заказу"
        , concat(a.firstname,' ',a.lastname) as "ФИО Клиента"
        , a.phone as "Телефон клиента"
        , o.email as "Почта клиента"
        , li.quantity as "Кол-во товара"
        , li.price as "Цена товара"
        , co.company_id company_id
        , li.offer_id offer_id
    from
        (
        select id as shipment_id, order_id,number,delivery_window_id 
        from spree_shipments 
        where(store_id = 93369) and delivery_window_id is not null
        and deleted_at is null
        ) s
    join delivery_windows dw on dw.id = s.delivery_window_id
    join spree_orders o on o.id = s.order_id
    join spree_addresses a on o.ship_address_id = a.id
    join companies_orders co on co.order_id = o.id
    join spree_line_items li on li.shipment_id = s.shipment_id and li.deleted_at is null
    where 1=1
    and date(convert_tz(dw.starts_at, 'UTC', 'Europe/Moscow')) >= now() - INTERVAL 14 DAY
  order by  shipment_number, convert_tz(o.completed_at, 'UTC', 'Europe/Moscow') 
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
        from (select id company_id, name, inn from gp_rep.rep__companies where mt_src_cd = 'B2B_COMPANY') c 
        left join cm_users u on u.company_id = c.company_id
        left join phones p on p.user_id = u.user_id
        where 1 
        group by 1,2,3,4,6
        """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

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
            and store_id in (93369)
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
                and store_id in (93369)
                )

        select 
            x_offers.id offer_id,
            "Наименование Товара",
            "Артикул Ритейлера"
        from x_offers
            left join x_prices on x_prices.offer_id = x_offers.id
        """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def pushed_df():

    SQL = """
            select distinct shipment_number from cdm.b2b__pulses_logs where process_name = 'sipmlewine_pulse'
            """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def send_mail_dag():

    pushed = pushed_df()

    df = SQL_tables()    
    df = df[~df.shipment_number.isin(pushed.shipment_number)]

    if len(df) >0:
        
        managers_df = SQL_managers()
        producthub_df = SQL_producthub()
        df = df.merge(producthub_df,on = 'offer_id')

        df = df.merge(managers_df,how = 'left',on = 'company_id')
        df["Почта ответственного менеджера"] = df["Почта ответственного менеджера"].fillna('viktoria.samonaeva@sbermarket.ru')
        df["Телефон ответственного менеджера"] = df["Телефон ответственного менеджера"].fillna('79103036925')
        df["ФИО ответственного менеджера"] = df["ФИО ответственного менеджера"].fillna('Самонаева Виктория')

        df = df.drop(columns = ['offer_id','company_id'])
        df = df.drop_duplicates()

        log_df = df[['shipment_number']].drop_duplicates()
        log_df['process_name'] = 'sipmlewine_pulse'
        log_df['shipment_logged_at'] = pd.datetime.now() + timedelta(hours = 3)
        
        ch_dataframe_to_clickhouse(log_df.drop_duplicates(), "cdm.b2b__pulses_logs", ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)
        
        df = df.rename(columns={"shipment_number": "Номер заказа"})
        for i in np.unique(df['Номер заказа'].values):
            pmdf = df[df['Номер заказа'] == i]
            PATH = f'Sbermarket_orders_{i}.xlsx'
            pmdf.to_excel(PATH, index=False)
            send_mail(to = email ,
                      subject = f"Заказ {i} Сбермаркет из SimpleWine",
                      text = f"Заказ {i} Сбермаркет из SimpleWine",
                      file_paths = [PATH]
                      )
    else:
        logging.info('No Data Received')


        
with DAG(

        dag_id='simplewine_pulse',
        description='simplewine_pulse',
        schedule_interval='0/12 * * * *',
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
