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

from libs.dwh_ch import ch_pd_read_sql_query, DwhChCheckQuerySensor, ch_dataframe_to_clickhouse
from libs.airflow_connections import CH_ANALYTICS__MARKETING_B2B__CONN_ID

mysql_hook = MySqlHook(mysql_conn_id='mysql_instamart')

def from_next_cloud_to_dataframe(path, **kwargs):
    conn_obj = NextCloud.SpreadNextCloud() # creating connector object to NextCloud
    df = conn_obj.download_file_to_df(path, **kwargs) # path = absolute or relative path. Eg: '/home/user/file.csv' or 'file.csv'
    return df

def from_dataframe_to_next_cloud(df, path, **kwargs):
    conn_obj = NextCloud.SpreadNextCloud() # creating connector object to NextCloud
    conn_obj.upload_df_to_nextcloud(df=df, remote_path=path, **kwargs) # this uploads your df to nextcloud for the provided remote_path
    return True


email = ['89153539612@mail.ru','grigoriy.zorin@sbermarket.ru']

def prodway_orders():

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
        where retailer_id = 23173 and delivery_window_id is not null
        and deleted_at is null
        ) s
        join delivery_windows dw on dw.id = s.delivery_window_id
        join spree_orders o on o.id = s.order_id
        join spree_addresses a on o.ship_address_id = a.id
        join companies_orders co on co.order_id = o.id
        join spree_line_items li on li.shipment_id = s.shipment_id and li.deleted_at is null
    where 1=1
        and o.state in('complete','resumed')
        and date(convert_tz(dw.starts_at, 'UTC', 'Europe/Moscow')) >= now() - INTERVAL 14 DAY
    order by  shipment_number, convert_tz(o.completed_at, 'UTC', 'Europe/Moscow') 
    """
    )
    return SQL

def prodexpress_orders():

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
        where retailer_id = 24219 and delivery_window_id is not null
        and deleted_at is null
        ) s
        join delivery_windows dw on dw.id = s.delivery_window_id
        join spree_orders o on o.id = s.order_id
        join spree_addresses a on o.ship_address_id = a.id
        join companies_orders co on co.order_id = o.id
        join spree_line_items li on li.shipment_id = s.shipment_id and li.deleted_at is null
    where 1=1
        and o.state in('complete','resumed')
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
            and retailer_id  in (23173,24219)
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
                and retailer_id  in (23173,24219)
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
            select distinct shipment_number from cdm.b2b__pulses_logs where process_name in ('prodway_pulse','prodexpress_pulse')
            """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def send_mail_dag():

    pushed = pushed_df()

    df_prodway = prodway_orders()
    df_prodway = df_prodway[~df_prodway.shipment_number.isin(pushed.shipment_number)]

    ######## separate pusle ########

    df_prodexpress = prodexpress_orders()
    df_prodexpress = df_prodexpress[~df_prodexpress.shipment_number.isin(pushed.shipment_number)]
    

    if len(df_prodway) >0:

        managers_df = SQL_managers()
        producthub_df = SQL_producthub()
        df_prodway = df_prodway.merge(producthub_df,on = 'offer_id')

        df_prodway = df_prodway.merge(managers_df,how = 'left',on = 'company_id')
        df_prodway["Почта ответственного менеджера"] = df_prodway["Почта ответственного менеджера"].fillna('viktoria.samonaeva@sbermarket.ru')
        df_prodway["Телефон ответственного менеджера"] = df_prodway["Телефон ответственного менеджера"].fillna('79103036925')
        df_prodway["ФИО ответственного менеджера"] = df_prodway["ФИО ответственного менеджера"].fillna('Самонаева Виктория')

        df_prodway = df_prodway.drop(columns = ['offer_id','company_id'])
        df_prodway = df_prodway.drop_duplicates()

        log_df = df_prodway[['shipment_number']].drop_duplicates()
        log_df['process_name'] = 'prodway_pulse'
        log_df['shipment_logged_at'] = pd.datetime.now() + timedelta(hours = 3)

        ch_dataframe_to_clickhouse(log_df.drop_duplicates(), "cdm.b2b__pulses_logs", ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

        df_prodway = df_prodway.rename(columns={"shipment_number": "Номер заказа"})

        for i in np.unique(df_prodway['Номер заказа'].values):
            pmdf = df_prodway[df_prodway['Номер заказа'] == i]
            PATH = f'Sbermarket_orders_{i}.xlsx'
            pmdf.to_excel(PATH, index=False)
            send_mail(to = email ,
                      subject = f"Заказ {i} СберМаркет ритейлер ПродВей",
                      text = f"Заказ {i} СберМаркет ритейлер ПродВей",
                      file_paths = [PATH]
                      )
    else:
        logging.info('No Data Received ProdWay')


    if len(df_prodexpress) >0:
        managers_df = SQL_managers()
        producthub_df = SQL_producthub()

        df_prodexpress = df_prodexpress.merge(producthub_df,on = 'offer_id')

        df_prodexpress = df_prodexpress.merge(managers_df,how = 'left',on = 'company_id')
        df_prodexpress["Почта ответственного менеджера"] = df_prodexpress["Почта ответственного менеджера"].fillna('viktoria.samonaeva@sbermarket.ru')
        df_prodexpress["Телефон ответственного менеджера"] = df_prodexpress["Телефон ответственного менеджера"].fillna('79103036925')
        df_prodexpress["ФИО ответственного менеджера"] = df_prodexpress["ФИО ответственного менеджера"].fillna('Самонаева Виктория')

        df_prodexpress = df_prodexpress.drop(columns = ['offer_id','company_id'])
        df_prodexpress = df_prodexpress.drop_duplicates()

        log_df = df_prodexpress[['shipment_number']].drop_duplicates()
        log_df['process_name'] = 'prodexpress_pulse'
        log_df['shipment_logged_at'] = pd.datetime.now() + timedelta(hours = 3)

        ch_dataframe_to_clickhouse(log_df.drop_duplicates(), "cdm.b2b__pulses_logs", ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

        df_prodexpress = df_prodexpress.rename(columns={"shipment_number": "Номер заказа"})
        for i in np.unique(df_prodexpress['Номер заказа'].values):
            pmdf = df_prodexpress[df_prodexpress['Номер заказа'] == i]
            PATH = f'b2b_prodexpress_orders_{i}.xlsx'
            pmdf.to_excel(PATH, index=False)
            send_mail(to = email ,
                      subject = f"Заказ {i} ритейлер ПродЭкспресс",
                      text = f"Заказ {i} ритейлер ПродЭкспресс",
                      file_paths = [PATH]
                      )
    else:
        logging.info('No Data Received ProdExpress')
        
    

with DAG(

        dag_id='prodway_prodexpress_pulse',
        description='prodway_prodexpress_pulse',
        schedule_interval='0/12 * * * *',
        start_date=datetime(2024, 4, 18, 10, 0, 0, 0),
        max_active_runs=1,
        tags=['b2b'],
        catchup=False

) as dag:

    send = PythonOperator(
        task_id='to_send',
        python_callable=send_mail_dag
    )

    send