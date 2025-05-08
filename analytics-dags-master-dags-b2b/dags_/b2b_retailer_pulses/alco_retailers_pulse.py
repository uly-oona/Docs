import pandas as pd
import numpy as np
import mysql.connector
from gspread_pandas import Spread
import psycopg2 as pg
from time import sleep
import sqlalchemy as sa
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

from libs.dwh_ch import ch_pd_read_sql_query, DwhChCheckQuerySensor, ch_dataframe_to_clickhouse
from libs.airflow_connections import CH_ANALYTICS__MARKETING_B2B__CONN_ID

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


def alco_orders():

    SQL = mysql_hook.get_pandas_df( 
        '''
          select distinct
            convert_tz(o.completed_at, 'UTC', 'Europe/Moscow') as 'Дата и время заказа'
            , concat(o.delivery_date,' ',o.delivery_time) as 'Ожидаемые дата и время доставки'
            , o.number as "Номер Заказа (R)"
            , s.number as "Номер Заказа (H)"
            , s.retailer_name "Ритейлер"
            , concat(a.firstname,' ',a.lastname) as "ФИО Клиента"
            , a.phone as "Телефон клиента"
            , o.email as "Почта клиента"
            , co.company_id company_id
            , li.offer_id offer_id
            , li.quantity  "Кол-во товара"
            , li.price  "Цена товара"
            from spree_orders o 
            join  
                (
                select 
                    order_id, number, r.name retailer_name, s.retailer_id retailer_id
                from spree_shipments s
                join spree_retailers r on r.id = s.retailer_id
                where s.retailer_id in (53,226,235,296,304,322,421,3989,3990) 
                and s.deleted_at is null 
                ) s on s.order_id = o.id
            join spree_payments p on p.order_id = o.id
            join spree_addresses a on o.ship_address_id = a.id
            join spree_line_items li on li.order_id = o.id and li.deleted_at is null
            left join companies_orders co on co.order_id = o.id
            where 1
            and o.state = 'complete'
            and o.tenant_id = 'sbermarket'
            and p.payment_method_id = 3
            and o.shipping_method_kind = 'by_courier'
            and s.retailer_id in (53,226,235,296,304,322,421,3989,3990)
            and date(convert_tz(o.completed_at, 'UTC', 'Europe/Moscow')) >= date(now()) - interval 2 day
        '''
    )
    return SQL


def SQL_managers():

    SQL = """
        select
            cm.company_id company_id
            ,ifNull(u.email,'viktoria.samonaeva@sbermarket.ru') as manager_email
            -- ,REPLACE(REPLACE(REPLACE(toString(groupUniqArray(if(p.value = '','79103036925',p.value ))), '[', ''), ']', ''),'''',' ') as "Телефон ответственного менеджера"
            -- ,ifNull(u.firstname||' '||u.lastname,'Самонаева Виктория') as "ФИО ответственного менеджера"
        from (select * from gp_rep.rep__company_managers where deleted_at is null ) cm 
        left join int_spree_users u on cm.user_id = u.id 
        left join int_spree_phones p on u.id = p.user_id
        where 1 
        -- group by company_id,"Почта ответственного менеджера", "ФИО ответственного менеджера"
        """
        
    df = ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

    return df

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
            and retailer_id in (53,226,235,296,304,322,421,3989,3990)
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
                and retailer_id in (53,226,235,296,304,322,421,3989,3990)
                )

        select 
            x_offers.id offer_id,
            "Наименование Товара",
            "Артикул Ритейлера"
        from x_offers
            left join x_prices on x_prices.offer_id = x_offers.id
        """

    df = ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

    return df

def pushed_df():

    SQL = """
            select distinct shipment_number from cdm.b2b__pulses_logs where process_name = 'b2b_alco_orders_pulsed'
            """

    df = ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

    return df

def send_mail_dag():

    pushed = pushed_df()
    df = alco_orders()    
    df = df[~df["Номер Заказа (H)"].isin(pushed.shipment_number)]

    if len(df) >0:

        managers_df = SQL_managers()
        producthub_df = SQL_producthub()

        df = df.merge(producthub_df,on = 'offer_id')
        df = df.merge(managers_df,how = 'left',on = 'company_id')

        df = df.drop(columns = ['offer_id','company_id'])
        df = df.drop_duplicates()


        log_df = df[['Номер Заказа (H)']].drop_duplicates()
        log_df = log_df.rename(columns = {"Номер Заказа (H)":'shipment_number'})
        log_df['process_name'] = 'b2b_alco_orders_pulsed'
        log_df['shipment_logged_at'] = pd.datetime.now() + timedelta(hours = 3)

        ch_dataframe_to_clickhouse(log_df.drop_duplicates(), "cdm.b2b__pulses_logs", ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)
        

        for i in np.unique(df['Номер Заказа (R)'].values):
            pmdf = df[df['Номер Заказа (R)'] == i]
            PATH = f'Sbermarket_orders_{i}.xlsx'
            email = ['grigoriy.zorin@sbermarket.ru', 'b2b.team.leads@sbermarket.ru, ','albina.fedorova@sbermarket.ru']
            email = email + list(pmdf['manager_email'].drop_duplicates().values)
            ret = pmdf['Ритейлер'].drop_duplicates().values[0]
            pmdf.to_excel(PATH, index=False)
            send_mail(to = email ,
                      subject = f"Заказ {i} Сбермаркет ритейлер {ret}",
                      text = f"Заказ {i} Сбермаркет ритейлер {ret} неалкогольный ассортимент",
                      file_paths = [PATH]
                      )
    else:
        logging.info('No Data Received')

   
with DAG(

        dag_id='alco_retailers_pulse',
        description='alco_retailers_pulse',
        schedule_interval='00 10,16 * * *',
        start_date=datetime(2023, 12, 11, 10, 0, 0, 0),
        tags=['b2b'],
        catchup=False

) as dag:

    send = PythonOperator(
        task_id='to_send',
        python_callable=send_mail_dag
    )

    send
