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

from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

ch_hook = ClickHouseHook(clickhouse_conn_id='clickhous_analytics')

from libs.dwh_ch import ch_pd_read_sql_query, DwhChCheckQuerySensor, ch_dataframe_to_clickhouse
from libs.airflow_connections import CH_ANALYTICS__MARKETING_B2B__CONN_ID

def SQL_tables1():

    SQL = """ 
        with alco_orders as (

        select distinct shipment_id
        from line_items 
        where 1
        and (parent_category_id = 200000 or parent_category_id = 453) 
        and li_deleted_at is null
        and retailer_id in (8,23939)
        ),
        
        -- аггрегированная таблица без декомпозицию до артикулов 
        aggregated_table as (
            select 
                formatDateTime(toStartOfMonth(shipped_at), '%%Y-%%m') as "Месяц"
                , store_id as "Id магазина"
                , store_name as "Наименование магазина"
                , count (distinct order_number) as "Количество заказов"
                , count(distinct case when payment_method_name = 'По счёту для бизнеса' then order_number else null end) as "Число заказов с оплатой переводом"
                , sum(cogs_adjusted) as "Выручка Лента"
                , avg(cogs_adjusted) as "Средний чек"
                , avg(case when payment_method_name = 'По счёту для бизнеса' then cogs_adjusted else null end ) as "Средний чек с оплатой переводом"
                , count(distinct case when total_weight > 80000 then order_number else null end) as "Число тяжелых заказов (более 80 кг)"
                , count(distinct case when total_weight > 80000 and payment_method_name = 'По счёту для бизнеса' then order_number else null end) as "Число тяжелых заказов с оплатой переводом"
            from gp_rep.rep__bi_shipment s
            left join alco_orders ao on ao.shipment_id = s.shipment_id
            where shipment_state = 'shipped'
                and retailer_id in (8,23939)
                and shipped_at > '2023-01-01'
                and b2b_measure in (1, 2)
                and ao.shipment_id = 0
            group by 
                "Месяц"
                , store_id
                , store_name
            order by 
                "Месяц" desc 
                , store_id asc 
            )

        select 
            *
        from 
            aggregated_table
             """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def SQL_tables2():

    SQL = """ 
-- декомпозиция месяцев до товаров 
        with alco_orders as (
        select distinct shipment_id
        from line_items 
        where 1
        and (parent_category_id = 200000 or parent_category_id = 453) 
        and li_deleted_at is null
        and retailer_id in (8,23939)
        ),

        b2b_orders as (
        select 
        order_id, shipped_at
        from gp_rep.rep__bi_shipment
        where shipment_closed_flg
        and shipment_state = 'shipped'
        and b2b_measure in (1,2)
        and toStartOfWeek(shipped_at,1) = toStartOfWeek(now(),1)  - interval 1 week

        ),

        orders_with_sku as(
        select 
            formatDateTime(toStartOfWeek(shipped_at,1), '%%Y-%%m-%%d') as "Неделя"
            , li.store_id as "Id магазина"
            , sc.veeroute_identifier as "Наименование магазина"
            , li.retailer_sku as "Артикул товара"
            , li.product_name as "Наименование товара"
            , sum(li.quantity) as "Количество проданного товара"
            , sum((1-li.vat_rate/100)*li.price*li.quantity) as "Сумма проданного товара (без ндс)"
        from line_items li
        join b2b_orders b on b.order_id = li.order_id
        join int_stores s on s.id = li.store_id and s.training = 0 
        join int_store_configs sc on li.store_id = sc.store_id
        left join alco_orders ao on ao.shipment_id = li.shipment_id
        where li.retailer_id in (8,23939) 
            and li.li_deleted_at is null 
            and ao.shipment_id = 0
        group by 
            "Неделя"
            , li.store_id
            , sc.veeroute_identifier
            , li.retailer_sku
            , li.product_name
        order by 
            "Неделя" desc
            , li.store_id asc
            , "Сумма проданного товара (без ндс)" desc
        )

        select 
            *
        from 
            orders_with_sku

            """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def split_dataframe(df, chunk_size = 1000000): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

email = ['roman.gaylit@sbermarket.ru','anna.butorina@lenta.com','Anastasiya.plokhodko@lenta.com',
'ekaterina.marten@lenta.com','olga.a.orlova@sbermarket.ru','yuliya.i.pozdnyakova@lenta.com',
'natalya.strigacheva@lenta.com','olga.buyanova@lenta.com','aleksandra.pekhoto@sbermarket.ru']
PATH1 = 'b2b_lenta_orders_info.xlsx'
PATH2='b2b_lenta_orders_with_sku.xlsx'


def send_mail_dag():
    
    df1 = SQL_tables1()
    df2 = SQL_tables2()
    df1.to_excel(PATH1, index=False)

# на случай, если файл слишком большой ...
    if len(df2) > 1048576:
        writer = pd.ExcelWriter(PATH2)
        for i in range(len(split_dataframe(df2))):
            split_dataframe(df2)[i].to_excel(writer,sheet_name = f'sheet_{i}',index = False)
        writer.save()
    else:
        df2.to_excel(PATH2, index=False)
    
    send_mail(to= email ,
        subject="Статистика по b2b продажам Ленты",
        text="Статистика по b2b продажам Ленты",
        file_paths=[PATH1,PATH2])


with DAG(

        dag_id='b2b_lenta_weekly_stats',
        description='b2b_lenta_orders',
        schedule_interval='00 12 * * 1',
        start_date=datetime(2022, 2, 22, 10, 0, 0, 0),
        tags=['b2b'],
        catchup=False

) as dag:

    send = PythonOperator(
        task_id='to_send',
        python_callable=send_mail_dag
    )

    send
