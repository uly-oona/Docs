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

def SQL_tables1(ds):
    
    SQL = f""" 

    with stores_orders as (
        select 
            shipped_at
            , shipment_id
            , store_name
        from 
            prod_marketing.main_marketing_dash  
        where 1
            and flag_b2b in ('представители компании','галочки')
            and retailer_id = 15 
            and shipment_state = 'shipped'
            and shipped_at > '2022-01-01'
        ),

    stores_orders_inc as (
        select 
            shipment_id
            , retailer_sku 
            , price 
        from 
            line_items 
        where 
            shipment_id in (select distinct shipment_id from stores_orders)
            and li_deleted_at is null
        )
        
    select 
        *
    from 
    (
    select 
        formatDateTime(toStartOfWeek(t1.shipped_at, 1),'%%Y-%%m-%%d') as "Неделя"
        , t1.store_name as "Наименование магазина"
        , t2.retailer_sku as "Артикул"
        , avg(t2.price) as "Средняя цена артикула за неделю"
        , count(distinct t1.shipment_id) as "Кол-во заказов артикула за неделю"
    from 
        stores_orders t1 
    left join 
        stores_orders_inc t2 
        on t1.shipment_id = t2.shipment_id
    group by 
        "Неделя"
        , "Наименование магазина"
        , "Артикул"
    order by 
        "Неделя" desc 
        , "Наименование магазина"
        , "Кол-во заказов артикула за неделю" desc 
    )
    where 1=1 
        and toDate("Неделя") = toDate('{ds}')

     """

    df = ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

    return df

def SQL_tables2(ds):

    SQL = f""" 

    with data as (
        select
        * 
        from prod_marketing.main_marketing_dash 
        where 1 
        and flag_b2b in ('представители компании','галочки')
        and retailer_id = 15 
        and shipment_state = 'shipped'
        and shipped_at > '2022-01-01'
       ),
    
     stores_general_info as (
        select 
            toStartOfWeek(shipped_at, 1) as week 
            , store_id 
            , store_name
            , sum(gmv_service_fee_net_promo + gmv_goods_net_promo) as gmv_net_promo
            , sum(gmv_service_fee+ gmv_goods) as gmv
            , count (distinct shipment_id) as orders_cnt
        from 
            data 
        group by 
            week 
            , store_id
            , store_name
        ), 
        
        stores_orders_inc as (
        select 
            shipment_id
            ,shipment_shipped_at
            ,store_id
            , count (distinct retailer_sku) as retailer_sku_cnt
        from 
            line_items 
        where 
            shipment_id in (select distinct shipment_id from data)
            and li_deleted_at is null
        group by 
            shipment_id,shipment_shipped_at,store_id
        ),
        
    stores_retailer_sku_info as (
        select 
            toStartOfWeek(shipment_shipped_at, 1) as week 
            , store_id
            , sum(retailer_sku_cnt) as sum_retailer_sku_cnt
        from 
            stores_orders_inc t2 
            
        group by 
            week
            , store_id
        )


    select 
        *
    from 
    (
    select 
        formatDateTime(t1.week, '%%Y-%%m-%%d') as "Неделя"
        , t1.store_name as "Наименование магазина"
        , t1.gmv as "Сумма заказов"
        , t1.orders_cnt as "Кол-во заказов"
        , t2.sum_retailer_sku_cnt as "Кол-во артикулов"
        
    from 
        stores_general_info t1 
    left join 
        stores_retailer_sku_info t2 
        on t1.week = t2.week and t1.store_id = t2.store_id
    order by 
        "Неделя" desc 
        , "Наименование магазина"
    )
    where 1=1 
        and toDate("Неделя") = toDate('{ds}')

     """

    df = ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

    return df

def SQL_tables3(ds):

    SQL = f""" 

    with stores_orders as (
            select 
                shipped_at
                , shipment_id
                , store_name
            from 
                prod_marketing.main_marketing_dash  
            where 1
                and flag_b2b in ('представители компании','галочки')
                and retailer_id = 15 
                and shipment_state = 'shipped'
                and shipped_at > '2022-01-01'
            ),

        stores_orders_inc as (
            select 
                shipment_id
                , retailer_sku 
                , product_name
                , price 
                , if(master_category = '', 'Категория не определена',master_category) master_category
                , if( m.id = 0, 'Категория не определена',m.name) parent_category
            from 
                line_items li 
            left join int_master_categories m  on li.parent_category_id = m.id
            where 
                shipment_id in (select distinct shipment_id from stores_orders)
                and li_deleted_at is null
            )
            

        select 
            *
        from 
        (
        select 
            formatDateTime(toStartOfWeek(t1.shipped_at, 1),'%%Y-%%m-%%d') as "Неделя"
            , t2.master_category as "Категория"
            , t2.parent_category as "Родительская категория"
            , avg(t2.price) as "Средняя цена категории за неделю"
            , count(distinct t1.shipment_id) as "Кол-во заказов категории за неделю"
        from 
            stores_orders t1 
        left join 
            stores_orders_inc t2 
            on t1.shipment_id = t2.shipment_id
        group by 
            "Неделя"
            , "Категория"
            , "Родительская категория"
        order by 
            "Неделя" desc 
            , "Категория"
            , "Кол-во заказов категории за неделю" desc 
        )
        where 1=1 
            and toDate("Неделя") = toDate('{ds}')
     """

    df = ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)
    
    return df

def send_mail_dag(**kwargs):

    df1 = SQL_tables1(kwargs['execution_date'])
    df2 = SQL_tables2(kwargs['execution_date'])
    df3 = SQL_tables3(kwargs['execution_date'])
    
    PATH1 = 'b2b_продажи_АШАН_артикулы.xlsx'
    PATH2 ='b2b_продажи_АШАН.xlsx'
    PATH3 = 'b2b_продажи_АШАН_категории.xlsx'

    df1.to_excel(PATH1, index=False)
    df2.to_excel(PATH2, index=False)
    df3.to_excel(PATH3, index=False)

    email = ['o.luzgina@auchan.ru', 'roman.gaylit@sbermarket.ru','a.antonyuk@auchan.ru', 'b2b@auchan.ru', 'olga.a.orlova@sbermarket.ru']
    if len(df1) > 0 and len(df2) > 0:
        send_mail(to=email,
          subject="Статистика по b2b продажам АШАН в разрезе магазинов, артикулов, категорий",
          text="Статистика по b2b продажам АШАН в разрезе магазинов, артикулов, категорий",
          file_paths=[PATH1,PATH2,PATH3])
    else:
        logging.info('No Data Received')


with DAG(

        dag_id='b2b_auchan_pulse',
        description='b2b_auchan_pulse',
        schedule_interval='0 9 * * 1',
        start_date=datetime(2022, 12, 19, 7, 0, 0, 0),
        tags=['b2b'],
        catchup=False

) as dag:

    send = PythonOperator(
        task_id='to_send',
        python_callable=send_mail_dag,
        op_kwargs={
            'execution_date': "{{ ds }}"
        }
    )

    send
