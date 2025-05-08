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

from libs.dwh_ch import ch_pd_read_sql_query, DwhChCheckQuerySensor, ch_dataframe_to_clickhouse
from libs.airflow_connections import CH_ANALYTICS__MARKETING_B2B__CONN_ID

email = ['svolkova@informat.ru','kuzmenkova@informat.ru', 'nikolai.romanov@sbermarket.ru','kkostylev@informat.ru', 'artem.subbotin@sbermarket.ru','spb.vva@informat.ru','spb.vee@informat.ru','spb.iov@informat.ru','ufa.ak@informat.ru','ufa.lg@informat.ru','ufa.lz@informat.ru','daniil.mazakin@sbermarket.ru']
PATH = 'Sklad.xlsx'
PATH_2 = 'clients_contact_info.xlsx'

mysql_hook = MySqlHook(mysql_conn_id='mysql_instamart')

def SQL_tables():

    SQL = mysql_hook.get_pandas_df( """ 

        select 
              s.number as 'Номер заказа Сбермаркет'
            , a.full_address as 'Адрес доставки'
            , li.quantity as 'Количество'
            , li.price as 'Цена'
            , li.offer_id offer_id
            , co.company_id company_id
            , case when o.payment_state not in ('not_paid','failed') then 1 else 0 end as pay_flg
        from 
            (
            select order_id,delivery_window_id,number 
            from spree_shipments 
            where retailer_id = 211 
                and delivery_window_id is not null
            ) s
            join delivery_windows dw on dw.id = s.delivery_window_id
            join spree_orders o on o.id = s.order_id
            join companies_orders co on co.order_id = o.id 
            join spree_addresses a on o.ship_address_id = a.id 
            join spree_line_items li on li.order_id = s.order_id
        where 1 = 1
            and o.state  = 'complete'
            and li.retailer_id = 211
            and li.deleted_at is null
            and case when DAYOFWEEK(convert_tz(now(), 'UTC', 'Europe/Moscow')) = 6 then date(convert_tz(now(), 'UTC', 'Europe/Moscow')) + INTERVAL 3 DAY = date(convert_tz(starts_at, 'UTC', 'Europe/Moscow'))
                    else date(convert_tz(now(), 'UTC', 'Europe/Moscow')) + INTERVAL 1 DAY = date(convert_tz(starts_at, 'UTC', 'Europe/Moscow'))
                
            end
 """)
    return SQL

def paper_ords():

    SQL = mysql_hook.get_pandas_df( """ 

            select
            s.number as "Номер заказа",
            a.full_address "Адрес доставки",
            CONCAT(su.firstname,' ',su.lastname)  "Контактное лицо",
            coalesce(su.pending_email, su.email) "Почта",
            a.phone "Телефон"
            , co.company_id company_id
            , case when o.payment_state not in ('not_paid','failed') then 1 else 0 end as pay_flg
        from (
            select 
                order_id,number,
                delivery_window_id 
            from spree_shipments 
            where retailer_id = 211 
                and delivery_window_id is not null
                ) s
            join delivery_windows dw on dw.id = s.delivery_window_id
            join spree_orders o on o.id = s.order_id
            join companies_orders co on co.order_id = o.id 
            join spree_addresses a on o.ship_address_id = a.id 
            join spree_users su on su.id = o.user_id
        where 1 = 1
            and o.state  = 'complete'
            and case when DAYOFWEEK(convert_tz(now(), 'UTC', 'Europe/Moscow')) = 6 then date(convert_tz(now(), 'UTC', 'Europe/Moscow')) + INTERVAL 3 DAY = date(convert_tz(dw.starts_at, 'UTC', 'Europe/Moscow'))
                    else date(convert_tz(now(), 'UTC', 'Europe/Moscow')) + INTERVAL 1 DAY = date(convert_tz(dw.starts_at, 'UTC', 'Europe/Moscow'))
                end
            and o.id in (select distinct order_id from order_promotions where promotion_id in (select promotion_id from promotion_codes where value = 'informat')) # промики для больших заказов с бумагой
        
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
        from (select id company_id, inn, name, kpp, if(postpay_flg or deposit_flg, 'postpay', 'another') con_flag from gp_rep.rep__companies where mt_src_cd = 'B2B_COMPANY') c 
        left join cm_users u on u.company_id = c.company_id
        left join phones p on p.user_id = u.user_id
        where 1 
        group by 1,2,3,4,5,7,8
        """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def send_mail_dag():

    df = SQL_tables()
    managers_df = SQL_managers()
    producthub_df = SQL_producthub()
    df = df.merge(producthub_df,on = 'offer_id')
    df = df.merge(managers_df,how = 'left',on = 'company_id')

    df = df[(df.pay_flg == 1) | (df.con_flag == 'postpay')]

    df["Почта ответственного менеджера"] = df["Почта ответственного менеджера"].fillna('viktoria.samonaeva@sbermarket.ru')
    df["Телефон ответственного менеджера"] = df["Телефон ответственного менеджера"].fillna('79103036925')
    df["ФИО ответственного менеджера"] = df["ФИО ответственного менеджера"].fillna('Самонаева Виктория')

    df = df.drop(columns = ['offer_id','company_id','pay_flg','con_flag'])
    df = df.drop_duplicates()
    
    df.to_excel(PATH, index=False)

    df_p = paper_ords()
    
    df_p = df_p.merge(managers_df,how = 'left',on = 'company_id')
    df_p = df_p[(df_p.pay_flg == 1) | (df_p.con_flag == 'postpay')]

    df_p = df_p.drop(columns = ['company_id','pay_flg','con_flag'])

    df_p.to_excel(PATH_2, index=False)

    if (len(df) > 0) & (len(df_p) > 0):
        send_mail(to=email,
        subject="Оплаченные заказы Сбермаркет на завтра (для склада inФОРМАТ)",
        text="Оплаченные заказы Сбермаркет на завтра (для склада inФОРМАТ)",
        file_paths=[PATH,PATH_2])
    elif (len(df) > 0):
        send_mail(to=email,
        subject="Оплаченные заказы Сбермаркет на завтра (для склада inФОРМАТ)",
        text="Оплаченные заказы Сбермаркет на завтра (для склада inФОРМАТ)",
        file_paths=[PATH])
    else:
        logging.info('No Data Received')

with DAG(

        dag_id='informat_sklad',
        description='informat_sklad',
        schedule_interval='02 15 * * 1-5',
        start_date=datetime(2021, 7, 18, 10, 0, 0, 0),
        tags=['b2b'],
        catchup=False

) as dag:

    send = PythonOperator(
        task_id='to_send',
        python_callable=send_mail_dag
    )

    send
