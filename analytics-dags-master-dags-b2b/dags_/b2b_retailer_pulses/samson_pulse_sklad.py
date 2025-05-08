import pandas as pd
import numpy as np
import mysql.connector
from gspread_pandas import Spread
import psycopg2 as pg
from time import sleep
import sqlalchemy as sa

import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders
from libs.mailer import send_mail

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.mysql_hook import MySqlHook
from libs.helpers import get_clickhouse_connection
from airflow.hooks.base_hook import BaseHook

from airflow.models import Variable
import sys
import os.path as op
import os
import subprocess
from datetime import timedelta, date, datetime
import json
import logging

mysql_hook = MySqlHook(mysql_conn_id='mysql_instamart')

from libs.dwh_ch import ch_pd_read_sql_query, DwhChCheckQuerySensor, ch_dataframe_to_clickhouse
from libs.airflow_connections import CH_ANALYTICS__MARKETING_B2B__CONN_ID


email = ['intmag@samsonpost.ru','grigoriy.zorin@sbermarket.ru','aleksandra.pekhoto@sbermarket.ru']
PATH = 'Sklad.xlsx' 

def SQL_tables():

    SQL = mysql_hook.get_pandas_df( """ 

        select
            order_number as "Номер заказа Сбермаркет",
            convert_tz(completed_at, 'UTC', 'Europe/Moscow') as "Время оформления",
            convert_tz(starts_at, 'UTC', 'Europe/Moscow') as "Время доставки",
            full_address as "Адрес доставки",
            quantity as "Количество товаров",
            coalesce(assembly_comment_order,assembly_comment) "комментарий к сборке заказа",
            special_instructions "инструкции к заказу",
            offer_id,
            company_id,
            pay_flg
        from    
            (
                select distinct
                    order_number
                    ,completed_at
                    ,starts_at
                    ,full_address
                    ,offer_id
                    ,quantity 
                    ,assembly_comment
                    ,special_instructions
                    ,assembly_comment_order
                    ,store_id
                    ,co.company_id company_id
                    ,pay_flg
                from
                    (
                    select 
                        so.number as order_number,
                        so.payment_state,
                        so.id as order_id,
                        so.completed_at,
                        sh.assembly_comment,
                        dw.starts_at,
                        sa.full_address,
                        sli.offer_id,
                        sli.quantity,
                        so.user_id,
                        so.special_instructions special_instructions,
                        so.assembly_comment assembly_comment_order,
                        sh.store_id store_id,
                        case when 
                            (so.payment_state in ('paid','overpaid') or (pp.payment_method_id in (4,11,12,20)) or (bd.order_id <> 0))
                            then 1 
                            else 0 
                        end as pay_flg
                    from 
                        spree_shipments sh
                    join
                        spree_orders so on sh.order_id = so.id and sh.deleted_at is null 
                    join 
                        delivery_windows dw on dw.id = sh.delivery_window_id
                    join 
                        spree_line_items sli on sli.shipment_id = sh.id and sli.deleted_at is null
                    join 
                        spree_addresses sa on sa.id = so.ship_address_id
                    left join 
                        spree_payments pp on pp.order_id = so.id
                    left join 
                        (
                        select distinct order_id
                        from (
                             select 
                                 order_id, 
                                 number,
                                 case when 
                                         (dense_rank() over (partition by order_id order by payment_total) + dense_rank() over (partition by order_id order by payment_total desc) - 1) >1  
                                     then  
                                         sum(payment_total) over (partition by order_id)
                                     else 
                                         payment_total
                                     end as payment_total_corrected,
                                 sum(total) over (partition by order_id)  sum_total
                             from spree_shipments 
                             where order_id in (select o.id from spree_orders o join spree_shipments s on s.order_id = o.id where s.retailer_id = 150 and o.payment_state = 'balance_due')
                                 and deleted_at is null 
                            ) a 
                        where sum_total - payment_total_corrected <= 1000
                        ) bd on bd.order_id = so.id
                    where 1=1
                        and case when WEEKDAY(DATE(now())) = 4 then DATE(dw.starts_at) = DATE(now() + interval 3 day) else DATE(dw.starts_at) = DATE(now() + interval 1 day) end
                        and sh.retailer_id = 150
                        and so.state in ('complete', 'resumed')
                        and so.id not in (select distinct order_id from b2b_unpaid_orders where export_lock_state = 'locked')
                    ) komus_shipments
                left join companies_orders co on komus_shipments.order_id = co.order_id
                ) res
        order by convert_tz(starts_at, 'UTC', 'Europe/Moscow') ,order_number
        
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
            and retailer_id = 150
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
                and retailer_id = 150
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
        , c.name "Название компании"
        , ifNull(u.user_email,'zabotab2b@sbermarket.ru') as "Почта ответственного менеджера"
        ,REPLACE(REPLACE(REPLACE(toString(groupUniqArray(if(p.phone = '','78005509411',p.phone))), '[', ''), ']', ''),'''',' ') as "Телефон ответственного менеджера"
        , ifNull(u.user_fio,'Группа поддержки b2b') as "ФИО ответственного менеджера"
        , con_flag
        from (select id company_id, name, inn, if(postpay_flg or deposit_flg, 'postpay', 'another') con_flag from gp_rep.rep__companies where mt_src_cd = 'B2B_COMPANY') c 
        left join cm_users u on u.company_id = c.company_id
        left join phones p on p.user_id = u.user_id
        where 1 
        group by 1,2,3,4,6,7
        """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def send_mail_dag():

    df = SQL_tables()
    df.company_id = df.company_id.astype('str')
    managers_df = SQL_managers()
    managers_df.company_id = managers_df.company_id.astype('str')
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
    
    send_mail(to= email ,
          subject="Оплаченные заказы Самсон из Сбермаркет на завтра",
          text="Оплаченные заказы Самсон из Сбермаркет на завтра",
          file_paths=[PATH])


with DAG(

        dag_id='samson_sklad',
        description='samson_sklad',
        schedule_interval='30 13 * * 1-5',
        start_date=datetime(2021, 7, 18, 10, 0, 0, 0),
        tags=['b2b'],
        catchup=False

) as dag:

    send = PythonOperator(
        task_id='to_send',
        python_callable=send_mail_dag
    )

    send
