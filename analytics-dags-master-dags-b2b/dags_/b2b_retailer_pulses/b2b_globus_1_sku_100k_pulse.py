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

def from_next_cloud_to_dataframe(path, **kwargs):
    conn_obj = NextCloud.SpreadNextCloud() # creating connector object to NextCloud
    df = conn_obj.download_file_to_df(path, **kwargs) # path = absolute or relative path. Eg: '/home/user/file.csv' or 'file.csv'
    return df

def from_dataframe_to_next_cloud(df, path, **kwargs):
    conn_obj = NextCloud.SpreadNextCloud() # creating connector object to NextCloud
    conn_obj.upload_df_to_nextcloud(df=df, remote_path=path, **kwargs) # this uploads your df to nextcloud for the provided remote_path
    return True


def SQL_tables():

    SQL = mysql_hook.get_pandas_df( 
    """ 
    with orders as (
    select 
        o.id order_id,
        completed_at,
        payment_state,
        pp.payment_method_id payment_method_id,
        co.company_id company_id
    from spree_orders o 
    join spree_payments pp on pp.order_id = o.id
    join companies_orders co on co.order_id = o.id
    where completed_at > now() - INTERVAL 30 DAY
        and o.state in ('complete', 'resumed')
        and o.id not in (select distinct order_id from b2b_unpaid_orders where export_lock_state = 'locked')
    ),

    shipments as (
    select  
        id, 
        order_id, 
        number,
        store_id,
        item_total,
        delivery_window_id
    from spree_shipments 
    where retailer_id = 11501
        and updated_at > now() - INTERVAL 30 DAY
        and deleted_at is null 
    ),
    
    slots as (
    select 
        id,
        starts_at,
        concat(DATE_FORMAT(starts_at, '%d.%m.%y %H:%i') ,' - ', DATE_FORMAT(ends_at,'%H:%i')) as slot 
    from delivery_windows
    where id in (select delivery_window_id from shipments)
        and starts_at > now() - INTERVAL 30 DAY
    ),
    
    store_keys as (
    select 
        sc.store_id store_id,
        sc.store_key_number store_key_number
    from stores s
        join (
            select 
                substr(import_key, locate('-', import_key)+1,15) store_key_number,
                import_key,
                store_id 
            from store_configs
            ) sc on s.id = sc.store_id
    where s.retailer_id = 11501
    ),

    ord_sh as (
    select 
        o.order_id order_id,
        s.id shipment_id,
        completed_at,
        payment_state,
        payment_method_id,
        s.number shipment_number,
        cast(sk.store_key_number as char) store_key_number,
        slot,
        company_id,
        item_total
    from orders o 
    join shipments s on o.order_id = s.order_id
    join store_keys sk on sk.store_id = s.store_id
    join slots sl on sl.id = s.delivery_window_id
    ), 

    ord_sh_li as (
    select 
        os.order_id order_id,
        os.completed_at completed_at, 
        os.payment_state payment_state,
        os.payment_method_id payment_method_id,
        os.shipment_number shipment_number,
        os.store_key_number store_key_number,
        os.slot slot,
        os.company_id company_id,
        os.item_total item_total,
        li.offer_id offer_id,
        li.price price,
        li.quantity quantity,
        li.price*li.quantity sku_total
    from ord_sh os 
    join spree_line_items li on li.shipment_id = os.shipment_id and li.deleted_at is null
    where (li.price*li.quantity >= 100000 or item_total >= 200000)
    )

    select * from ord_sh_li
    """
    )
    return SQL


def SQL_managers():
        SQL = """
        select
            c.company_id company_id
            ,inn
            ,ifNull(u.email,'zabotab2b@sbermarket.ru') as "Почта ответственного менеджера"
            ,con_flag
            -- ,REPLACE(REPLACE(REPLACE(toString(groupUniqArray(if(p.value = '','78005509411',p.value ))), '[', ''), ']', ''),'''',' ') as "Телефон ответственного менеджера"
            -- ,ifNull(u.firstname||' '||u.lastname,'Группа поддержки b2b') as "ФИО ответственного менеджера"
        from (select id company_id, inn, if(postpay_flg or deposit_flg, 'postpay', 'another') con_flag from gp_rep.rep__companies where mt_src_cd = 'B2B_COMPANY') c 
        left join (select * from gp_rep.rep__company_managers where deleted_at is null ) cm on c.company_id = cm.company_id
        left join int_spree_users u on cm.user_id = u.id 
        -- left join int_spree_phones p on u.id = p.user_id
        where 1 
        -- group by company_id,"Почта ответственного менеджера", "ФИО ответственного менеджера"
            """
        return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)



def SQL_producthub():

        SQL = """
    with    x_prices as (
            select
                offer_id,
                argMax(price,dt) price,
                argMax(discount,dt) discount,
                argMax(discount_starts_at,dt) discount_starts_at,
                argMax(discount_ends_at,dt) discount_ends_at
            from ods.product_hub__product_price
            where 1 
                and published = 1 
                and retailer_id = 11501
            group by offer_id
                ), 

            x_offers as (
                select
                    distinct 
                    id,
                    name as "Наименование Товара",
                    sku,
                    retailer_sku as "Артикул Ритейлера",
                    store_id,
                    retailer_id,
                    stock,
                    max_stock,
                    updated_at_offer,
                    updated_at_stock,
                    vat_rate
                from ods.product_hub__offer_x_stock o
                where 1
                    and deleted_at is null 
                    and published = 1
                    and status = 1
                    and retailer_id = 11501
                    )
            select 
                x_offers.id offer_id,
                x_offers."Наименование Товара" "Наименование Товара",
                x_offers."Артикул Ритейлера" "Артикул Ритейлера"
            from x_offers 
                left join x_prices on x_prices.offer_id = x_offers.id
            where 1 

            """
        return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)


def pushed_df():

    SQL =   """
            select distinct shipment_number from cdm.b2b__pulses_logs where process_name = 'b2b_globus_sku_100k_pulse'
            """
    return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

def get_directors():
    pre_df = from_next_cloud_to_dataframe('b2b_analytics/Почты магазинов Глобус.xlsx')
    pre_df = pre_df.drop_duplicates()    
    return pre_df

def send_mail_dag():

    df = SQL_tables()
    pushed = pushed_df()
    managers_df = SQL_managers()

    df = df.merge(managers_df, how = 'left' ,on = 'company_id')
    df = df[(df.payment_state.isin(['paid','overpaid'])) | (df.con_flag == 'postpay') | (df.payment_method_id.isin([4,11,12,20]))]
    df = df[~df.shipment_number.isin(pushed.shipment_number)]

  
    if len(df) >0:
        
        directors = get_directors()
        directors.store_mail = directors.store_mail.apply(lambda x: x.lower())
        directors.store_key_number = directors.store_key_number.astype('str')

        producthub_df = SQL_producthub()

        df["Почта ответственного менеджера"] = df["Почта ответственного менеджера"].fillna('zabotab2b@sbermarket.ru')

        df = df.merge(producthub_df,on = 'offer_id')

        df = df[['shipment_number', 'completed_at', 'store_key_number', 'item_total', 'inn', 
        'Почта ответственного менеджера',  'Наименование Товара', 'Артикул Ритейлера','price', 'quantity', 'sku_total']]
        df = df.drop_duplicates()

        df.columns = ['Номер заказа','Дата оформления', 'store_key_number' , 'Сумма товаров в заказе', 
        'ИНН', 'Почта ответственного менеджера',  'Наименование Товара', 'Артикул Ритейлера','Цена','Кол-во','Сумма по товару']

        for i in np.unique(df['Номер заказа'].values):
            pmdf = df[df['Номер заказа'] == i]

            PATH = f'Sbermarket_orders_{i}.xlsx'

            email = list(pmdf['Почта ответственного менеджера'].drop_duplicates().values)
            email = email + list(pmdf.merge(directors, on = 'store_key_number')['store_mail'].drop_duplicates().values)
            email = email + ['N.Stasevich@globus.ru','A.Kirzhaczkih@globus.ru', 'A.Soshin@globus.ru',
            'olga.a.orlova@sbermarket.ru','aleksandra.pekhoto@sbermarket.ru']

            email_to_text = str.replace(str.replace(str.replace(str.replace(str(email),'[',''),']',''),"'",""),',',';')
            store_key = pmdf['store_key_number'].drop_duplicates().values

            pmdf = pmdf.drop(columns = 'store_key_number')
            pmdf.to_excel(PATH, index=False)

            send_mail(to = email ,
                      subject = f"Увеличенный заказ {i} - Сбермаркет/Глобус",
                      text = f"""Заказ {i} Сбермаркет Глобус
                      Прошу согласовать отгрузку по указанным ценам.
                      Заказ считается согласованным только после получения ответа от ГМ. 
                      Контактный телефон для связи с ГМ: 88002348020
                      доб.№ ГМ: {store_key} 
                      внутренний телефон: 1300

                      При ответе на это письмо, добавляйте в копию {email_to_text}
                      """,
                      file_paths = [PATH]
                      )

        log_df = df[['Номер заказа']].drop_duplicates()
        log_df = log_df.rename(columns = {'Номер заказа':'shipment_number'})
        log_df['process_name'] = 'b2b_globus_sku_100k_pulse'
        log_df['shipment_logged_at'] = pd.datetime.now() + timedelta(hours = 3)
        ch_dataframe_to_clickhouse(log_df, "cdm.b2b__pulses_logs", ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID) 
    else:
        logging.info('No Data Received')
        
    

with DAG(

        dag_id='b2b_globus_1_sku_100k_pulse',
        description='отправка пульса по б2б заказам глобуса, если заказ больше чем на 200к, хотя бы 1 скю на 100к',
        schedule_interval='0/7 * * * *',
        start_date=datetime(2024, 5, 15, 10, 0, 0, 0),
        max_active_runs=1,
        tags=['b2b'],
        catchup=False

) as dag:

    send = PythonOperator(
        task_id='to_send',
        python_callable=send_mail_dag
    )

    send
