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

with DAG(

        dag_id='b2b_komus_pulse_spb',
        description='b2b_komus_pulse_spb',
        schedule_interval='59 12 * * 1-5',
        start_date=datetime(2023, 3, 15),
        tags=['b2b'],
        catchup=False

) as dag:

    mysql_hook = MySqlHook(mysql_conn_id='mysql_instamart') 
    

    def SQL_komus_shipments():
        
        query = """ 
        with cities as (
        select 
            s.id store_id, 
            oz.name city_name 
        from stores s 
        join operational_zones oz on oz.id = s.operational_zone_id
        where retailer_id = 694
        and oz.name = 'Санкт-Петербург'
        )
        
        select
            shipment_number as "Номер заказа Сбермаркет",
            convert_tz(completed_at, 'UTC', 'Europe/Moscow') as "Время оформления",
            convert_tz(starts_at, 'UTC', 'Europe/Moscow') as "Время доставки",
            full_address as "Адрес доставки",
            quantity as "Количество товаров",
            coalesce(assembly_comment_order,assembly_comment) "комментарий к сборке заказа",
            special_instructions "инструкции к заказу",
            CONCAT(su.firstname,' ',su.lastname) "ФИО клиента",
            coalesce(su.pending_email, su.email) "почта клиента",
            CONCAT('+',phone) "телефон клиента",
            offer_id,
            company_id,
            pay_flg
            
        from  
            (
                select distinct
                    order_number
                    ,shipment_number
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
                    ,user_id
                    ,phone
                from
                    (
                    select 
                        so.number as order_number,
                        sh.number as shipment_number,
                        so.payment_state payment_state,
                        so.id as order_id,
                        so.completed_at,
                        sh.assembly_comment,
                        dw.starts_at,
                        sa.full_address,
                        sa.phone phone,
                        sli.offer_id,
                        sli.quantity,
                        so.user_id user_id,
                        so.special_instructions special_instructions,
                        so.assembly_comment assembly_comment_order,
                        sh.store_id store_id,
                        pp.payment_method_id payment_method_id
                        ,case when (so.payment_state in ('paid','overpaid') ) or (pp.payment_method_id in (4,11,12,20)) or (so.payment_state = 'balance_due' and bd.id <> 0) 
                              then 1 
                              else 0 end as pay_flg
                        
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
                    join 
                        cities c on c.store_id = sh.store_id
                    left join 
                        spree_payments pp on pp.order_id = so.id
                    -- left join 
                    --     (select distinct co.order_id from companies_orders co join companies c on c.id = co.company_id and (c.postpay or c.deposit) and not c.contract_suspended ) co on co.order_id = so.id
                    left join 
                        (
                        select 
                            o.id id,
                            sum(s.payment_total) summ_paid,
                            count(distinct s.payment_total) cnt_pay,
                            count(s.payment_total) cnt_ords,
                            sum(s.total) summ_total
                            ,if(count(distinct s.payment_total) = 1, sum(s.payment_total)/count(s.payment_total),sum(s.payment_total))summ_paid_t
                        from spree_orders o
                        join spree_shipments s on s.order_id = o.id
                        where 1 
                            and o.state = 'complete'
                            and o.id in (select o.id from spree_orders o join spree_shipments s on s.order_id = o.id where s.retailer_id = 694 and o.payment_state = 'balance_due')
                            and o.payment_state = 'balance_due'
                        group by 1
                        having summ_total - summ_paid_t <= 1000
                        ) bd on bd.id = so.id
                    where 1=1
                        and case when WEEKDAY(DATE(now())) = 4 then DATE(dw.starts_at) = DATE(now() + interval 3 day) else DATE(dw.starts_at) = DATE(now() + interval 1 day) end
                        and sh.retailer_id = 694
                        and so.state in ('complete', 'resumed')
                        -- and (so.payment_state in ('paid','overpaid') or co.order_id <> 0 or pp.payment_method_id in (4,11,12,20) or (so.payment_state = 'balance_due' and bd.id <> 0))
                        and so.id not in (select distinct order_id from b2b_unpaid_orders where export_lock_state = 'locked')
                    ) komus_shipments
                left join companies_orders co on komus_shipments.order_id = co.order_id
            ) res
            left join spree_users su on su.id = res.user_id
        order by convert_tz(starts_at, 'UTC', 'Europe/Moscow') ,shipment_number   
                
        """

        tbl = mysql_hook.get_pandas_df(query)

        return tbl


    def paper_orders_contact():
        query = """

        with cities as (
        select 
            s.id store_id, 
            oz.name city_name 
        from stores s 
        join operational_zones oz on oz.id = s.operational_zone_id
        where retailer_id = 694
        and oz.name = 'Санкт-Петербург'
        )

            select
                    shipment_number as "Номер заказа",
                    full_address "Адрес доставки",
                    user_fio "Контактное лицо",
                    user_email "Почта",
                    phone "Телефон",
                    special_instructions "инструкции к заказу",
                    assembly_comment "комментарий к сборке заказа",
                    offer_id,
                    company_id,
                    pay_flg
            from
                (
                    select
                        komus_shipments.*,
                        co.company_id,
                        CONCAT(su.firstname,' ',su.lastname) user_fio,
                        coalesce(su.pending_email, su.email) user_email,
                        pap.offer_id offer_id
                    from
                        (
                        select 
                            so.number as order_number,
                            sh.number as shipment_number,
                            so.payment_state,
                            so.id as order_id,
                            so.completed_at,
                            dw.starts_at,
                            sa.full_address,
                            sa.phone phone,
                            so.user_id,
                            so.special_instructions,
                            coalesce(so.assembly_comment,sh.assembly_comment) assembly_comment,
                            sh.store_id store_id
                            , case when (so.payment_state in ('paid','overpaid') ) or (pp.payment_method_id in (4,11,12,20)) or (so.payment_state = 'balance_due' and bd.id <> 0) 
                              then 1 
                              else 0 end as pay_flg
                        from 
                            spree_shipments sh
                        join
                            spree_orders so on sh.order_id = so.id
                        join 
                            delivery_windows dw on dw.id = sh.delivery_window_id
                        join 
                            spree_addresses sa on sa.id = so.ship_address_id
                        join 
                            cities c on c.store_id = sh.store_id
                        left join 
                            spree_payments pp on pp.order_id = so.id
                        -- left join 
                        --     (select distinct co.order_id from companies_orders co join companies c on c.id = co.company_id and (c.postpay or c.deposit) and not c.contract_suspended) co on co.order_id = so.id
                        left join 
                            (
                            select 
                                o.id id,
                                sum(s.payment_total) summ_paid,
                                count(distinct s.payment_total) cnt_pay,
                                count(s.payment_total) cnt_ords,
                                sum(s.total) summ_total
                                ,if(count(distinct s.payment_total) = 1, sum(s.payment_total)/count(s.payment_total),sum(s.payment_total))summ_paid_t
                            from spree_orders o
                            join spree_shipments s on s.order_id = o.id
                            where 1 
                                and o.state = 'complete'
                                and o.id in (select o.id from spree_orders o join spree_shipments s on s.order_id = o.id where s.retailer_id = 694 and o.payment_state = 'balance_due')
                                and o.payment_state = 'balance_due'
                            group by 1
                            having summ_total - summ_paid_t <= 1000
                            ) bd on bd.id = so.id
                        where 1=1
                            and case when WEEKDAY(DATE(now())) = 4 then DATE(dw.starts_at) = DATE(now() + interval 3 day) else DATE(dw.starts_at) = DATE(now() + interval 1 day) end
                            and sh.retailer_id = 694
                            and so.state in ('complete', 'resumed')
                            -- and (so.payment_state in ('paid','overpaid') or pp.payment_method_id in (4,11,12,20) or (so.payment_state = 'balance_due' and bd.id <> 0))
                            and so.id not in (select distinct order_id from b2b_unpaid_orders where export_lock_state = 'locked')
                        ) komus_shipments
                    join (
                         select order_id, offer_id,quantity
                         from spree_line_items
                         where 1
                         and deleted_at is null 
                        ) pap on pap.order_id = komus_shipments.order_id
                    left join companies_orders co on komus_shipments.order_id = co.order_id
                    left join companies c on c.id = co.company_id
                    left join spree_users su on su.id = komus_shipments.user_id
                    where 1 
                    and pap.quantity >= 240
                    order by inn, order_number 
                ) a       
        """

        tbl = mysql_hook.get_pandas_df(query)
        
        return tbl


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
                and retailer_id = 694
            group by offer_id
                ), 
                
            x_products as (
            select 
                product_sku,
                master_category_id 
            from ods.product_hub__product 
            where status = 1 
            and deleted_at is null 
            ),

            x_offers as (
                select
                    distinct 
                    id,
                    name as "Наименование Товара",
                    sku,
                    retailer_sku as "Артикул Ритейлера",
                    store_id,
                    retailer_id
                from ods.product_hub__offer_x_stock o
                where 1
                    and deleted_at is null 
                    -- and published = 1
                    and status = 1
                    and retailer_id = 694
                    )

            select 
                x_offers.id offer_id,
                x_offers."Наименование Товара" "Наименование Товара",
                x_offers."Артикул Ритейлера" "Артикул Ритейлера",
                x_products.master_category_id master_category_id
            from x_products 
                join x_offers on x_products.product_sku = x_offers.sku
                left join x_prices on x_prices.offer_id = x_offers.id
            where 1 

            """
        return ch_pd_read_sql_query(SQL, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)


    def send_mail_dag():

        df1 = SQL_komus_shipments()
        df3 = paper_orders_contact()
        managers_df = SQL_managers()
        producthub_df = SQL_producthub()
        papers_offers = producthub_df[producthub_df.master_category_id == 3200010001]
        papers_offers = papers_offers[['offer_id','master_category_id']]

        df1.company_id = df1.company_id.astype('str')
        managers_df.company_id = managers_df.company_id.astype('str')

        df1 = df1.merge(managers_df,how = 'left',on = 'company_id')

        df1 = df1[(df1.pay_flg==1) | (df1.con_flag=='postpay')]

        df1["Почта ответственного менеджера"] = df1["Почта ответственного менеджера"].fillna('zabotab2b@sbermarket.ru')
        df1["Телефон ответственного менеджера"] = df1["Телефон ответственного менеджера"].fillna('78005509411')
        df1["ФИО ответственного менеджера"] = df1["ФИО ответственного менеджера"].fillna('Группа поддержки b2b')

        df1 = df1.merge(producthub_df,on = 'offer_id')
        df1 = df1.drop(columns = ['offer_id','company_id','master_category_id'])
        df1 = df1.drop_duplicates()


        df3 = df3.merge(papers_offers,on = 'offer_id')

        df3 = df3.merge(managers_df[["company_id","ИНН","КПП","Название компании","con_flag"]],on = 'company_id')

        df3 = df3[(df3.pay_flg==1) | (df3.con_flag=='postpay')]

        df3 = df3.drop(columns = ['offer_id','master_category_id','company_id','con_flag','pay_flg'])
        df3 = df3.drop_duplicates()


        curdt = date.today().strftime("%d_%m_%Y")

        PATH1 = f'b2b_Комус_СберМаркет_{curdt}.xlsx'
        PATH3 = f'Грузополучатели_по_заказам_с_бумагой_{curdt}.xlsx'

        df1.to_excel(PATH1, index=False)
        df3.to_excel(PATH3, index=False)

        email = ['moo42@optovy.komus.net','grigoriy.zorin@sbermarket.ru', 'aleksandra.pekhoto@sbermarket.ru',
         'klsvt@tvr.komus.net','baa1v@bony.komus.net','sif83@optovy.komus.net'] 


        if (len(df1) > 0) & (len(df3) > 0):
            send_mail(to=email,
            subject="(Санкт-Петербург) B2B заказы Комус Сбермаркет",
            text="Информация по b2b заказам в Комусе через Сбермаркет г. Санкт-Петербург",
            file_paths=[PATH1,PATH3])
        elif (len(df1) > 0):
            send_mail(to=email,
            subject="(Санкт-Петербург) B2B заказы Комус Сбермаркет",
            text="Информация по b2b заказам в Комусе через Сбермаркет г. Санкт-Петербург",
            file_paths=[PATH1])
        else:
            logging.info('No Data Received')


    send = PythonOperator(
        task_id='to_send',
        python_callable=send_mail_dag
    )

    send