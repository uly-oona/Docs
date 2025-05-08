import pandas as pd
from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

from libs.helpers import task_fail_mattermost_alert #get_clickhouse_connection, 
# from libs.mailer import send_mail
# from libs import mattermost

from libs.airflow_connections import MATTERMOST_ALERTS_ANALYTICS_AIRFLOW__CONN_ID, CH_ANALYTICS__MARKETING_B2B__CONN_ID
from libs.dwh_ch import ch_pd_read_sql_query, ch_dataframe_to_clickhouse, DwhChEtlLogPushOperator

USER = 'ulyana.baislamova, ext-evgeniy.chelnokov, ilya.fedorov, konstantin.gerasimov, mariya.kurdyumova'

params = {
    'owner': USER,
    'email': ['ulyana.baislamova@sbermarket.ru', 'danyar.yusupov@sbermarket.ru', 'ext-evgeniy.chelnokov@sbermarket.ru', 'ilya.fedorov@sbermarket.ru', 'konstantin.gerasimov@sbermarket.ru', 'mariya.kurdyumova@sbermarket.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=1000),
    'on_failure_callback': partial(task_fail_mattermost_alert, http_conn_id=MATTERMOST_ALERTS_ANALYTICS_AIRFLOW__CONN_ID, user=USER)
}

# очистка stage витрины до
def clear_stage_table():
    sql = f"""
    TRUNCATE TABLE stage.dm__bi__b2b_shipment
    """
    ch_pd_read_sql_query(sql=sql, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

# очистка stage витрины после
def clear_stage_table_after():
    sql = f"""
    TRUNCATE TABLE stage.dm__bi__b2b_shipment
    """
    ch_pd_read_sql_query(sql=sql, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

# перенос из stage в prod витрину
def alter_into_prod_table():
    sql = f"""
    ALTER TABLE dm.bi__b2b_shipment REPLACE PARTITION tuple() FROM stage.dm__bi__b2b_shipment
    """
    ch_pd_read_sql_query(sql=sql, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)

# заполнение stage витрины
def stage_bi__b2b_shipment():

    sql = f"""   
INSERT INTO stage.dm__bi__b2b_shipment 

with 
    toDate('2021-01-01') as start_date,
    toDate(today()) as end_date

, cte_company_info as (
    select
        company_group_id
        , start_period
        , end_period 
        , manager_id
    from gp_rep.rep__group_companies_managers_month 
)

, cte_b2b_managers_info as (
    select distinct
        manager_id, 
        manager_email
    from analytics.b2b_managers_info
)

, cte_first_manager as (
    select  
        company_group_id, 
        argMin(manager_id, start_period) as first_manager_id
    from gp_rep.rep__group_companies_managers_month
    group by company_group_id
) 

-- текущий и первый менеджер компании
, cte_managers as (
    select
        mg.company_group_id as company_group_id
        , mg.start_period     as date_start
        , mg.end_period       as date_end
        , su.manager_id       as manager_id
        , su.manager_email    as manager_email
        , f.first_manager_id  as first_manager_id
    from cte_company_info as mg
    -- сотрудники
    inner join cte_b2b_managers_info as su
        on mg.manager_id = su.manager_id
    -- первый менеджер у компании
    inner join cte_first_manager as f
        on f.company_group_id = mg.company_group_id
)

-- -- компания, группа, менеджеры компании
, cte_gc as (
    select 
        e.employee_user_id as employee_user_id
        , e.company_group_id as company_group_id
    from gp_rep.rep__group_companies_employees  as e
)

-- -- заказы-сертификаты b2b
, cte_shipments_with_cert as (
    select distinct 
        toUInt32(shipment_id) as shipment_id
        -- , if(promo_type='b2b', 'b2b', 'b2c') as cert_type
    from gp_rep.rep__bi_adjustment
    where adjustment_type = 'gift certificate'
)

-- -- инфа по заказам b2b и по сертификатам (тк тоже b2b)
, cte_rep_bi_shipment_raw as (
    select 
    -- пользователь
        user_id
        , phone
        , prime_lvl
        
    -- заказ
        , order_id
        , order_number
        , shipment_id
        , shipment_number
        
    -- даты
        , shipped_at
        , multiIf(toDate(shipped_at, 'Europe/Moscow') is null
                , toDate(starts_at, 'Europe/Moscow')
                , toDate(shipped_at, 'Europe/Moscow')) as shipped_dt
        , starts_at
        , completed_at
        , created_at
        
    -- инфа про заказ
        , shipment_state
        , payment_method_name
        , company_name
        , company_inn
        , ship_address_id
        
    -- b2b
        , b2b_measure
        , b2b_order_company_flg
        , b2b_certificate_profit
        , b2b_certificates
        , bb_sert
        , goods_instamart
        
    -- деньги
        , gmv_goods
        , gmv_advertising
        , gmv_goods_net_promo
        , gmv_service_fee
        , gmv_service_fee_net_promo
        , service_proj_picking_fee
        , service_proj_delivery_fee
        
        , gross_profit
        , packing_costs
        , acquiring_costs
        , total as customer_total
        
    -- промо
        , goods_promo
        , goods_promo_total
        , service_promo_total
        , sberspasibo_promo
        , promo_total
        , promo_code
        , surge_sum
        
    -- каунты 
        , total_weight
        , item_count
        , replaced_items_cnt
        , canceled_items_cnt
        , total_quantity
        , total_cost
        
    -- инфа про магаз
        , type_delivery
        , tenant_id
        , api_client_id
        , city_id
        , city_name
        , retailer_id
        , retailer_name
        , store_id
        , store_name
        , platform
        , os
    from gp_rep.rep__bi_shipment
    where 1
        -- and toDate(report_dttm, 'Europe/Moscow') between start_date and end_date
        and toDate(coalesce(shipped_at,starts_at),'Europe/Moscow') between start_date and end_date -- чтобы совпадало с отчетом отмены
        and (b2b_measure >= 1 
            or shipment_id in (select shipment_id from cte_shipments_with_cert))
)

, cte_companies as (
    select 
        inn, 
        id,
        postpay_flg, 
        prepay_flg, 
        deposit_flg,
        created_at,
        lowerUTF8(arrayStringConcat(groupUniqArray(manager_comment))) as manager_comment_list
    from gp_rep.rep__companies
    where mt_src_cd = 'B2B_COMPANY'
    group by inn, id, postpay_flg, prepay_flg, deposit_flg, created_at
    ) 

-- дополнили инфой о компании и методе оплаты
, cte_rep_bi_shipment_raw_x_companies as (
    select 

      -- пользователь
        user_id
        , phone
        , prime_lvl
        
    -- заказ
        , order_id
        , order_number
        , shipment_id
        , shipment_number
        
    -- даты
        , shipped_at
        , shipped_dt
        , starts_at
        , completed_at
        , created_at
        
    -- инфа про заказ
        , shipment_state
        , payment_method_name
        , company_name
        , company_inn
        , ship_address_id
        
    -- b2b
        , b2b_measure
        , b2b_order_company_flg
        , b2b_certificate_profit
        , b2b_certificates
        , bb_sert
        , goods_instamart
        
    -- деньги
        , gmv_goods
        , gmv_advertising
        , gmv_goods_net_promo
        , gmv_service_fee
        , gmv_service_fee_net_promo
        , service_proj_picking_fee
        , service_proj_delivery_fee
        , gross_profit
        , packing_costs
        , acquiring_costs
        , customer_total
        
    -- промо
        , goods_promo
        , goods_promo_total
        , service_promo_total
        , sberspasibo_promo
        , promo_total
        , promo_code 
        , surge_sum
        
    -- каунты 
        , total_weight
        , item_count
        , replaced_items_cnt
        , canceled_items_cnt
        , total_quantity
        , total_cost
        
    -- инфа про магаз
        , type_delivery        
        , tenant_id
        , api_client_id
        , city_id
        , city_name
        , retailer_id
        , retailer_name
        , store_id
        , store_name
        , platform
        , os
        , multiIf(c.deposit_flg = 1, 'Депозит',
                    c.postpay_flg = 1, 'Постоплата',
                    c.prepay_flg = 1, 'Предоплата (Договор)',
                    'Предоплата (Оферта)'
            )    as payment_type
        , c.id   as company_id
    from cte_rep_bi_shipment_raw as r
    left join cte_companies as c
        on r.company_inn = c.inn
)

, cte_adjustment  as (
    select 
        shipment_id, 
        sum(amount) as amount -- Сумма скидки на уровне adjustment_type
    from gp_rep.rep__bi_adjustment 
    where adjustment_type in ('sberspasibo', 'sberloyalty') 
    group by shipment_id
)            

-- дополнили промо и группой компании. 
, cte_shipments_x_cg as (
    select 
    -- пользователь
        user_id
        , phone
        , prime_lvl
        
    -- заказ
        , order_id
        , order_number
        , s.shipment_id as shipment_id
        , shipment_number
        
    -- даты
        , shipped_at
        , shipped_dt
        , starts_at
        , completed_at
        , created_at
        
    -- инфа про заказ
        , shipment_state
        , payment_method_name
        , company_name
        , company_inn
        , ship_address_id
        
    -- b2b
        , b2b_measure
        , b2b_order_company_flg
        , b2b_certificate_profit
        , b2b_certificates
        , bb_sert
        , goods_instamart
        
    -- деньги
        , gmv_goods
        , gmv_advertising
        , gmv_goods_net_promo
        , gmv_service_fee
        , gmv_service_fee_net_promo
        , service_proj_picking_fee
        , service_proj_delivery_fee
        , gross_profit
        , packing_costs
        , acquiring_costs
        , customer_total
        
    -- промо
        , goods_promo
        , goods_promo_total
        , service_promo_total
        , sberspasibo_promo
        , promo_total
        , promo_code 
        , surge_sum
        
    -- каунты 
        , total_weight
        , item_count
        , replaced_items_cnt
        , canceled_items_cnt
        , total_quantity
        , total_cost
        
    -- инфа про магаз
        , type_delivery        
        , tenant_id
        , api_client_id
        , city_id
        , city_name
        , retailer_id
        , retailer_name
        , store_id
        , store_name
        , platform
        , os
        , payment_type
        , s.company_id as company_id
        , if(s.b2b_measure = 2, gc.company_group_id, 0)         as company_group_id
        , coalesce(sp.amount,0)                                 as sberspasibo_sberloyalty_promo
    
    --зачем тут такая странная логика для assigned_manager_id? тут же либо gcm.manager_id либо 0
        , multiIf((gcm.manager_id = gcm.first_manager_id) and
                        ((s.shipped_at >= (gcm.date_start - toIntervalDay(10))) and (s.shipped_at <= gcm.date_end))
                    , gcm.manager_id,
                        (s.shipped_at >= gcm.date_start) and (s.shipped_at <= gcm.date_end),
                      gcm.manager_id, 0
                )                                               as assigned_manager_id
                
    from cte_rep_bi_shipment_raw_x_companies  as s
    left join cte_gc                          as gc
        on gc.employee_user_id = s.user_id
    asof left join cte_managers               as gcm
        on gcm.company_group_id = gc.company_group_id
            and s.shipped_at <= gcm.date_end
    left join cte_adjustment as sp 
        on sp.shipment_id = s.shipment_id
    settings join_algorithm = 'hash'
)

, cte_company_sales_contracts as (
    select distinct
        company_id
    from analytics.int_company_sales_contracts
    where archived_at is null
) 

-- -- был ли контакт с продавцом
, cte_shipments_full as (
    select 
    -- пользователь
        user_id
        , phone
        , prime_lvl
        
    -- заказ
        , order_id
        , order_number
        , shipment_id
        , shipment_number
        
    -- даты
        , shipped_at
        , shipped_dt
        , starts_at
        , completed_at
        , created_at
        
    -- инфа про заказ
        , shipment_state
        , payment_method_name
        , company_name
        , company_inn
        , ship_address_id
        
    -- b2b
        , b2b_measure
        , b2b_order_company_flg
        , b2b_certificate_profit
        , b2b_certificates
        , bb_sert
        , goods_instamart
        
    -- деньги
        , gmv_goods
        , gmv_advertising
        , gmv_goods_net_promo
        , gmv_service_fee
        , gmv_service_fee_net_promo
        , service_proj_picking_fee
        , service_proj_delivery_fee
        , gross_profit
        , packing_costs
        , acquiring_costs
        , customer_total
        
    -- промо
        , goods_promo
        , goods_promo_total
        , service_promo_total
        , sberspasibo_promo
        , promo_total
        , promo_code
        , sberspasibo_sberloyalty_promo 
        , surge_sum
        
    -- каунты 
        , total_weight
        , item_count
        , replaced_items_cnt
        , canceled_items_cnt
        , total_quantity
        , total_cost
        
    -- инфа про магаз
        , type_delivery        
        , tenant_id
        , api_client_id
        , city_id
        , city_name
        , retailer_id
        , retailer_name
        , store_id
        , store_name
        
    -- тех часть заказа 
        , platform
        , os
        
    -- компания
        , payment_type
        , ships.company_id as company_id
        , company_group_id
        , assigned_manager_id
        , if(cs.company_id is null or cs.company_id = 0, 'Нет', 'Да') as has_contract
            --- promo_total_from_sh
    from cte_shipments_x_cg as ships
    left join cte_company_sales_contracts cs
    on cs.company_id = ships.company_id
)

, cte_b2b_managers_max_info  as (
    select distinct
        manager_id
        , manager_email
        , department
        , subdepartment
    from analytics.b2b_managers_info
    where manager_id != 0
)

-- -- Вспомогательная cte для cte_company_group_ids
, cte_shipments_for_existing_cg as (
    select 
        shipment_id
        , company_group_id
        , assigned_manager_id
    from cte_shipments_x_cg
    where 1 = 1
        and company_group_id != 0
        and company_group_id is not null
)

-- -- Чек/Группа компаний/Емейл менеджера/department менеджера/subdepartment менеджера
, cte_company_group_ids as (
    select 
        ships.shipment_id as shipment_id
        , ships.company_group_id as company_group_id
        , mng.manager_email as manager_email
        , mng.manager_id as manager_id
        , mng.department    as department
        , mng.subdepartment as subdepartment
    from cte_shipments_for_existing_cg as ships
    left join cte_b2b_managers_max_info as mng
    on mng.manager_id = ships.assigned_manager_id
)


-- -- дата первого заказа в компании
, cte_company_id_first_order as ( 
     select 
         company_id
         , min(toDate(shipped_at, 'Europe/Moscow')) as company_id_first_order_at
     from cte_rep_bi_shipment_raw_x_companies as shipments
     where 1 = 1
         and shipments.shipment_state = 'shipped'
     group by company_id
)

, cte_gp_rep__rep__group_companies as (
    select 
        company_group_id,
        company_id
    from gp_rep.rep__group_companies
)

, cte_first_company_in_group as (
    select 
        gc.company_group_id as company_group_id
        , fc.company_id_first_order_at as company_id_first_order_at
        , argMin(ic.id, created_at) as first_company_id
        , argMin(ic.inn, created_at) as first_company_inn
    from cte_gp_rep__rep__group_companies as gc  
    inner join cte_companies as ic
        on gc.company_id = ic.id
    left join cte_company_id_first_order as fc 
        on fc.company_id = gc.company_id
    group by gc.company_group_id, company_id_first_order_at
)


---***

-- ИНН/Доход/Издержки/Количество бранчей/СРМ Сегмент/Оквед/Основная деятельность/оквэд/Сотрудников/Оквеэд код/СРМ сегмент
-- Скорее всего внешняя информация о клиенте

, cte_dadata_info as (
    select distinct
        inn                as company_inn
        , finance_income
        , finance_expence
        , branch_count
        , o.category       as category_type
        , o.name           as name_okved
        , o.main_code_name as main_code_name
        , o.chapter_name   as okved_chapter_name
        , i.employee_count as employee_count
        , o.code           as okved_code
        , o.segment        as segment
        , o.subsegment     as subsegment
    from sandbox.cev_dadata_info as i
    left join sandbox.cev_okveds as o
        on o.code = i.okved_main
    left join sandbox.cev_regbank_region_city as r
        on r.code = substringUTF8(i.inn, 1, 2)
    )


, cte_company_employee_work_periods as (
    select 
        user_id,
        company_id,
        created_at as user_comp_created_at
    from gp_rep.rep__company_employee_work_periods
    -- where deleted_at is null
)

-- для каждого юзера определяем первую зарегистрированную компанию
, cte_company_employees as ( 
    select
        user_id,
        argMin(t1.id, user_comp_created_at ) as company_id,
        argMin(inn, user_comp_created_at) as company_inn,
        argMin(created_at, user_comp_created_at) as company_created_at,
        min(user_comp_created_at) as user_comp_created_at_
    from cte_companies as t1
    join cte_company_employee_work_periods as t2
        on t1.id = t2.company_id
    group by user_id
)

, cte_companies_sources as (
    select 
        company_inn,
        traffic_source_new,
        traffic_source_new_plus,
        utm_source_new,
        utm_medium_new,
        utm_term_new
    from sandbox.b2b_companies_sources as data
)

-- каждому юзеру приписываем источник
, cte_company_groups_utm_sources as (
    select
        user_id,
        ce.company_inn as company_inn,
        ce.company_id as company_id,
        data.traffic_source_new as traffic_source,
        data.traffic_source_new_plus as traffic_source_plus,
        data.utm_source_new as utm_source,
        concat(utm_source_new, '_',  utm_medium_new, '_', utm_term_new) as utm,
        if(
            utm_source_new in ('sber', 'sbbol_story') or
            traffic_source_new in ('sbbol', 'sbbol_landing', 'sber_lead'), 1, 0
            ) as company_sber_flg,
        if(tmp_ic.manager_comment_list like ('%%физик%%') or
           tmp_ic.manager_comment_list like ('%%фрод%%') or
           tmp_ic.manager_comment_list like ('%%тест%%') or
           tmp_ic.manager_comment_list like ('%%ликвид%%'), 1, 0
           ) as company_group_fraud_flg
    from cte_company_employees as ce
    left join cte_companies_sources as data
        on data.company_inn = ce.company_inn
    left join cte_companies as tmp_ic
    on ce.company_id = tmp_ic.id
)

, cte_bts as (
    select 
        shipment_id
        , picking_costs
        , drive_costs
        , taxi_costs
-- источник будет перенесен в гп реп (!?)
    from analytics.bi_order_payments
    where shipment_id in (select shipment_id from cte_rep_bi_shipment_raw)
)

, cte_b2b_phone_first_order as (
    select
        phone,
        min(toDate(shipped_at, 'Europe/Moscow'))                                               as first_order_dt,
        max(toDate(shipped_at, 'Europe/Moscow'))                                               as last_order_dt,
        argMin(shipment_id, shipped_at)                                                        as first_order_shipment_id_phone,
        arrayExists(
                    elem -> assumeNotNull( elem <= toDate(first_order_dt + INTERVAL 14 DAY) and elem > first_order_dt ),
                    groupArray( toDate(shipped_at, 'Europe/Moscow') )
                    )                                                                          as ret_14d,
        has(
            groupArray( assumeNotNull( toStartOfMonth( shipped_at ) ) ),
            assumeNotNull( toStartOfMonth(first_order_dt) ) + INTERVAL 1 MONTH
            )
                                                                                               as ret_2m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
            assumeNotNull( toStartOfMonth(first_order_dt) ) + INTERVAL 2 MONTH
            )                                                                                  as ret_3m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(first_order_dt) ) + INTERVAL 3 MONTH
           )                                                                                   as ret_4m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(first_order_dt) ) + INTERVAL 4 MONTH
           )                                                                                   as ret_5m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(first_order_dt) ) + INTERVAL 5 MONTH
           )                                                                                   as ret_6m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(first_order_dt) ) + INTERVAL 6 MONTH
           )                                                                                   as ret_7m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(first_order_dt) ) + INTERVAL 7 MONTH
           )                                                                                   as ret_8m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(first_order_dt) ) + INTERVAL 8 MONTH
           )                                                                                   as ret_9m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(first_order_dt) ) + INTERVAL 9 MONTH
           )                                                                                   as ret_10m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(first_order_dt) ) + INTERVAL 10 MONTH
           )                                                                                   as ret_11m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(first_order_dt) ) + INTERVAL 11 MONTH
           )                                                                                   as ret_12m,
        sum(goods + total_cost) as                                                        user_gmv,
        count(*) as                                                                            user_order_cnt,
        count(distinct toStartOfMonth(shipped_at, 'Europe/Moscow')) as                         user_active_months_count,
        greatest(dateDiff('month',toStartOfMonth(first_order_dt), toStartOfMonth(last_order_dt)),1) as user_age_month,
        user_order_cnt/user_active_months_count as                                             user_avg_orders_per_active_month,
        user_order_cnt/user_age_month as                                                       user_avg_orders_per_month,
        user_gmv/user_age_month as                                                             user_avg_arpu_per_month,
        user_gmv/user_order_cnt as                                                             user_aov
    from gp_rep.rep__bi_shipment as shipments
    where b2b_measure in (1,2)
          and shipment_state = 'shipped'
          and phone is not null
    group by phone
)

-- -- Вспомогательное cte для cte_b2b_shipments_filtered
, cte_shipped_b2b_orders as (
    select 
        shipment_id
        , shipped_at
        , starts_at
        , user_id
    from gp_rep.rep__bi_shipment
    where shipment_state = 'shipped'
        and b2b_measure = 2
)

, cte_b2b_shipments_filtered as (
    select 
        s.shipment_id
        , cte_gc.company_group_id as company_group_id
        , s.shipped_at
        , s.starts_at
    from cte_shipped_b2b_orders s
    left join cte_gc as gc
        on gc.employee_user_id = s.user_id
)

, cte_b2b_company_group_id_first_order as (
    select
        company_group_id,
        min(toDate(shipped_at, 'Europe/Moscow'))                                                              as company_group_first_order_dt,
        argMin(shipment_id, shipped_at)                                                                       as first_order_shipment_id_cg,
        arrayExists(
                    elem -> assumeNotNull(
                                            elem <= toDate(company_group_first_order_dt + INTERVAL 14 DAY)
                                            and
                                            elem > company_group_first_order_dt
                                         ),
                   groupArray(toDate(shipped_at, 'Europe/Moscow') ) )                                         as cg_ret_14d,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
            assumeNotNull( toStartOfMonth(company_group_first_order_dt) ) + INTERVAL 1 MONTH
            )                                                                                                 as cg_ret_2m,
        has(
            groupArray( assumeNotNull( toStartOfMonth( shipped_at ) ) ),
            assumeNotNull( toStartOfMonth( company_group_first_order_dt ) ) + INTERVAL 2 MONTH
            )                                                                                                 as cg_ret_3m,

        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(company_group_first_order_dt) ) + INTERVAL 3 MONTH
            )                                                                                                 as cg_ret_4m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(company_group_first_order_dt) ) + INTERVAL 4 MONTH
            )                                                                                                 as cg_ret_5m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(company_group_first_order_dt) ) + INTERVAL 5 MONTH
            )                                                                                                 as cg_ret_6m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(company_group_first_order_dt) ) + INTERVAL 6 MONTH
            )                                                                                                 as cg_ret_7m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(company_group_first_order_dt) ) + INTERVAL 7 MONTH
            )                                                                                                 as cg_ret_8m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(company_group_first_order_dt) ) + INTERVAL 8 MONTH
            )                                                                                                 as cg_ret_9m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(company_group_first_order_dt) ) + INTERVAL 9 MONTH
            )                                                                                                 as cg_ret_10m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(company_group_first_order_dt) ) + INTERVAL 10 MONTH
            )                                                                                                 as cg_ret_11m,
        has(
            groupArray( assumeNotNull( toStartOfMonth(shipped_at, 'Europe/Moscow') ) ),
           assumeNotNull( toStartOfMonth(company_group_first_order_dt) ) + INTERVAL 11 MONTH
            )                                                                                                 as cg_ret_12m
    from cte_b2b_shipments_filtered as b2b_shipments_filtered
    where company_group_id != 0
      and company_group_id is not null
    group by company_group_id
)

, cte_hvs as (
    select 
        sh_id, 
        1 as hvs, 
        type_of_initiative as hvs_promo
    from sandbox.b2b_hvs_shipments_view
)

-- Явно укаазаны типы полей по yml
, cte_final as (
    select
    -- пользователь
        toNullable(toUInt32(fm.user_id)) as user_id,
        toString(coalesce(fm.phone, ''))  as user_phone,
        if(first_order_shipment_id_phone = fm.shipment_id, 'new_b2b', 'repeated_b2b') as new_or_repeated_b2b,
        if(toDate(shipped_at, 'Europe/Moscow') = company_id_first_order_at,
                'new_b2b_company_id',
                'repeated_b2b_company_id'
            ) as new_or_repeated_b2b_company_id,
        if(first_order_shipment_id_cg = fm.shipment_id, 'new_b2b_company_group', 'repeated_b2b_company_group') as new_or_repeated_b2b_company_group,
        prime_lvl,
        
    -- компания
        toUInt32(fm.company_id) as company_id,
        toString(company_name),
        -- fm.company_inn as company_inn,
        toUInt32(company_sber_flg),
        toUInt8(company_group_fraud_flg),
        toNullable(toDate(first_order_dt)) as user_first_order_dt,
        toNullable(toDate(company_group_first_order_dt)),
        toNullable(toDate(company_id_first_order_at)),
        toUInt32(cte_company_group_ids.company_group_id) as company_group_id, 
        toUInt32(ship_address_id),
        toString(category_type) as company_category_type,
        toString(okved_chapter_name) as company_okved_chapter_name,
        toString(main_code_name) as company_main_code_name,
        toString(segment),
        toString(subsegment),
        toString(okved_code),
        toUInt32(hvs),
        toNullable(toString(hvs_promo)),
        
    -- менеджер
        toUInt32(manager_id),
        -- manager_email,
        department,
        subdepartment,
        
    -- траффик
        utm_source,
        utm,
        traffic_source,
        traffic_source_plus,
   
    -- заказ
        toUInt32(fm.order_id) as order_id,
        toString(fm.order_number) as order_number,
        toUInt32(fm.shipment_id) as shipment_id,
        toString(fm.shipment_number) as shipment_number,
   
    -- даты
        toNullable(toDateTime(created_at)),
        toNullable(toDateTime(completed_at)),
        toDate(shipped_dt),
        toNullable(toDateTime(shipped_at)),
        toString(shipment_state),

    -- инфа про заказ
        toString(type_delivery),
        toUInt16(city_id),
        toString(city_name),
        toUInt16(retailer_id),
        toString(retailer_name),
        toUInt32(store_id),
        toString(store_name),
        toString(platform),
        toString(os),
        toNullable(toString(payment_method_name)),
        
    -- b2b
        toUInt8(b2b_measure),
        toFloat32(b2b_certificates),
        toUInt8(fm.b2b_order_company_flg) as b2b_order_company_flg,
        toFloat32(b2b_certificate_profit),
        toUInt64(bb_sert),
        toFloat32(goods_instamart),
   
    -- деньги 
        toFloat32(gmv_goods),
        toFloat32(goods_promo),
        toFloat32(goods_promo_total),
        toFloat32(service_promo_total),
        toFloat32(service_promo_total + goods_promo_total) as promo_total_with_service,
        toFloat32(gmv_advertising),
        toFloat32(gmv_service_fee),
        toFloat32(gmv_goods_net_promo),
        toFloat32(gmv_service_fee_net_promo),
        toFloat32(if(b2b_measure = 2 and shipment_state = 'shipped', b2b_certificate_profit, 0)) as gmv_expired_certificates,
        toFloat32(gmv_goods_net_promo + gmv_service_fee_net_promo + bb_sert - gmv_expired_certificates) as gmv_sales_net_promo,
        toFloat32(packing_costs + picking_costs + acquiring_costs) as direct_fulfilment,
        toFloat32(if(shipment_state = 'shipped', gross_profit, 0) -
            (
                (picking_costs - if(shipment_state = 'shipped', service_proj_picking_fee, 0)) +
                (delivery_cost - if(shipment_state = 'shipped', service_proj_delivery_fee, 0)) +
                if(shipment_state = 'shipped', acquiring_costs, 0) +
                if(shipment_state = 'shipped', packing_costs, 0)
            ))
            as cp1,
        toFloat32(gross_profit),
        toFloat32(ifNull(cte_bts.picking_costs, 0)) as picking_costs,
        toFloat32(ifNull(cte_bts.drive_costs, 0)) as drive_costs,
        toFloat32(ifNull(cte_bts.taxi_costs, 0)) as taxi_costs,
        toFloat32(drive_costs + taxi_costs) as delivery_cost,
        toFloat32(acquiring_costs),
        toFloat32(packing_costs),
        toFloat32(user_avg_orders_per_active_month),
        toFloat32(user_avg_orders_per_month),
        toFloat32(user_avg_arpu_per_month),
        toFloat32(user_aov),

    -- промо
        toFloat32(sberspasibo_promo),
        toFloat32(promo_total),
        toNullable(toString(promo_code)),
        toUInt32(sberspasibo_sberloyalty_promo), 
        toUInt16(surge_sum),
        
    -- каунты 
        toUInt32(total_weight),
        toUInt16(item_count),
        toUInt8(replaced_items_cnt),
        toUInt8(canceled_items_cnt),
        toUInt32(total_quantity),
        toUInt32(total_cost),
        toFloat32(customer_total),
        
        multiIf(b2b_measure = 1, 'TICKS',
            b2b_measure = 4, 'B2B_CERT',
            b2b_measure = 3, 'B2B2B_METRO',
            e.category_type = '', 'NA', e.category_type = 'Government',
            'GOVERNMENT', e.category_type = 'Charity', 'CHARITY',
            (lowerUTF8(company_name) like '%%ип %%') or
            (lowerUTF8(company_name) like '%%индивидуальный предприниматель%%'),
            'INDIVIDUAL', e.finance_income is null, 'NO_FIN_DATA',
            (e.finance_income >= 2000000001 or e.employee_count >= 251),
            'LARGE', -- 2 MLRD OR 251 Employeers
            (e.finance_income between 800000001 and 2000000000 or
            e.employee_count between 101 and 250), 'MEDIUM',
            (e.finance_income between 120000001 and 800000000 or
            e.employee_count between 16 and 100), 'SMALL',
            (e.finance_income <= 120000000 or e.employee_count <= 15),
            'MICRO',
            'other'
        ) as company_type,

        
        fm.payment_type as payment_type,
        fm.has_contract as has_contract,
        (toString(tenant_id)), 
        
       case
             when tenant_id = 'smbusiness' and platform = 'web' and b2b_measure = 2 
                 then 'b2b web'
             when tenant_id = 'sbermarket' and platform = 'web' and b2b_measure = 2 and b2b_order_company_flg = 1
                 then 'для бизнеса с компанией b2c web'
             when tenant_id = 'sbermarket' and platform = 'web' and b2b_measure = 2 and b2b_order_company_flg = 0
                 then 'для себя с компанией b2c web'
             when tenant_id = 'sbermarket' and platform = 'web' and b2b_measure = 1 
                 then 'с галочкой b2c web'
             when b2b_measure = 3 
                 then 'сторонние заказы'
             when b2b_measure = 4 
                 then 'сертификаты'
             when b2b_measure = 0 
                 then 'использованные сертификаты b2c'
             when tenant_id in ('metro', 'selgros', 'lenta', 'okey') and platform = 'web' 
                 then 'white label'
             when platform = 'app' and b2b_measure = 2 and b2b_order_company_flg = 1
                 then 'для бизнеса с компанией app'
             when platform = 'app' and b2b_measure = 2 and b2b_order_company_flg = 0 
                 then 'для себя с компанией app'
             when platform = 'app' and b2b_measure = 1 
                 then 'с галочкой app'
             else 'иное'
        end as order_type,
        
        toUInt8(ret_14d),
        toUInt8(ret_2m),
        toUInt8(ret_3m),
        toUInt8(ret_4m),
        toUInt8(ret_5m),
        toUInt8(ret_6m),
        toUInt8(ret_7m),
        toUInt8(ret_8m),
        toUInt8(ret_9m),
        toUInt8(ret_10m),
        toUInt8(ret_11m),
        toUInt8(ret_12m),         
        toUInt8(cg_ret_14d),
        toUInt8(cg_ret_2m),
        toUInt8(cg_ret_3m),
        toUInt8(cg_ret_4m),
        toUInt8(cg_ret_5m),
        toUInt8(cg_ret_6m),
        toUInt8(cg_ret_7m),
        toUInt8(cg_ret_8m),
        toUInt8(cg_ret_9m),
        toUInt8(cg_ret_10m),
        toUInt8(cg_ret_11m),
        toUInt8(cg_ret_12m)
FROM  cte_shipments_full as fm

left join cte_b2b_phone_first_order
on cte_b2b_phone_first_order.phone = fm.phone

left join cte_company_group_ids
on fm.shipment_id = cte_company_group_ids.shipment_id

left join cte_b2b_company_group_id_first_order
on cte_b2b_company_group_id_first_order.company_group_id = cte_company_group_ids.company_group_id

left join cte_company_groups_utm_sources 
on cte_company_groups_utm_sources.user_id = fm.user_id

left join cte_first_company_in_group
on cte_first_company_in_group.company_group_id = cte_company_group_ids.company_group_id

left join cte_dadata_info as e
on cte_first_company_in_group.first_company_inn = e.company_inn

left join cte_bts
on cte_bts.shipment_id = fm.shipment_id

left join cte_hvs as  hvs
on hvs.sh_id = fm.shipment_id
)

select *
from cte_final
    """
    ch_pd_read_sql_query(sql=sql, ch_conn_id=CH_ANALYTICS__MARKETING_B2B__CONN_ID)


# настройки дага
with DAG(dag_id='bi__b2b_shipment',
         description='all b2b orders',
         default_args=params,
         schedule_interval='50 5 * * *',
         start_date=datetime(2024, 8, 20),
         tags=['b2b'],
         catchup=False,
         max_active_runs=1
         ) as dag:
    # очистка stage витрины
    clear_stage_table = PythonOperator(
        task_id='clear_stage_table',
        python_callable=clear_stage_table,
        dag=dag,
        op_kwargs={'ch_conn': CH_ANALYTICS__MARKETING_B2B__CONN_ID}
    )
    # заполнение stage витрины
    stage_bi__b2b_shipment = PythonOperator(
        task_id='stage_bi__b2b_shipment',
        python_callable=stage_bi__b2b_shipment,
        dag=dag,
        op_kwargs={'ch_conn': CH_ANALYTICS__MARKETING_B2B__CONN_ID}
    )
    # перенос из stage в prod витрину
    alter_into_prod_table = PythonOperator(
        task_id='alter_into_prod_table',
        python_callable=alter_into_prod_table,
        dag=dag,
        op_kwargs={'ch_conn': CH_ANALYTICS__MARKETING_B2B__CONN_ID}
    )
    # DwhChEtlLogPushOperator чтобы на основе этой витрины построить свою в отдельном даге
    bi__b2b_shipment__push_etl_log = DwhChEtlLogPushOperator(
        task_id="bi__b2b_shipment__push_etl_log",
        push_etl_log="dm.bi__b2b_shipment",
        dag=dag,
        ch_wait_replication_finish_tables="dm.bi__b2b_shipment",
    )
    # очистка stage витрины в конце
    clear_stage_table_after = PythonOperator(
        task_id='clear_stage_table_after',
        python_callable=clear_stage_table_after,
        dag=dag,
        op_kwargs={'ch_conn': CH_ANALYTICS__MARKETING_B2B__CONN_ID}
    )
clear_stage_table>>stage_bi__b2b_shipment>>alter_into_prod_table>>[bi__b2b_shipment__push_etl_log, clear_stage_table_after]
