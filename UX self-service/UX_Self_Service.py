#!/usr/bin/env python
# coding: utf-8

# # Self-Service для UX-выгрузок
# 
# <p style="font-size: 18px;">Привет! Это инструмент, который поможет тебе сделать выгрузку пользователей с нужными фильтрами. </p>
#     
# Подробное описание (запуск, настройка фильтров, описание фильтров) [находится здесь](https://wiki.sbmt.io/pages/viewpage.action?pageId=3153553163). **Его обязательно нужно прочитать, иначе у вас может ничего не работать**

# ### Как запустить?
# 
# **1.** Нажимай на запуск и жди пока появятся фильтры:
# 
# <img style="float: left;" src="start_pic.png" width="450"/>

# **2.** Нажимай "Перезапуск и запуск всех ячеек":
# 
# <img style="float: left;" src="restart.png" width="450"/>

# **3.** Выбирай фильтры:
# 
# <img style="float: left;" src="filters.png" width="450"/>

# **4.** Нажимай "Начать расчет"

# **5.** Подожди несколько минут (до 10) и нажимай на кнопку "Скачать файл"

# **6.** Если нужно сбросить какие-либо фильтры или установить новые, нужно повторить все с 1 пункта
# 
# Если вдруг что-то сломалось и расчеты не производятся, попробуй снова все повторить с 1 пункта

# ⚠️ **Никогда не нажимаем "Перезапись"**, если выскочило такое окошко. Либо просто закрываем, либо перезапускаем
# 
# <img style="float: left;" src="rewrite.png" width="450"/>

# In[1]:


import warnings
warnings.filterwarnings("ignore")
from IPython.display import HTML

# HTML('''
<script>
code_show=true; 
function code_toggle() {
 if (code_show){
 $('div.input').hide();
 } else {
 $('div.input').show();
 }
 code_show = !code_show
} 
$( document ).ready(code_toggle);
</script>
<form action="javascript:code_toggle()"><input type="submit" value="Скрыть/показать код расчета"></form>
''')
# In[2]:


get_ipython().run_cell_magic('capture', '', '!pip install -q panel==0.12.6')


# In[3]:


get_ipython().run_cell_magic('capture', '', '!pip install bokeh==2.4.0')


# In[4]:


get_ipython().run_cell_magic('capture', '', '!pip install panel --upgrade')


# In[5]:


get_ipython().run_cell_magic('capture', '', '!pip install --upgrade numpy==1.26.4\n\n#https://stackoverflow.com/questions/78634235/numpy-dtype-size-changed-may-indicate-binary-incompatibility-expected-96-from')


# In[6]:


import ipywidgets as widgets
from IPython.display import display
from ipywidgets import TwoByTwoLayout
from ipywidgets import Button, HBox, VBox, Checkbox, Layout, Label, Text, Output, DatePicker, Dropdown, SelectMultiple
from IPython.display import Markdown
import panel as pn
from io import StringIO
from multiprocessing import Process, Pipe
pn.extension()
from datetime import datetime, date, time, timedelta
from ast import literal_eval
from pandas.io.json import json_normalize 
import pandas as pd
import numpy as np

import sqlalchemy as sa
from sqlalchemy.pool import NullPool
from dotenv import dotenv_values
from tqdm import tqdm


# In[7]:


config = dotenv_values("/home/jovyan/shared/Customer/.env") 


# In[8]:


ch_host = config['ch_host']
ch_cert = config['ch_cert']
ch_port = config['ch_port']
ch_db   = config['ch_db']
ch_user = config['ch_user']
ch_pass = config['ch_pass']

engine = sa.create_engine(f'clickhouse+native://{ch_user}:{ch_pass}@{ch_host}:{ch_port}/{ch_db}?secure=True&ca_certs={ch_cert}')


# In[9]:


def set_conditions():
    
    # platform
    if 'Все платформы' in platform.value or len(platform.value) == 0:
        platform_condition = "and 1=1"
    else:
        platform_condition = f"""and lower(platform) in ({platform.value})"""
    
    # shipping_method_kind
    if 'Не важно' in shipping_method_kind.value or len(shipping_method_kind.value) == 0:
        shipping_method_kind_condition = "and 1=1"
    else:
        shipping_method_kind_condition = f"""and hasAny(shipping_method_kinds, ['{"','".join(shipping_method_kind.value)}'])"""
    
    # os
    if 'Все ОС' in os.value or len(os.value) == 0:
        os_condition = "and 1=1"
    else:
        os_condition = f"""and lower(os) in ({os.value})"""
    
    # device_type
    if 'Все типы' in device_type.value or len(device_type.value) == 0:
        device_type_condition = "and 1=1"
    else:
        device_type_condition = f"""and device_type in ({device_type.value})"""
        
    # retailer_name
    if retailer_name.value == '' or len(retailer_name.value) == 0:
        retailer_name_condition = "and 1=1"
    else:
        retailer_name_condition = f"""and hasAny(retailer_names, ['{"','".join(retailer_name.value.split(', '))}'])"""
        
    # type_delivery
    if 'Не важно' in type_delivery.value or len(type_delivery.value) == 0:
        type_delivery_condition = "and 1=1"
    else:
        type_delivery_condition = f"""and type_delivery in ({type_delivery.value})"""
        
    # prime_lvl
    if 'Не важно' in prime_lvl.value or len(prime_lvl.value) == 0:
        prime_lvl_condition = "and 1=1"
    else:
        prime_lvl_condition = f"""and prime_lvl in ({prime_lvl.value})"""
        
    # retailer_category_name
    if 'Не важно' in retailer_category_name.value or len(retailer_category_name.value) == 0:
        retailer_category_name_condition = "and 1=1"
    else:
        retailer_category_name_condition = f"""and hasAny(retailer_category_names, ['{"','".join(retailer_category_name.value)}'])"""
        
    # city
    if city.value == '' or len(city.value) == 0:
        city_condition = "and 1=1"
    else:
        city_condition = f"""and hasAny(order_cities, ['{"','".join(city.value.split(', '))}'])"""
    
    # cancellations
    if 'Не важно' in cancellations.value or len(cancellations.value) == 0:
        cancellations_condition = "and 1=1"
    else:
        if cancellations.value == "Есть отмены":
            cancellations_condition = f"""and has(shipment_states, 'canceled') = 1"""
        if cancellations.value == "Нет отмен":
            cancellations_condition = f"""and has(shipment_states, 'canceled') = 0"""
    
    # sberid_use
    if 'Не важно' in sberid.value or len(sberid.value) == 0:
        sberid_condition = "and 1=1"
    else:
        sberid_condition = f"""and sberid_use in ({sberid.value})"""
    
    # min_orders_period
    min_orders_period_condition = f"and orders_period >= {min_orders_period.value}" if min_orders_period.value !=  '' else "and 1=1"
    
    # min_addresses_period
    min_addresses_period_condition = f"and address_count >= {min_addresses_period.value}" if min_addresses_period.value !=  '' else "and 1=1"
    
    # spasibo
    if 'Не важно' in spasibo.value or len(spasibo.value) == 0:
        spasibo_condition = "and 1=1"
    else:
        spasibo_condition = f"""and spasibo_charged in ({spasibo.value})"""
        
    # bnpl_used
    if 'Не важно' in bnpl.value or len(bnpl.value) == 0:
        bnpl_condition = "and 1=1"
    else:
        bnpl_condition = f"""and bnpl_used in ({bnpl.value})"""
    
    # new_or_old
    if 'Не важно' in new_or_old.value or len(new_or_old.value) == 0:
        new_or_old_condition = "and 1=1"
    else:
        new_or_old_condition = f"""and new_or_old in ({new_or_old.value})"""
        
    # use_interval
    if 'Не важно' in use_duration.value or len(use_duration.value) == 0:
        use_duration_condition = "and 1=1"
    else:
        use_duration_condition = f"""and use_interval in ({use_duration.value})"""
    
    # b2b
    b2b_condition = "and b2b_measure = 0" if exclude_b2b.value == True else "and 1=1"  
    # only b2b
    only_b2b_condition = "and b2b_measure > 0" if only_b2b.value == True else "and 1=1"  
    
    # nonfood
    nonfood_condition = "and orders_nonfood_category > 0" if must_buy_nonfood.value == True else "and 1=1"
    
    # food
    food_condition = "and orders_food_category > 0" if must_buy_food.value == True else "and 1=1"
    
    # min orders per month
    month_orders_condition = f"and min_month_orders >= {month_orders.value}" if month_orders.value !=  '' else "and 1=1"
    
    return(platform_condition,
           shipping_method_kind_condition,
           os_condition,
           device_type_condition,
           retailer_name_condition,
           type_delivery_condition,
           prime_lvl_condition,
           retailer_category_name_condition,
           city_condition,
           cancellations_condition,
           sberid_condition,
           min_orders_period_condition,
           min_addresses_period_condition,
           spasibo_condition,
           bnpl_condition,
           new_or_old_condition,
           use_duration_condition,
           b2b_condition,
           only_b2b_condition,
           nonfood_condition,
           food_condition,
           month_orders_condition)


# In[10]:


def calculate_report(self):
    
    start_time = datetime.now()
    
    platform_condition, shipping_method_kind_condition, os_condition, device_type_condition, retailer_name_condition, type_delivery_condition, prime_lvl_condition, retailer_category_name_condition, city_condition, cancellations_condition, sberid_condition, min_orders_period_condition, min_addresses_period_condition, spasibo_condition, bnpl_condition, new_or_old_condition, use_duration_condition, b2b_condition, only_b2b_condition, nonfood_condition, food_condition, month_orders_condition = set_conditions()
    indicator.value = 10
    
    q = f"""

        with

        have_sberid_users as (
            select
                toString(user_id) as user_id,
                maxIf(1, provider = 'sberid_plan_moscow') as sberid_plan_moscow, -- есть sberID, но нет привязки к СМ
                maxIf(1, provider = 'sberbank') as sberbank -- есть sberID, есть привязка к СМ
            from analytics.int_spree_user_authentications auth
            group by user_id
        )

        select user_id,
               case when sberid_plan_moscow = 0 and sberbank = 0 then 'Не используют sberID'
                    when sberid_plan_moscow = 1 and sberbank = 0 then 'Используют sberID, но не привязали его к СМ'
                    when sberbank = 1 then 'Используют sberID, привязали его к СМ'
               end as sberid_use
        from have_sberid_users

    """

    sberid_using = pd.read_sql(q, con=engine)
    indicator.value = 20
    
    q = f"""
        
        with
        base_for_order_seq as (
            select user_id as user_uuid,
                   order_seq,
                   backend_order_completed_at
            from analytics.web_funnel
            where backend_order_completed_at is not null
                and backend_order_completed_at between now() - interval '1 year' and now()
            UNION ALL 
            select context_traits_backend_user_uuid as user_uuid,
                   order_seq,
                   backend_order_completed_at
            from analytics.new_app_funnel_table
            where backend_order_completed_at is not null
                and backend_order_completed_at between now() - interval '1 year' and now()

        ),
        order_seq_x_using_duration as (
            select user_uuid, 
                   case when max(order_seq) > 7 then '>7 заказов'
                        else '1-7 заказов'
                   end as new_or_old,
                   toMonth( toDate(max(backend_order_completed_at) - min(backend_order_completed_at)) ) as diff
            from base_for_order_seq
            group by user_uuid
        )
        select user_uuid,
               new_or_old,
               case when diff <= '1' then 'Меньше месяца'
                    when diff between '1' and '3' then '1-3 месяцев'
                    when diff between '3' and '6' then '3-6 месяцев'
                    when diff between '6' and '12' then '6-12 месяцев'
               end as use_duration
        from order_seq_x_using_duration

    """

    funnel_table = pd.read_sql(q, con=engine)
    indicator.value = 30
    
    q = f"""
        
        with
        
        toDate('{datetime.strftime(period_start.value, format="%Y-%m-%d")}') as start_date, 
        toDate('{datetime.strftime(period_end.value, format="%Y-%m-%d")}') as end_date,

        category_orders as (
            select order_id,
                max(arrayReverse(dictGetHierarchy('dict.ods__product_hub__master_category',toUInt64(assumeNotNull(master_category_id))))[1] not in (1420,100000)) as has_nonfood,
                max(arrayReverse(dictGetHierarchy('dict.ods__product_hub__master_category',toUInt64(assumeNotNull(master_category_id))))[1] in (1420,100000)) as has_food      
            from analytics.line_items 
            where shipment_shipped_at is not null
            and toDate(shipment_shipped_at) between start_date and end_date
            group by order_id
       ),
       
        dogfooding_users as (
        select toString(uuid) as user_uuid
        from cdm.ab__dogfooding
        )
       
        select toString(user_id) as user_id, 
            toString(dictGet('analytics.spree_users_dict', 'uuid', toUInt64(user_id))) as user_uuid,
            dictGet('analytics.spree_users_dict', 'firstname', toUInt64(user_id)) as first_name,
            dictGet('analytics.spree_users_dict', 'lastname', toUInt64(user_id)) as last_name,
            argMin(first_paid_at, shipped_at) as first_paid_at,
            platform,
            os,
            device_type,
            uniq(order_number) as orders_period,
            uniq(concat(toString(address_latitude), toString(address_longitude))) as address_count,
            argMax(city_name, shipped_at) as last_order_city,
            max(shipped_at) as last_order_ts,
            maxIf(1, b2b=1) as is_b2b,
            groupArray(city_name) as order_cities,
            groupArray(shipment_state) as shipment_states,
            groupArray(shipping_method_kind) as shipping_method_kinds,
            groupArray(lower(retailer_name)) as retailer_names,
            groupArray(type_delivery) as types_delivery,
            argMax(prime_lvl, shipped_at) as prime_lvl,
            groupArray(retailer_category_name) as retailer_category_names,
            groupArray(
                        case when retailer_size = 'supermarket' then 'Супермаркеты'
                            when retailer_size = 'hypermarket' then 'Гипермаркеты'
                            else 'Другие'
                        end as retailer_size
                       ) as retailer_sizes,
            case when maxIf(1, loyalty_charged_spasibo > 0 or charged_spasibo > 0) = 1 then 'Списывают'
                 else 'Не списывают'
            end as spasibo_charged,
            case when maxIf(1, payment_method_name = 'Частями') = 1 then 'Используют' 
                 else 'Не используют'
            end as bnpl_used,
            uniqIf(order_id, order_id in (select order_id from category_orders where has_nonfood=1)) as orders_nonfood_category,
            uniqIf(order_id, order_id in (select order_id from category_orders where has_food=1)) as orders_food_category

        from analytics.bi_shipments_financial

        where toDate(created_at) between start_date and end_date
            {b2b_condition}
            {only_b2b_condition}
            and tenant_id = 'sbermarket'
            and user_id is not null
            and user_uuid not in (select user_uuid from dogfooding_users)
            {platform_condition}
            {os_condition}
            {device_type_condition}

        group by user_id, platform, device_type, os
        having 1=1
        {shipping_method_kind_condition}
        {type_delivery_condition}
        {retailer_category_name_condition}
        {retailer_name_condition}
        {prime_lvl_condition}
        {nonfood_condition}
        {food_condition}
        {city_condition}
        {cancellations_condition}
        {min_orders_period_condition}
        {min_addresses_period_condition}
        {spasibo_condition}
        {bnpl_condition}

    """

    shipments_table = pd.read_sql(q, con=engine)
    indicator.value = 40
    
    q = f"""
        
        with
        
        toDate('{datetime.strftime(period_start.value, format="%Y-%m-%d")}') as start_date, 
        toDate('{datetime.strftime(period_end.value, format="%Y-%m-%d")}') as end_date,

        month_data as (
            select toString(user_id) as user_id, toMonth(shipped_at) as month, uniq(order_id) as month_orders
            from gp_rep.rep__bi_shipment
            where toDate(shipped_at) between start_date and end_date
            and user_id <> ''
            and toDate(report_dttm) between start_date and end_date
            group by month, user_id
        )

        select user_id, min(month_orders) as min_month_orders 
        from month_data
        group by user_id
        having 1=1
            {month_orders_condition}
    """

    month_orders = pd.read_sql(q, con=engine)
    indicator.value = 45
    
    q = f"""
        select toString(user_id) as user_id, argMax(value, created_at) as phone_number  
        from analytics.int_spree_phones
        group by user_id 
    """
    
    phones = pd.read_sql(q, con=engine)
    indicator.value = 50
    
    q = f"""
        select toString(uuid) as user_uuid, 
               case when argMax(email, created_at) not like '%%@temp.temp%%' 
                    then argMax(email, created_at) 
                    else argMax(pending_email, created_at) 
                    end as user_email
        from analytics.int_spree_users
        where 1=1
            and email not like '%%sberbank%%'
            and email not like '%%sbermarket%%'
            and email not like '%%instamart%%'
            and email not like '%%kuper%%'
            and pending_email not like '%%sberbank%%'
            and pending_email not like '%%sbermarket%%'
            and pending_email not like '%%instamart%%'
            and pending_email not like '%%kuper%%'
        group by user_uuid
    """
    
    emails = pd.read_sql(q, con=engine)
    indicator.value = 55
    
    q = f"""
        select toString(user_id) as user_id, argMax(name, update_date) as segment 
        from prod_marketing.user_meta_segment
        group by user_id
    """
    
    segments = pd.read_sql(q, con=engine)
    indicator.value = 60
    
    ## ------ final filters ------ ###
    
    # new_or_old
    if 'Не важно' in new_or_old.value or len(new_or_old.value) == 0:
        pass
    else:
        funnel_table = funnel_table[funnel_table.new_or_old.isin(new_or_old.value)]
        
    # sberid
    if 'Не важно' in sberid.value or len(sberid.value) == 0:
        pass
    else:
        sberid_using = sberid_using[sberid_using.sberid_use.isin(sberid.value)]
    
    # use duration
    if 'Не важно' in use_duration.value or len(use_duration.value) == 0:
        pass
    else:
        funnel_table = funnel_table[funnel_table.use_duration.isin(use_duration.value)]
    indicator.value = 70
    
    ### ------ merge ------ ###
    total = shipments_table.merge(funnel_table, how='inner', left_on='user_uuid', right_on='user_uuid')
    total = total.merge(sberid_using, how='inner', left_on='user_id', right_on='user_id')
    total = total.merge(phones, how='inner', left_on='user_id', right_on='user_id')
    total = total.merge(emails, how='inner', left_on='user_uuid', right_on='user_uuid')
    total = total.merge(month_orders, how='inner', left_on='user_id', right_on='user_id')
    total = total.merge(segments, how='left', left_on='user_id', right_on='user_id')
    
    print(f'df length = {len(total)}')
    
    indicator.value = 80
    
    # major_shop_size
    
    def check_percentage(lst):
        count_abc = lst.count(value)
        total_count = len(lst)
        percentage = (count_abc / total_count) * 100
        return percentage >= 50

    if 'Не важно' in major_shop_size.value or len(major_shop_size.value) == 0:
        pass
    else:
        value = major_shop_size.value[0]
        total = total[total['retailer_sizes'].apply(check_percentage)]
    
    # n users
    if n_users.value == '' or len(n_users.value) == 0:
        pass
    else:
        total = total[:int(n_users.value)]
        
    indicator.value = 90
    
    # оставляем только необходимые поля
    total = total[['user_id', 'user_uuid', 'first_name', 'last_name', 'phone_number', 'user_email', 'segment', 'is_b2b', 'order_cities', 'platform', 'device_type', 'os', 'new_or_old', 'shipping_method_kinds', 'types_delivery', 'orders_period', 'min_month_orders', 'use_duration', 'order_cities', 'retailer_category_names', 'shipment_states', 'sberid_use', 'prime_lvl', 'spasibo_charged', 'bnpl_used', 'retailer_sizes', 'address_count', 'retailer_names']]
    
    ### ------ downloading file ------ ###
    file_downloading.filename = "ux_file.xlsx"
    total.to_excel(file_downloading.filename, index=False)
    file_downloading.file = file_downloading.filename
    indicator.value = 95
    
    file_downloading.button_type = 'primary' 
    file_downloading.auto = False
    file_downloading.embed = True
    
    indicator.value = 100
    
    end_time = datetime.now()
    print(f'Выгрузка заняла {round((end_time - start_time).seconds/60, 1)} минут')


# # Выгрузка

# In[11]:


# поля
platform  = pn.widgets.MultiChoice(name='Платформа', options=["Все платформы","web", "app"], value=["Все платформы"], margin=(50, 10, 10, 10))
shipping_method_kind  = pn.widgets.MultiChoice(name='Способ получения', options=["Не важно","pickup", "by_courier", "scan_pay_go"], value=["Не важно"], margin=(50, 10, 10, 10))
os  = pn.widgets.MultiChoice(name='Операционная система', options=["Все ОС","android", "ios", "windows", "mac", "linux"], value=["Все ОС"], margin=(50, 10, 10, 10))
device_type  = pn.widgets.MultiChoice(name='Тип девайса (для web)', options=["Все типы","desktop", "mobile"], value=["Все типы"], margin=(50, 10, 10, 10))
retailer_name = pn.widgets.TextInput(name='Название ритейлера (большими буквами) ***', placeholder='ВВЕДИТЕ НАЗВАНИЕ', margin=(50, 10, 10, 10))
type_delivery  = pn.widgets.MultiChoice(name='Тип доставки', options=["Не важно","asap", "planned", "pickup"], value=["Не важно"], margin=(50, 10, 10, 10))
prime_lvl  = pn.widgets.MultiChoice(name='Прайм', options=["Не важно","non-prime", "prime", "prime+", "young"], value=["Не важно"], margin=(50, 10, 10, 10))
retailer_category_name = pn.widgets.MultiChoice(name='Категория ритейлера', options=["Не важно", "Продукты питания", "Рестораны", "Алкоголь", "Товары для дома", "Детские товары", "Косметика", "Cпортивное питание", "Аптека", "Цветы", "Канцелярские товары", "Товары для животных", "Электроника", "Одежда, обувь, аксессуары"], value=["Не важно"], margin=(50, 10, 10, 10))
city = pn.widgets.TextInput(name='Город *', placeholder='Введите город', margin=(50, 10, 10, 10))
cancellations = pn.widgets.Select(name='Отмены заказов', options=["Не важно","Есть отмены", "Нет отмен"], value="Не важно", margin=(50, 10, 10, 10))
sberid = pn.widgets.MultiChoice(name='Сбер ID', options=["Не важно","Не используют sberID", "Используют sberID, но не привязали его к СМ", "Используют sberID, привязали его к СМ"], value=["Не важно"], margin=(50, 10, 10, 10))
min_orders_period = pn.widgets.TextInput(name='Мин. количество заказов за период', placeholder='Введите число', margin=(50, 10, 10, 10))
min_addresses_period = pn.widgets.TextInput(name='Мин. количество адресов', placeholder='Введите число', margin=(50, 10, 10, 10))
major_shop_size = pn.widgets.Select(name='Большинство заказов из **', options=["Не важно","Супермаркеты", "Гипермаркеты", "Другие"], value="Не важно", margin=(50, 10, 10, 10))
spasibo = pn.widgets.MultiChoice(name='Списания СберСпасибо', options=["Не важно","Списывают", "Не списывают"], value=["Не важно"], margin=(50, 10, 10, 10))
bnpl = pn.widgets.MultiChoice(name='Использование BNPL', options=["Не важно","Используют", "Не используют"], value=["Не важно"], margin=(50, 10, 10, 10))
new_or_old = pn.widgets.MultiChoice(name='1-7 заказов/8+ заказов', options=["Не важно", "1-7 заказов", ">7 заказов"], value=["Не важно"], margin=(50, 10, 10, 10))
use_duration = pn.widgets.MultiChoice(name='Длительность использования СМ', options=["Не важно","Меньше месяца", "1-3 месяца", "3-6 месяцев", "6-12 месяцев", "Больше 12 месяцев"], value=["Не важно"], margin=(50, 10, 10, 10))
n_users = pn.widgets.TextInput(name='Макс. количество пользователей в выгрузке', placeholder='Введите число', margin=(50, 10, 10, 10))
month_orders = pn.widgets.TextInput(name='Минимальное кол-во заказов в месяц', placeholder='Введите число', margin=(50, 10, 10, 10))

# чекбоксы
exclude_b2b = pn.widgets.Checkbox(name='исключить B2B')
only_b2b = pn.widgets.Checkbox(name='только B2B')
must_buy_nonfood = pn.widgets.Checkbox(name='исключить не покупавших в категории nonfood (Все остальное)')
must_buy_food = pn.widgets.Checkbox(name='исключить не покупавших в категории food (Продукты питания, RTE)')
food_nonfood_comment = pn.Row("Разделение **food/nonfood** работает корректно на данных после 01.09.2023")

period_start = pn.widgets.DatePicker(name='Начало периода',  margin=(50, 10, 10, 10))
period_end = pn.widgets.DatePicker(name='Конец периода (не более 5 мес от даты начала)',  margin=(50, 10, 10, 10))
file_downloading = pn.widgets.FileDownload(name="Скачать выгрузку", margin=(5, 10, 10, 10), height=50, width=400)
start_calculation_button = pn.widgets.Button(name='Начать расчет', button_type='success', margin=(50, 10, 10, 10), height=50)
    
box = pn.GridBox(period_start, period_end, platform, device_type, os, 
                 new_or_old, shipping_method_kind, type_delivery, min_orders_period, month_orders, use_duration, 
                 city, retailer_category_name, cancellations, sberid, prime_lvl, spasibo, bnpl, 
                 major_shop_size, min_addresses_period, retailer_name, n_users,
                 ncols=3, nrows=5, height=800)

chechboxes = pn.GridBox(exclude_b2b, only_b2b, must_buy_food,must_buy_nonfood, food_nonfood_comment,
                 ncols=1, nrows=3, height=160)

indicator = pn.indicators.Progress(name='Progress', value=0, width=200)
user_buttons = pn.GridBox(start_calculation_button, indicator, file_downloading,ncols=1, nrows=3, height=300)

start_calculation_button.on_click(calculate_report)
display(pn.Row(box),pn.Row(chechboxes), 
        pn.Row("*Города вводить через запятую с пробелом (пример: Самара, Москва). Если нужен один город – вводим одно слово.\n \
                **Более 50% заказов пользователя сделаны из супермаркетов/гипермаркетов/других магазинов.\n \
                ***Пишем большими буквами (пример: ПЯТЕРОЧКА, МАГНИТ). Если нужно указать несколько ритейлеров – вводить через запятую с пробелом"),
        pn.Row(user_buttons)
       )


# ## Поиск названий ритейлеров

# Ниже представлен топ-100 популярных ритейлеров за последний месяц. Для выгрузки можно выбирать названия отсюда

# In[12]:


def find_retailer_names():
    q = '''
        select retailer_name, uniq(order_number) as orders
        from gp_rep.rep__bi_shipment
        where toDate(report_dttm) > now() - interval '1 month'
        group by retailer_name
        order by orders desc
    '''

    retailers = pd.read_sql(q, con=engine)
    retailers = list(retailers['retailer_name'].values)
    retailers = [x.upper() for x in retailers]
    return retailers[:100]

for i in find_retailer_names():
    print(i)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




