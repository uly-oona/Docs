## Как подготовить дизайн свитчбэк-теста

В этом документе разберемся с тем, как сделать дизайн теста на примере ухудшающего теста Out-of-Stock (OOS). Это ML-модель каждый час прогнозирует для каждого товара СберМаркета доступность для сборки на ближайшее время. Если модель предсказывает, что товар собрать не получится, он блокируется в каталоге магазина

### Почему здесь используется свитчбэк-тест?

Среди товаров, которые модель может заблокировать, есть “якорные товары”, из-за которых пользователь вообще собирает корзину. Представим, что мы тестируем эту фичу с рандомизацией по магазинам, так пользователю могут быть одновременно доступны магазины с OOS, где его товар заблокирован и без OOS, где товар доступен к заказу. Не увидев искомый товар, пользователь может уйти в другой магазин. Так мы применяем воздействие только к магазинам тестовой группы, а увеличивается конверсия в магазинах контрольной группы.

В данном примере нарушается основная предпосылка валидности вообще любого эксперимента, которая называется **stable unit treatment value assumption (SUTVA)**. Эта предпосылка означает, что воздействие, которое мы применяем к юнитам из тестовой группы, должно отражаться только на них и не должно влиять на юниты из контроля. Т.к. она  нарушается, классический А/Б тест невозможен.

Если это не ваш случай, скорее вам нужен дизайн теста по магазина. Если вы сомневаетесь, приходите в ~ab-stores.

### Перед началом дизайна эксперимента

Убедитесь в следующем

- Вам необходим дизайн именно свитчбэк-эксперимента
- Вы определили регионы и магазины, которые могут участвовать в эксперименте (как в контрольной, так и в тестовой группах)
- Вы определили целевые и информационные метрики и ожидаемые минимальные эффекты для них
- У теста есть [документация](https://wiki.sbmt.io/pages/viewpage.action?pageId=3025185771) в [пространстве](https://wiki.sbmt.io/pages/viewpage.action?pageId=3022176076) платформы экспериментов

### Как использовать библиотеку

1. Клонировать этот репозиторий
`git clone https://gitlab.sbmt.io/analytics/datazone/ab_methodology.git`
2. Установить [poetry](https://python-poetry.org/docs/). Дополнительно про установку и использование poetry можно почитать [здесь](https://wiki.sbmt.io/display/ANLT/Poetry).
3. Проверить установку poetry: ``poetry --version`` 
4. Выполнить ``poetry install --no-root``
5. Если вы используете библиотеку из ноутбука: 
    - ``poetry add ipykernel``
    - ``poetry run python -m ipykernel install --user --name ab_methodology``
    - выбрать установленный кернел 
6. Заполнить .env файл с кредами к БД. Публичный файл лежит ``data/connectors.env``
7. Если запускаете библиотеку из ноутбука: ``%dotenv /full/path/to/connectors.env`` или ``load_dotenv("/full/path/to/connectors.env")``

#### Использование коробочного решения через .py файл

Передаем путь до конфига эксперимента и путь к .env файлу с кредами.

- Создание дизайна: `poetry run python get_switchback_test.py '/full/path/to/test_params_name.py' '/full/path/to/connectors.env'`
- Оценка эксперимента:  `poetry run python get_switchback_final_results.py '/full/path/to/test_params_name.py' '/full/path/to/connectors.env'`

#### Использование через ноутбук

Пример ноутбука лежит в ``notebooks/swithcback_test_example.ipynb``.  Это более гибкий инструмент, в котором можно воспользоваться функционалом, не поддерживаемом из коробки (дизайн для нескольких тестовых групп, загрузка результатов в сэндбокс). В тетрадке лежат и функции для дизайна, и для оценки.

### Проверяем метрики

Многие метрики уже добавлены в соответствующий файл [metrics_functions.py](data/metrics_functions.py). Обязательными метриками для каждого эксперимента являются GMV, Gross Profit, AOV, количество заказов. Перед использованием стоит посмотреть, что запросы метрик соответствуют тому, что вы хотите посчитать. Например, убедитесь, что в запросе для метрики фильтрация по времени осуществляется по желаемой колонке (например, created_at или shipped_at). В случае, если метрика состоит из нескольких компонент, убедитесь, что все нужные компоненты указаны. Если вы не влияете на какие-то части “составной метрики”, их можно удалить для увеличения чувствительности. Например, по дефолту gmv считается как `gmv_advertising + gmv_service_fee_net_promo + gmv_goods_net_promo`, но вы в тесте влияете только на `gmv_goods_net_promo`, можно добавить новую метрику, состоящую только из `gmv_goods_net_promo`.

Обратите внимание, что в зависимости от того, в каком [блоке](https://wiki.sbmt.io/pages/viewpage.action?pageId=3025189720), то есть изолированной части заказа вы проводите изменение -- оформлении (то есть изменение связано с интерфейсом, с которым пользователь сталкивается до осуществления заказа) или execution (что происходит с заказом после того как пользователь оформил заказ, сборка, доставка, отмены) даты, по которым происходит фильтрация будут отличаться. Так для экспериментов в блоке оформления нужно использовать даты оформления заказа (completed_at), для блока execution -- даты доставки (shipped_at). Названия метик, которые считаются по shipped_at и эквивалентам должны заканчиваться на `_sh` (`gmv_sh`, `gross_profit_sh`). 

В общем случае не нужно ставить дополнительных фильтров на быструю/плановую доставку, наблюдения будут фильтроваться по генеральной совокупности. Однако для метрик, которые считаются по сменам (OPH, MPH, утилизация) нет однозначной привязки наблюдений к магазинам быстрой доставки. Запросы для быстрой и плановой разделены (`_express`, `_planned`), магазины быстрой для этих метрик не фильтруются по генеральной совокупности. Метрики в быстрой доставке, считающиеся по сменам, мы **можем считать только на уровне заказа, агрегировать до магазина-часа мы не можем**. 

Найти существующую метрику можно по значению словаря `metrics_full_name` в data/handle_metrics.py или по самим запросам в `data/metrics_functions.py`.

Для добавления метрик есть определенные требования, часть из них обусловлена функциональностью, часть удобством использования. Пожалуйста, придерживайтесь их.

1. Запросы для метрик хранятся в [metrics_functions.py](data/metrics_functions.py)
2. Для каждой метрики запросы для числителя и знаменателя пишутся отдельно. Для обозначения числителя мы добавляем к названию метрики `_num`, для знаменателя -- `_denom`.
3. Требования к функциям:
    1. Называется по имени метрики с префиксом get (например, get_gmv)
    2. В докстринге указаны пояснения к метрике / есть название тикета теста, в рамках которого метрика создается
    3. Принимает аргументы для фильтрации по времени `date_from` и `date_to`
4. В каждом запросе должны присутствовать следующие поля:
    1. Название метрики
    2. Дата в формате DateTime и названием Date
    3. Одно из обозначений магазина — `store_id_click` (id магазина в клике), `store_id_postgres` (id магазина в шоппере), `store_uuid` (uuid магазина)

Давайте добавим новую метрику — “Доля отмененных товаров по причине `Нет нужного товара`”. Она считается как $\frac{Кол-во\ отмененных\ товаров}{Все\ товары}$.  Метрики агрегируются суммой, поэтому для того, чтобы посчитать все товары для знаменателя, проставляем единицы. Обратим внимание, что числитель -- бинарная метрика, то есть для каждого заказа определяется, было ли опоздание. Добавим эти запросы в [metrics_functions.py](data/metrics_functions.py).

```python
def get_order_canc_no_good_num(start_date, end_date):
    """Доля отмен по причине Нет нужного товара для AB-1012 (числитель)"""

    q = f"""
    select toDate(period_dt) as Date,
           store_id as store_id_click,
           case when shipment_state='canceled' and goods_instamart=0 and bb_sert=0 and cancellation_reason_nm = 'Нет нужного товара' then 1 else 0 end as orders_canc_no_good_num
    from dm.bi__shipments_financial
    where toDate(period_dt) >= ('{start_date}')
    and toDate(period_dt) <= ('{end_date}')
    """
    return read_sql_query(q, con="click")

def get_order_canc_no_good_denom(start_date, end_date):
    """Доля отмен по причине Нет нужного товара для AB-1012 (знаменатель)"""

    q = f"""
    select toDate(period_dt) as Date,
           store_id as store_id_click,
					 1 as orders_canc_no_good_denom
    from dm.bi__shipments_financial
    where toDate(period_dt) >= ('{start_date}')
    and toDate(period_dt) <= ('{end_date}')
    """
    return read_sql_query(q, con="click")
```

Дальше обновим [handle_metrics.py](data/handle_metrics.py).  Добавим название метрики ``order_canc_no_good`` и числитель и знаменатель. Если знаменателя нет, на его месте нужно написать None. Важно, чтобы название метрики в данных совпадало с названием ключа в словаря. 

```python
metrics_definition = {...,
											"order_canc_no_good": ["order_canc_no_good_num", "order_canc_no_good_denom"]
}
```

Для числителя и знаменателя (если он есть), нужно заполнить словарь с указанием на соответствующие им функции.

```python
metrics_func = {...,
							  "order_canc_no_good_num": get_order_canc_no_good_num,
							  "order_canc_no_good_denom": get_order_canc_no_good_denom
}
```

Добавляем полные названия для всей метрики:

```python
metrics_full_name = {...,
										"order_canc_no_good": "Доля отмен по причине 'Нет нужного товара'"
}
```

После того как вы добавите новые метрики, не забудьте сделать MR. После того как вы добавите новые метрики, не забудьте сделать MR. Чтобы убедиться, что запросы и функции для метрик корректные, стоит воспользоваться [ноутбуком](notebooks/check_metrics.ipynb) или напрямую функцией [check_metrics](test_collection/check_metrics.py). Для свитчбэк-тестов нужно задать ``stores_test=True``. 


### Создаем файл генеральной совокупности

В свитчбэк-тестах наш юнит регион-время, однако в файле генеральной совокупности мы также передаем все магазины, для которых может проводиться тест. Часто мы можем включить только магазины быстрой или плановой доставок, не хотим включать магазины из технического теста. Таких условий может быть очень много, поэтому мы ожидаем, что вы  передадите список из точек, которые уже подпадают по условие. Перед выбором магазинов для участия в эксперименте, убедитесь, что они доступные и в них были заказы за последний месяц.

Id магазина должно быть также одно из: store_uuid (uuid магазина), store_id_click (id магазина из клика), store_id_postgres (id магазина из шоппера). Эта название должно совпадать с filter_column_id из конфига, сейчас поддерживаются только значения магазинов. В файле также должно быть switch_unit_name из конфига, сейчас поддерживаются city_id и operational_zone_id.

Поддерживаются .csv файлы, с разделителем запятой. Проверьте, что в вашем файле нет пропусков.

В случае теста OOS мы добавляем все магазины, на которых включен OOS. Так как мы переключаем по городам, добавляем колонку city_id. Пример колонок: store_uuid, operational_zone_id, city_id


### Заполнение конфига

Конфиг представлен в виде словаря, на его заполненную версию можно посмотреть в [get_switchback_test.py](test_params/get_switchback_test.py).

`test_name` — номер задачи

Мы рекомендуем оставлять именно номер задачи, а не название, тк по нему проще всего определить, какая фича тестируется и кто этот тест проводит. Задачу не нужно заводить на доске AВ, но в ней нужно указать ссылку на документацию с описанием и результатами. 

`gen_pop_csv_file_path` -- путь до .csv файла с юнитами генеральной совокупности (например, все магазины, которые могут участвовать в эксперименте)

`w_length` -- длина теста в неделях

Длина эксперимента должна быть кратна 7, это связано с сезонностью и возможно разными эффектами в будни и в выходные, поэтому минимальная рекомендуемая длина теста — 2 недели. С увеличением длины теста до 4х недель мощность увеличивается не сильно, при возможности стоит увеличивать именно кол-во регионов.

`filter_column_id` -- название колонки для фильтрации всех метрик по юнитам генеральной совокупности

Обсудили это в части про файл генеральной совокупности, напомню, что название в параметре `filter_column_id` должно быть в файле. Метрики фильтруются по id магазина, поэтому поддерживаются следующие поля: uuid магазина (`store_uuid`) / id магазина из клика (`store_id_click`) / id магазина из шоппера (`store_id_postgres`)

`effects` -- словарь метрика-эффект, отдельно прописывается числитель и знаменатель в зависимости от предполагаемого влияния эксперимента, названия должны совпадать значениям в metrics_func

Например, чтобы включить в тест AOV, мы отдельно прописываем gmv и кол-во заказов. Иногда мы предполагаем, что влияем только на числитель, тогда можно для экономии времени включить в словарь только его.

`aggregation`-- словарь метрика-словарь с применяемыми к метрике типом агрегации
Какие-то метрики мы хотим считать по заказам, а какие-то с агрегацией по магазину в час. Например, кол-во заказов мы не можем считать с агрегацией на заказ, в наших исходных данных на уровне заказов кол-во заказов — константная единица. Иногда интерпретация метрик в зависимости от агрегации может быть разная, например, GMV на заказ (те чек) и GMV магазина в час. В случае ratio метрик стоит указывать числитель на уровне заказа и с агрегацией до магазина в час, и знаменатель на уровне магазина в час. 

`stratification_params` -- параметры стратификации

- `group_shares` -- список с долями каждой группы, при запуске из файла поддерживаются только 2 группы
- `traffic_size` -- доля трафика (юнитов генеральной совокупности), которую можно сплитовать

    Иногда мы хотим взять только часть генеральной совокупности в тест, например, если раскатка фичи очень дорогая. Однако стоит помнить, что при сокращении генеральной совокупности, мощность будет сокращаться.

- `n_splits` -- количество сплитов для симуляций

    Мы рекомендуем использовать минимум 200 сплитов для свитчбэк-тестов. Использование большего кол-ва сплитов помогает с большей точностью оценки FPR и мощности, однако в случае с методами, которые мы используем в этих тестах, большое кол-во симуляций будет затратным.

- `id_column` -- название колонки-юнита рандомизации, принимает значение ‘unit’
- ``cluster_column`` -- колонка кластеризации, возможное значение `strata`

``switchback_params`` -- параметры свитчбэк-теста

- `freq` -- частота переключения, записывается в формате ‘6H’, ‘12H’, ‘24H’ и так далее

    Чем чаще переключения, тем больше свитчбэк-юнитов и скорее всего больше мощность. Если у вас есть возможность для частого переключения, используйте ее. Однако стоит подумать, не будет ли у вас перетока между временными группами. Пример при тестировании сурджа — механизма регулирования кол-ва заказов: город днем в контроле, в это время появляется много заказов, и вечером, когда город будет в тесте, будет много опозданий. Эти опоздания не из-за фичи, а переток между временными юнитами.

    Максимальная рекомендуемая величина здесь — 24 часа, делать свитчбэк-юниты еще более длинными нет смысла.

- `switch_unit_name` -- название географической единицы для свитчбэк-юнита (поддерживаются city_id и operational_zone_id)
- `switchback_time` -- время переключения свитчбэка в формате HH:MM ('23:05')

Поддерживается одна из опций, т.е. только одна должна быть определена в конфиге:

- `local_time` -- True, если переключение свитчбэк-юнитов по локальному времени.
- `switchback_time_zone` -- одновременное переключение по конкретной таймзоне (например, "Europe/Moscow", "UTC")

`use_mlm` — использовать ли MLM в дополнение к CRSE

Для оценки эксперимента, мы используем два метода, использующие вложенную структуру данных. По-дефолту мы используем CRSE, он гораздо быстрее считается, но хуже с точки зрения FPR/мощности. Если вы получаете неудовлетворительные результаты при использовании CRSE, оценивайте MLM.

`mlm_params`-- параметры для MLM/CRSE (эти параметры одинаковы для использованию любым методом) ****

- `unit_interest` -- название географичего юнита, поддерживается city_id и operational_zone_id

    Название географического юнита свитчбэка (`switch_unit_name`) не должно отличаться от названия группы для MLM/CRSE (`unit_interest`)

### Загрузка конфига 

После разработки дизайна конфиг нужно загрузить в соответствующую таблицу sandbox.switcback_test_params при помощи [upload_test_params](test_collection/utils_upload_splits.py#L111). 

### Результат использования

Результаты выполнения пишутся в директорию с названием теста.

#### Общая информация

`test_name.log` -- файл с логами дизайна, здесь можно узнать, на какой стадии находится дизайн, какие файлы и куда сохраняются, какие ошибки возникают в процессе

#### Данные

По-дефолту промежуточные данные не сохраняются, однако это можно регулировать параметрами `save` и `save_raw` в [get_switchback_data](test_collection/get_swithcback_data.py#L31) и [get_swithcback_splits](test_collection/get_switchback_splits.py#L8)

- `switchback_splits_test_name.pkl` — сплиты симуляций
- `raw_exp_test_name.pkl` — экспериментальные данные до агрегации
- `exp_test_name.plk` — агрегированные данные

При оценке теста сохраняются такие же файлы, но с префиксом `raw_split_results_` или `spit_results_`.

#### Дизайн

- `FPR_test_name.md` -- таблица с FPR на АА для каждой метрики на 1% и 5% уровне значимости

    Мы считаем АА тесты валидными, если FPR меньше принятого уровня значимости (1%, 5%). Иногда мы хотим перейти к 1% уровню значимости, чтобы получить валидные FPR, тогда важно не забыть оценивать мощность на том же уровне значимости.

- `power_test_name.md` -- таблица с мощностью на AB для каждой метрики на 1% и 5% уровне значимости

    Мы считаем АБ тесты валидными, если мощность (power) больше 70%

- `effect_distribution_test_name.md` -- таблица с синтетическим эффектом (mean и std) для метрик
- `test_name_metric_method_AA_plot.png` -- CDF plot для отдельной метрики на АА тесте
- `test_name_metric_method_AB_plot.png` -- CDF plot для отдельной метрики на АB тесте

#### Сплит

- `start_exp_date` -- дата и время начала свитчбэк-юнита
- `end_exp_date` -- дата и время окончания свитчбэк-юнита
- `test_params['switch_unit_name']` -- id региона свитчбэк-юнита, например, operational_zone_id
- `treatment` -- group_0 - контрольная группа, group_1 - тестовая группа

Если было выбрано локальное время:

- `utc_offset` -- UTC Offset
- `time_zone` -- часовой пояс

Технические поля: time_cluster, start_end_hour, operational_zone_id, unit, strata

### Что делать, если FPR больше уровня значимости? 

- Перейти к 1% уровню значимости. Напомним, что и мощность мы будем считать на уровне 1% 
- Если вы оценивали с помощью CRSE, оценить с помощью MLM
- Удалить выбросы 
- Декомпозировать метрику. Например, gmv считается как gmv_advertising + gmv_service_fee_net_promo + gmv_goods_net_promo, возможно ваша фича действует только на одну из этих компонент, можно оставить только ее

### Что делать, если мощность недостаточна? 

- Увеличить кол-во доступных юнитов (увеличить кол-во доступных магазинов, ритейлеров или городов) 
- Увеличить длительность эксперимента 
- Если вы оценивали с помощью CRSE, оценить с помощью MLM
- Удалить выбросы 
- Декомпозировать метрику. Например, gmv считается как gmv_advertising + gmv_service_fee_net_promo + gmv_goods_net_promo, возможно ваша фича действует только на одну из этих компонент, можно оставить только ее

### Обновление сплита 

Допустим, вы сделали дизайн теста, вас устраивают результаты АА и АБ тестов, но техническая возможность провести тест наступает только через месяц. В этом случае можно переиспользовать старый сплит. Для этого нужно проставить в конфиге теста желаемые даты как ``fact_start_date`` и ``fact_end_date`` и обновить сплит через функцию [update_switchback_split](test_collection/utils_splits.py).

### Пересечение тестов 

Можем пересекать тесты, вкладывая их друг в друга, максимально возможно пересекать 3 теста, длиной свитчбэк-юнита в 12, 24 и 48 часов. Сплит 12 часов вложен в сплит 24 часов, то есть в каждый свитчбэк-юнит большего сплита вложены контрольная и тестовая группа меньшего сплита.  Подробнее про методологию можно прочитать [здесь](https://medium.com/bolt-labs/tips-and-considerations-for-switchback-test-designs-d1bd7c493024). Так как сплит для "вложенного" теста подготавливается на основании сплита с большей длиной свитчбэк-юнита, тесты для параллельного запуска нужно определить заранее. Для подготовки сплита нужно использовать функцию [generate_nested_sw_split](test_collection/utils_splits.py).

### Добавление информации о тесте в sandbox

После того как вы убедились, что дизайн вашего теста проходит по FPR/мощности и получили сплит, его нужно загрузить в `sandbox.splits_ab_switchback`. Для этого воспользуйтесь функцией [upload_final_split](test_collection/utils_upload_splits.py).  Это важно при планировании тестов, чтобы развести с потенциально пересекающимися тестами. Также вы можете использовать данные из таблицы при построении дашборда. Для построения дашборда можно воспользоваться этим [гайдом](https://wiki.sbmt.io/pages/viewpage.action?pageId=3029823657).

### Запуск теста 
Данная библиотека предоствляет только дизайн и сплит, для запуска теста обратитесь к разработчикам. [Здесь](https://wiki.sbmt.io/pages/viewpage.action?pageId=3036456357) можно почитать об одной возможности запуска тестов, по всем вопросам обращайтесь в ~proj-exp-platform.

Перед запуском теста напишите в ~ab-stores с предполагаемыми датами теста и ссылкой на документацию теста. 

### Подведение результатов

Для получения оценки эксперимента в конфиге заполняются следующие поля:

- `fact_start_date`- дата начала эксперимента в формате YYYY-MM-DD
- `fact_end_date` -- дата окончания эксперимента в формате YYYY-MM-DD

Сплит эксперимента должен называться по формату “split_test_name.csv”. Проверьте, что файл так называется.

В зависимости от метода, который вы использовали во время дизайна (MLM или CRSE) стоит использовать функции [evaluate_switchback_test_crse](test_collection/get_results.py) или [evaluate_switchback_test_mlm](test_collection/get_results.py). Помните, что вы оцениваете результаты на том же уровне значимости, какой был принят для каждой метрики во время дизайна.

В зависимости от агрегации, магазин/час или заказ, абсолютные значения метрики будут оцениваться на соответствующем уровне.

Что делать, если вы уже провели тест и вам нужно посчитать новую метрику? Включить ее в расчеты просто так нельзя. Сначала с ней нужно провести АА тесты, убедиться, что доля ложных прокрасов при выбранном уровне значимости подходит, и только потом оценивать ее в эксперименте. Если тест был давно, АА тесты нужно считать на датах эксперимента.

### Что-то случилось

Если что-то не получается или осталось непонятным, у вас есть запрос на добавление функционала или вы заметили ошибку, пишите в ~ab-stores.
