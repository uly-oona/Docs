# Глоссарий

## `DWH` - Корпоративное хранилище данных (КХД)
| Сокращение    | Описание | Дополнительно |
|   ---         | ---     | ---       |
| `GP PROD` | Продовая база данных Greenplum.| [Confluence](https://wiki.sbmt.io/pages/viewpage.action?pageId=3120605159) <br> <details>`host`: c-c9qe5c6g537prkfhuac2.rw.mdb.yandexcloud.net<br> `port`: 5432 </details> | 
| `GP DEV` | База данных Greenplum, предназначенная для разработки и тестирования. | <details>`host`: c-c9qfrnqkuod6v7ive03r.rw.mdb.yandexcloud.net<br> `port`: 5432</details>|
| `CH`  |   Продовая база данных Clickhouse. | [Confluence](https://wiki.sbmt.io/x/PNXdtQ) <br> <details>`host`: rc1b-t40nsk085ch73kmt.mdb.yandexcloud.net <br> `port`: 8123 <br> `schemas`: gp_dm, gp_rep  </details> |
| `Data Vault` | Методология проектирования детального слоя `DWH`, которая включает в себя три ключевых компонента:<br>`h_`: Хаб  - основное представление сущности; <br> `s_`: Сателлит - атрибуты сущности; <br> `l_`: Линк - связь между сущностями. | [Confluence](https://wiki.sbmt.io/pages/viewpage.action?pageId=3017788860) |
| `SCD2` | Медленно меняющиеся измерения (от англ. Slowly Changing Dimensions, SCD) — механизм отслеживания изменений в данных измерения в терминах хранилища данных. | [Confluence](https://wiki.sbmt.io/x/itopvg) |
| `Source`, `src`, `Источник` | Источник данных (PaaS, s3, Nextcloud и т.п.), откуда переливаются данные в `DWH`. | |
| `ods_<src>`, `vw_ods_<src>` | Схема `GP`: сырой слой (**operational data store**), хранящий данные из соответствующего `src`. Таблицы сохраняются в том виде, в котором они хранятся на `источнике` (as is). | [Список подключенных источников](https://airflow-dwh.sbmt.io/dbt-docs/#!/source_list/) <br> (раздел `Sources`)
| `dds` | Схема `GP`: детальный слой (**detail data sourse**). Содержит объекты `Data Vault`. Строится на данных слоев `ods_<src>` и `vw_ods_<src>`. | `dbt` структура: <br> [*.yml](https://gitlab.sbmt.io/analytics/datazone/data_lake/dwh-dags/-/tree/master/dbt_dags/models/schemas/detail), [*.sql](https://gitlab.sbmt.io/analytics/datazone/data_lake/dwh-dags/-/tree/master/dbt_dags/models/scripts/detail), [*.md](https://gitlab.sbmt.io/analytics/datazone/data_lake/dwh-dags/-/tree/master/dbt_dags/docs/detail)  |
| `dm` | Схема `GP`: слой витрин (**datamart**). Содержит таблицы в форме снежинки (**snowflake**) - факты и измерения. Строится на данных слоя `dds` | [Confluence](https://wiki.sbmt.io/pages/viewpage.action?pageId=3017757419) <br> `dbt` структура: <br> [*.yml](https://gitlab.sbmt.io/analytics/datazone/data_lake/dwh-dags/-/tree/master/dbt_dags/models/schemas/datamart), [*.sql](https://gitlab.sbmt.io/analytics/datazone/data_lake/dwh-dags/-/tree/master/dbt_dags/models/scripts/datamart), [*.md](https://gitlab.sbmt.io/analytics/datazone/data_lake/dwh-dags/-/tree/master/dbt_dags/docs/datamart) |
| `rep`| Схема `GP`: слой отчетов (**report**). Содержит готовые отчеты, которые построены на объектах слоя `dds` или `dm` с помощью `dbt` и могут реплицироваться в `CH`. | `dbt` структура: <br> [*.yml](https://gitlab.sbmt.io/analytics/datazone/data_lake/dwh-dags/-/tree/master/dbt_dags/models/schemas/report), [*.sql](https://gitlab.sbmt.io/analytics/datazone/data_lake/dwh-dags/-/tree/master/dbt_dags/models/scripts/report), [*.md](https://gitlab.sbmt.io/analytics/datazone/data_lake/dwh-dags/-/tree/master/dbt_dags/docs/report) |
| `gp_rep` | Схема `CH`: слой, куда реплицируются данные из схемы `rep` (`GP`) | [Ссылка на каталог](https://gitlab.sbmt.io/dwh/ch/ch-schema/-/tree/master/config/analytics/gp_rep) |

## `GIT` - распределенная система контроля версий
| Сокращение    | Описание | Дополнительно |
|   ---         | ---     | ---       |
| `MR` | Merge Request - запрос на слияние своей рабочей ветки с мастером. | [Инструкция по работе с GIT](https://wiki.sbmt.io/x/xiqPsg) |
| `dwh-dags` | Репозиторий проекта `DWH`, содержащий скрипты и конфиги, необходимые для построения `DAGs` для `Airflow`. | [Репозиторий](https://gitlab.sbmt.io/analytics/datazone/data_lake/dwh-dags) |
| `ae-utils` | Репозиторий, содержащий внутренние инструменты, полезные в работе с `CH` и `GP`. | [Репозиторий](https://gitlab.sbmt.io/dwh/ae-utils) |
| `ch-schema` | Репозиторий, необходимый для автоматизации создания и изменения таблиц в `CH`. | [Репозиторий](https://gitlab.sbmt.io/dwh/ch/ch-schema) <br> [Confluence](https://wiki.sbmt.io/pages/viewpage.action?pageId=3108898241) |
| `doc` | Текущий репозиторий, содержащий документацию проекта `DWH`.  | [Репозиторий](https://gitlab.sbmt.io/dwh/doc) |

## Инструментарий

| Сокращение    | Описание | Дополнительно |
|   ---         | ---     | ---       |
| `AirFlow` | Инструмент, который позволяет разрабатывать, планировать и осуществлять мониторинг ETL/ELT-процессов. | [Официальная документация](https://airflow.apache.org/docs/apache-airflow/stable/index.html) |
| `DAGs` |  Ключевая сущность `Airflow`. Это скрипты на Python, которые описывают логику выполнения задач: какие должны быть выполнены, в каком порядке и как часто. | [Официальная документация](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html) |
| `Docker` | Платформа, которая позволяет упаковать в контейнер приложение со всем окружением и зависимостями, а затем доставить и запустить его в целевой системе.  С помощю `Docker` мы можем тестировать `DAGs` на локальной машине в условиях, максимально приближенным к продовым. | [Официальная документация](https://docs.docker.com/guides/docker-overview/) <br> [Внутренняя инструкция по настройке](https://wiki.sbmt.io/x/Cku2tw)|
| `dbt` | Data Build Tool - фреймворк, предназначенный для перемещения данных между слоями в ХД и выполнения различных трансформаций данных (очистка, дедупликация, агрегирование, фильтрация, обогащение), а также для формирования документации. С помощью `dbt` формируются `DAGs` для `Airflow` без необходимости написания Python-скриптов. Для генерации модели необходимо, чтобы она была описана 3-мя файлами: <br> 1. *.sql - скрипт на SELECT из нужных таблиц с нужными фильтрами <br> 2. *.yml - конфиг, описывающий параметры создания таблицы <br> 3. *.md - документация, отражающая смысл данных таблицы | [Официальная документация](https://docs.getdbt.com/docs/build/documentation) <br> <details><summary> Confluence </summary> [Создание таблиц на dbt](https://wiki.sbmt.io/x/QY46tQ) <br> [Структура dbt конфига](https://wiki.sbmt.io/x/o51ztg)</details> |
| `dbt_dags` | Вложенная часть репозитория `dwh-dags`, которая содежит в себе `dbt`-проект, т.е. всё, что относится к `dbt`. | [Ссылка на каталог](https://gitlab.sbmt.io/analytics/datazone/data_lake/dwh-dags/-/tree/master/dbt_dags) |
| `dbt Docs` | Интегрированный в `Airflow` инструмент, позволяющий увидеть весь `dbt` проект в юзер-френдли формате: структура объектов, лайнейдж зависимостей, описательные характеристики объектов и скрипты их формирования. | [Ссылка на dbt Docs](https://airflow-dwh.sbmt.io/dbt-docs) |
| `DBeaver` | Приложение для работы с различными базами данных. Оно создано как универсальное решение, работающее с Greenplum, PostgreSQL, MySQL, Oracle, Microsoft SQL Server и другими.| [Инструкция по установке и настройке](https://wiki.sbmt.io/x/BRO_tg) |
| `VS Code` | Visual Studio Code - IDE для работы с кодом. | [Скачать](https://code.visualstudio.com/Download)
| `dbt Power User` | Расширение для `VS Code`, которое позволяет тестировать `dbt` модели без необходимости поднимать локальный `Docker` | [Инструкция по настройке](https://wiki.sbmt.io/x/vz8aug) |
| `ch_converter` | Python скрипт, который помогает быстро и эффективно сгенерировать yml-конфиг для репозитория `ch-schema` на базе yml-конфига из проекта `dbt_dags`. | [Ссылка на утилиту](https://gitlab.sbmt.io/dwh/ae-utils/-/tree/master/ch_converter) |
| `dbt_reviewer` | Внутренняя утилита для тестирования `dbt`-`MR`, представлена в виде Python-скрипта и является наиболее простым и универсальным решением, чем `dbt Power User` и `Docker`. | [Ссылка на демонстрацию](https://wiki.sbmt.io/x/SLVvvw) <br> [Ссылка на утилиту](https://gitlab.sbmt.io/dwh/ae-utils/-/tree/master/dbt_reviewer) |

# Начало работы

Для начала работы необходимо:

1. Изучить `Глоссарий`, убедиться что все формулировки понятны, а приложенная документация изучена.

2. Иметь сетевую связность к `CH`, `GP PROD`, `GP DEV`
    <details><summary>Подробнее</summary>  

    Проверка наличия сетевой связности осуществляется путем ввода в терминал команды:  
    Mac:
    ```shell
    nc -zc <host> <port>;
    ```
    Windows:
    ```shell
    Test-NetConnection -ComputerName '<host>' -Port <port>;
    ```
    При наличии сетевой связности получем ответ: `Connection to <host> <port> succeeded!`  
    Если ответа нет, либо ответ иной, то необходимо завести задачу на [сетевой доступ от пользователя в облако (VPN Firewall)](https://jira.sbmt.io/servicedesk/customer/portal/5/create/294) к нужному хосту по нужному порту.
    </details>

3. Иметь учетные записи для `CH`, `GP PROD`, `GP DEV`
    <details><summary>Подробнее</summary>

    Если УЗ нет, то создать [запрос доступов персональный](https://jira.sbmt.io/servicedesk/customer/portal/5/create/103) к нужному сервису.
    </details>

4. Иметь роль Developer в репозиториях `dwh-dags`, `ch-schema`, `ae-utils`
    <details><summary>Подробнее</summary>

    Убедиться в наличии доступа можно перейдя по ссылке из глоссария. Если ссылка не открвается, значит доступа нет и нужно создать [запрос доступов персональный](https://jira.sbmt.io/servicedesk/customer/portal/5/create/103) к нужному репозиторию.  
    Проверить доступные репозитории и роли в них, можно в своем профиле (пункт меню `Contributed projects`)
    </details>

5. Настроить локальную среду для разработки и тестирования
    <details><summary>Подробнее</summary>

    ℹ️ Все необходимые ссылки и инструкции можно найти в Глоссарии в разделе Инструментарий
    - склонировать GIT репозитории `dwh-dags`, `ae-utils` и `ch-schema`
        -  настроить локальный репозиторий `ae-utils` в соответствии с [README.md](https://gitlab.sbmt.io/dwh/ae-utils/-/blob/master/README.md)
            - настроить `dbt_reviewer` в соответствии с [readme.md](https://gitlab.sbmt.io/dwh/ae-utils/-/blob/master/dbt_reviewer/readme.md)
            - настроить `ch_converter` в соответствии с [readme.md](https://gitlab.sbmt.io/dwh/ae-utils/-/blob/master/ch_converter/readme.md)
    - установить `DBeaver` для запросов к БД
    - установить `VSCode` для взаимодействия с кодовой базой
    </details>


# Чеклист для построения отчета

- [ ] [Шаг 1](#check_list_step_1): Я придумал имена конечных отчетов, которые соответствуют правилам;
    <details><summary>Правила наименования объектов слоя rep</summary>
    
     Таблицы

    - Имена таблиц должны полностью отражать смысл сущности:
        - Например в случае с целями, выбранными клиентами, наименование таблицы будет не "goals", a "user_goals".
    - Используйте глаголы для таблиц, связанных с действиями:
        - Если таблица представляет собой некоторое действие или событие (например, транзакции или заказы), использование глаголов может помочь лучше отразить её назначение. Например, вместо "sale" можно использовать "sell" или "purchase".
    - Избегайте использования зарезервированных слов: 
        - Некоторые слова являются зарезервированными в SQL и не могут быть использованы в качестве имен таблиц. Перед созданием таблицы проверьте, что выбранное вами имя не является [зарезервированным](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql-keywords.html).
    - Используйте snake_case
        - Имена записываются в нижнем регистре с подчеркиванием между словами.
    - Наименование таблицы не должно превышать 40 символов.
    - По возможности избегайте аббревиатур в именах таблиц:
        - Аббревиатуры могут затруднить понимание содержимого таблицы. Если имеется необходимость в использовании сокращений (например когда название не умещается в лимит), рекомендуется использовать [стандартные сокращения](https://wiki.sbmt.io/pages/viewpage.action?pageId=3017824670).

- [ ] [Шаг 2](#check_list_step_2): Я разобрался с тем, какие объекты слоя `dds` мне понадобятся для построения отчета;

- [ ] [Шаг 3](#check_list_step_3): С помощью `DBeaver` я подготовил SQL-скрипты, которые:
    - исключают [наличие красных флагов в структуре SQL-запросов](https://wiki.sbmt.io/x/3Bz3uw)
    - при запуске в `GP PROD` возвращают необходимый результат

- [ ] [Шаг 4](#check_list_step_4): Я подготовил *.sql-файл для `dbt` слоя `rep`, куда сохранил рабочий скрипт и учел рекомендации.
    <details><summary>Рекомендации</summary>
    
    - все ссылки на таблицы заменены конструкциями вида `{{ ref('table_name') }}` 
    - соблюдены [правила нейминга](https://wiki.sbmt.io/x/nlngsw)
    - в джойнах используются алиасы
    - в отчетах не используются таблицы сырого слоя (`ods_<src>`, `vw_ods_<src>`)
    - в отчет при необходимости подтянуты идентификаторы объектов на источнике, а не идентификаторы объектов в DWH
    - не забыты условия фильтрации по источнику, флагам (учтено, что в линках хранятся данные из разных источников)
    - не хранятся в открытом виде персональные данные (ФИ(О), телефон, адрес жительства/прописки/доставки, почтовый ящик, немаскированные карточные данные). Персональные данные при сборке отчета нужно обернуть в функцию `md5()`.
      Например, при сборке отчета с полем `user_fio`, содержащим ФИО пользователя, в SQL-скрипте нужно использовать `md5(user_fio)`.
    </details>

- [ ] [Шаг 5](#check_list_step_5): Я подготовил *.yml-файл для `dbt` слоя `rep` на основе шаблона, где описал все необходимые параметры;
    <details><summary>Шаблон</summary>

    ```yml
    version: 2

    schedule: <время старта дага в формате крон * * * * * по UTC>
    sensor:
      required_tables:
        detail:
          - 'dds.<имя_таблицы_на_которой_строится_скрипт>'
          - 'dds.<имя_таблицы_на_которой_строится_скрипт>'
          - 'dds.<имя_таблицы_на_которой_строится_скрипт>'
        preaggregate:
          - 'dds.<имя_таблицы_на_которой_строится_скрипт>'
          - 'dds.<имя_таблицы_на_которой_строится_скрипт>'
          - 'dds.<имя_таблицы_на_которой_строится_скрипт>'
        datamart:
          - 'dm.<имя_таблицы_на_которой_строится_скрипт>'
          - 'dm.<имя_таблицы_на_которой_строится_скрипт>'
          - 'dm.<имя_таблицы_на_которой_строится_скрипт>'
    domain: <название_домена_если_применимо>
    owner:
      business:
        - first_name.last_name@sbermarket.ru
        - first_name.last_name@sbermarket.ru
    models:
      - name: <название_таблицы>
        config:
          materialized: snapshot_materialization
          process_type: 'report'
          src_cd: <источник>
          truncate_flg: True
          temp_models:
            - <название_временной_таблицы_если_используется_в_скрипте>
            - <название_временной_таблицы_если_используется_в_скрипте>
          meta:
            schema: 'rep'
            distribution: <Имя_колонки>
        description: '{{ doc("<название_таблицы>") }}'
        columns:
          - name: <Имя_колонки>
            data_type: <Тип_данных>
            description: <Описание_колонки>
          - name: <Имя_колонки>
            data_type: <Тип_данных>
            description: <Описание_колонки>
            nullable: <если поле содержит NULL, то true, в противном случае параметр не указываем, так как по умолчанию false>
          - name: <Имя_колонки>
            data_type: <Тип_данных>
            description: <Описание_колонки>
            nullable: <если поле содержит NULL, то true, в противном случае параметр не указываем, так как по умолчанию false>

        clickhouse:
          stage_schema_table: 'stage.gp_rep__rep__<название_таблицы>_shard'
          ch_schema_table_shard: gp_rep.rep__<название_таблицы>_shard
          ch_schema_table: gp_rep.rep__<название_таблицы>
          ch_push_etl_log_dttm: "data_interval_end.subtract(days=1).strftime('%Y-%m-%d')"
          columns:
            - <Имя_колонки>
            - <Имя_колонки>
            - <Имя_колонки>
          mode: overwrite
    ```
    </details>

- [ ] [Шаг 6](#check_list_step_6): Я подготовил *.md-файл для `dbt` слоя `rep` на основе шаблона, где описал смысл данных, хранящихся в таблице;
    <details><summary>Шаблон</summary>

    Для переноса строки на новую используем тег `<br>` в конце предыдущей строки.
    ```
    {% docs <название_таблицы> %}
    Первая строка описания таблицы<br>
    Вторая строка описания таблицы
    {% enddocs %}
    ```
    </details>

- [ ] [Шаг 7](#check_list_step_7): Я запустил `dbt_reviewer` и убедился, что целевые модели без ошибок создались в `GP DEV`;

- [ ] [Шаг 8](#check_list_step_8): Я [создал MR](https://wiki.sbmt.io/pages/viewpage.action?pageId=2995727046), который соответствует [правилам оформления](https://wiki.sbmt.io/x/qjRZtw) и в канале dwh-support попросил его поревьювить;

- [ ] [Шаг 9](#check_list_step_9): Я убедился, что в конце каждого файла есть пустая строка без символов (при просмотре `MR` в браузере нет надписи "No newline at end of file");

- [ ] [Шаг 10](#check_list_step_10): Я прошел ревью, отработав все замечания и получив 2 аппрува;

- [ ] [Шаг 11](#check_list_step_11): Я подготовил `MR` в `ch-schema` с помощью `ch_converter` и через канал dwh-support попросил его поревьювить;

- [ ] [Шаг 12](#check_list_step_12): После того, как мой `MR` в `ch-schema` был вмерджен я попросил отправить основной `MR` в релиз;
    - Это связано с особенностями репликации в `CH` - таким образом мы сперва  создаем условия для возможности перелить данные из `GP` в `CH` и избежать ошибок при выкатке в прод.

- [ ] [Шаг 13](#check_list_step_13): После того, как оба мои `MR` были вмерджены я заполнил информацию о своем отчете в [DataHub](https://datahub.k-analytics.sbmt.io/container/urn:li:container:480051fa0de49376e6f0b02edf6b8f92/Entities?is_lineage_mode=false) иначе проливка моего отчета будет невозможной
    <details><summary>Правила заполнения информации для витрин gp_rep в DataHub</summary>

    Обязательные параметры, которые нужно указать для витрин:
    * About -> Documentation - информация о витрине, включая требуемое время, к которому она должна быть сформирована (например, SLA 06:30 МСК).
    * Owners - владельцы витрины (минимум 2 человека).
    * Tags
        + Tier:
            + **Tier 1** - критически важные витрины данных (например, bi_shipment, bi_adjustment, company_managers).  
            + **Tier 2** - менее важные витрины, используемые для основных дашбордов в вашем домене.  
            + **Tier 3** - прочие витрины, которые используются в дашбордах.  
            + **Tier 4** - прочие таблицы, которые где-то используются, и вы знаете для чего они.
        + **GP_ODS** - тег проставляется только для витрин, построенных на сырых данных GP (ODS-слой)
    * Domain - бизнес-домен.  
    </details>
