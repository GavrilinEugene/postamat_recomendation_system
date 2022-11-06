# Репозиторий по сбору данных, обработке и созданию модели машинного обучения [#10 хакатона ](https://leaders2022.innoagency.ru/task10.html) от команды Geo.xyz

[Репозиторий сервсиса](https://github.com/hashbash/postamat)

## Оглавление:
1. [Подготовка](#Подготовка)
2. [Сбор данных](#Сбор-данных)
3. [Модель]( #Модель)

## Подготовка

 - Для разработки приложения необходимо склонировать репозиторий и поставить необходимые питоновские пакеты 

```Bash
git clone https://github.com/GavrilinEugene/postamat_recomendation_system.git data_postamat
cd data_postamat
python3 -m venv venv
source venv/bin/activate
```
 - Развернуть Postgis BD


## Сбор данных
Собираются данные по геополигонам административных районов и округов Москвы, населению в различных локациях
города, а также информация с Портала открытых данных Правительства Москвы (data.mos.ru) о городских объектах,
рекомендованных в техническом задании для размещения постаматов(киоски, библиотеки, МФЦ и т.д.)

Для работы с данными необходимо заполнить файл enum.py
```    
# insert mapbox token (https://docs.mapbox.com/help/tutorials/get-started-tokens-api/)
token = 'insert_mapbox_token'

# insert gis token (not free)
key = 'insert_gis_key'

# h3 index resolution
RES = 10

postgress_connection = '*.*.*.*:port/name'
postgress_db = 'database'
lp = 'login:password'
schema = 'schema'

api_key= 'insert mosru api token'
```


### collect_population_data.py
Сбор данных о населении в локациях и запись в базу данных на основе файла с данными из открытых источников.
<br>
```Bash
$python src/collect_population_data.py 
```

### collect_adm_okr.py
Сбор данных о геополигонах административных районов и округов Москвы и запись их в базу данных.<br>
<br>
```Bash
$python src/collect_adm_okr.py
```
  
### collect_data_from_mos_api.py
Сбор данных Портала открытых данных Правительства Москвы (data.mos.ru) о городских объектах,
рекомендованных в техническом задании для размещения постаматов(киоски, библиотеки, МФЦ и т.д.)
и запись их в базу данных.
<br>
```Bash
$python src/collect_data_from_mos_api.py
```

### collect_gis_data.py
Инфраструктурные объекты города: магазины, кафе, автомойки и прочее
```Bash
$python src/collect_gis_data.py
```

### collect_gis_data.py
Инфраструктурные объекты города 
```Bash
$python src/collect_isochrones.py
```

### upload_data_to_database.py
Загрузка больших  данных в созданную DB(изохроны, инфраструктура, сетка Москвы, агрегированные признаки)
```Bash
$python src/upload_data_to_database.py
```
