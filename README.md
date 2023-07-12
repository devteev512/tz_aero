Требования
========
- Docker
- Astro CLI

Установка
================
- Склонировать репозиторий
```sh
git clone git@github.com:devteev512/tz_aero.git
```
- Собрать проект с помощью astro dev
```sh
  astro dev start
```

Описание
===========================
- Коннектор оформил в виде плагина, выполняет запрос к API https://random-data-api.com/api/cannabis/random_cannabis?size=10 и парсит данные в БД Postgres, предварительно подготовив таблицу в базе(создание, если не существует + truncate). Код можно посмотреть по пути plugins/cann_api_connector/connector.py
- Даг работающий по расписанию раз в 12 часов находится в dags/cann_api_DAG.py
