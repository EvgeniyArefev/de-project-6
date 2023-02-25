# Проект «Аналитические базы данных (на примере Vertica)»

### Описание

Целями проекта являются:

- научиться работать с аналитическими базами данных на примере `Vertica`;
- поработать с `MPP`-базой;
- разобраться с сортировкой, сегментацией, партицированием.

### Задача проекта

- спроектировать аналитическое хранилище по модели `Data Vault`
- составить аналитические запросы к `Vertica`

### Структура репозитория

- `/src/dags` - DAG's проекта
- `/src/sql` - DDL и DML скрипты

### Как запустить контейнер

Запустите локально команду:

```
docker run \
-d \
-p 3000:3000 \
-p 3002:3002 \
-p 15432:5432 \
--mount src=airflow_sp5,target=/opt/airflow \
--mount src=lesson_sp5,target=/lessons \
--mount src=db_sp5,target=/var/lib/postgresql/data \
--name=de-sprint-5-server-local \
sindb/de-pg-cr-af:latest
```

После того как запустится контейнер, будут доступны:

- Airflow
  - `localhost:3000/airflow`
- БД
  - `jovyan:jovyan@localhost:15432/de`
