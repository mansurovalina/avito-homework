# Olist: локальное обучение через PostgreSQL, Airflow и MLflow

Проект обучает выданную модель опоздания доставки Olist. PostgreSQL хранит фиксированный набор заказов, Airflow выполняет четыре этапа, MLflow хранит параметры, метрики и модель. Обучение выполняется на CPU.

## Запуск с чистого клона

Нужны Git, работающий Docker Engine и Docker Compose v2. Клонируйте репозиторий и выполняйте все команды из каталога с `compose.yaml`:

```bash
git clone git@github.com:mansurovalina/avito-homework.git avito-homework
cd olist-homework
cp .env.example .env
docker compose up -d --build
docker compose ps
```

Дождитесь состояния `healthy` у сервисов. Откройте Airflow по адресу <http://127.0.0.1:8080> и MLflow по адресу <http://127.0.0.1:5050>. Если порт занят, измените `AIRFLOW_PORT` или `MLFLOW_PORT` в `.env`. Внутри Docker Airflow обращается к PostgreSQL по имени `source`, к MLflow — по `http://mlflow:5000`.

В Airflow включите и запустите `example_one_task`, затем включите `olist_training` и запустите его вручную. Четыре задачи `extract → validate → prepare → train` должны стать зелёными. DAG также имеет еженедельное расписание без догоняющих запусков.

Для диагностики:

```bash
docker compose logs --tail=100 airflow
docker compose logs --tail=100 mlflow
docker compose exec -T source psql -U olist -d olist -c 'SELECT count(*), count(DISTINCT order_id) FROM orders;'
```

Исходный набор содержит 21 663 уникальных заказа. При границе по умолчанию `2017-07-31` ожидаются 17 928 строк train и 3 735 строк validation.

## Результат обучения

В MLflow откройте эксперимент `olist_training`. Для каждого запуска Airflow создаётся отдельный MLflow run. В нём сохраняются параметры `epochs`, `seed`, `lr`, `cutoff`, `train_rows`, `validation_rows`, `snapshot_sha256`, метрики `train_loss` и `validation_pr_auc` по эпохам, тег `dag_run_id` и артефакт `model/`. Тег содержит точный идентификатор запуска Airflow, а SHA256 вычислен по исходному CSV, созданному задачей `extract`.

Скопируйте 32-символьный идентификатор MLflow run и подставьте его в команду:

```bash
docker compose exec -T airflow python -m check_model --run-id 9da9770cee144acb8c14e52d69cd0a1d
```

Проверка скачивает модель из MLflow и выводит пять прогнозов. Проверенный локальный результат: `[0.7106074, 0.5012513, 0.2709562, 0.1435118, 0.2275717]`. Этот run существует только в локальном volume автора: после чистого клонирования используйте идентификатор созданного там run.

## Проверка повторов и восстановления

Запустите `olist_training` вручную ещё раз. Оба запуска должны завершиться успешно и создать разные MLflow run. В базе остаётся 21 663 заказа; рабочие CSV лежат в разных каталогах запусков внутри volume `work`.

Для проверки валидации используйте испорченную **копию** снимка с повторным `order_id` и временно верните её путь из `extract`. Новый запуск должен остановиться на `validate`; `prepare` и `train` не выполняются. После опыта восстановите код.

Для проверки Clear временно вызовите исключение в `train`, запустите новый DAG run, затем удалите исключение и выполните Clear только задачи `train` в этом run. В проверенном запуске `manual__2026-11-04T16:03:08.446287+00:00` первоначально упал только `train`. После Clear он завершился успешно; `extract`, `validate` и `prepare` сохранили свои прежние времена выполнения. Обучение использовало уже созданные файлы train/validation.

Результаты MLflow сохраняются в именованном volume. Проверьте это так:

```bash
docker compose restart
docker compose up -d --force-recreate --no-deps mlflow
docker compose exec -T airflow python -m check_model --run-id <ПРЕЖНИЙ_MLFLOW_RUN_ID>
```

Дождитесь готовности MLflow перед последней командой. Не используйте `docker compose down -v`: она удаляет volumes с данными и моделью.

## Состав проекта

- `src/olist_pipeline.py`: чтение БД, проверки, подготовка, обучение и запись в MLflow.
- `dags/olist_training.py`: расписание и зависимости задач.
- `src/olist_model.py`: выданная модель и преобразования признаков; не изменяется.
- `src/check_model.py`: выданная проверка загрузки и прогноза; не изменяется.
- `data/`: выданный фиксированный набор, описание и лицензия; остаётся в Git.

Локальный `.env`, снимки, модели и логи исключены из Git. Данные Olist распространяются по условиям `data/LICENSE.md`.
