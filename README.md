# Bulk Import Service
[![Bulk service](https://github.com/DevNullPilgrim/Bulk-Import-Service/actions/workflows/main.yml/badge.svg?branch=main&event=push)](https://github.com/DevNullPilgrim/Bulk-Import-Service/actions/workflows/main.yml)

Асинхронный импорт CSV в Postgres с прогрессом, режимами insert_only/upsert и отчётом об ошибках в S3 (MinIO).

## Стек:

- FastAPI
- Celery
- Postgres
- Redis
- MinIO
- Alembic.
---


## Возможности

- Загрузка CSV → создание задачи импорта (асинхронно)
- Статусы job: `pending → processing → done/failed`
- Прогресс: `processed_rows` растёт во время обработки
- Два режима:
  - `insert_only` — дубли (в БД или внутри файла) считаются ошибкой
  - `upsert` — `ON CONFLICT (email) DO UPDATE`
- `errors.csv`: полный отчёт по битым строкам загружается в MinIO
- Эндпоинт `/imports/{id}/errors` возвращает presigned URL на скачивание отчёта
- Авторизация JWT: регистрация/логин
- Idempotency: `Idempotency-Key` уникален на пользователя — повторный POST возвращает тот же job
---

## Сервисы

- API: `http://localhost:8000` (Swagger: `/docs`)
- MinIO Console: `http://localhost:9001`
- MinIO S3 endpoint: `http://localhost:9000`
---

## Структура проекта
```
bulk_import_service/
├── app/                      # FastAPI приложение (HTTP API)
│   ├── main.py               # создание FastAPI + подключение роутеров
│   ├── api/                  # роуты, зависимости, схемы
│   │   ├── deps.py           # auth dependencies (current_user и т.п.)
│   │   └── routers/
│   │       ├── auth.py       # регистрация/логин (JWT)
│   │       └── imports.py    # POST /imports, GET /imports/{id}, /errors
│   ├── core/                 # конфиг, безопасность, клиенты
│   │   ├── config.py         # Settings (.env/env vars)
│   │   ├── security.py       # хеширование паролей + JWT utils
│   │   └── celery_client.py  # постановка задач в Celery
│   ├── db/                   # SQLAlchemy база/сессии
│   │   ├── base.py           # Base.metadata для моделей
│   │   └── session.py        # создание engine/session
│   ├── models/               # ORM модели (User, ImportJob, Customer)
│   └── storage/
│       └── s3.py             # MinIO/S3 put/get + presigned URL
│
├── worker/                   # Celery worker (обработка импортов)
│   ├── celery_app.py         # task: скачать CSV → обработать → записать в БД → errors.csv
│   └── errors_report.py      # сборка errors.csv
│
├── alembic/                  # миграции БД (schema evolution)
│   ├── env.py                # подключение metadata + запуск миграций
│   └── versions/             # ревизии миграций
│
├── tests/                    # интеграционные тесты API/worker/БД
│
├── docker-compose.yml         # локальный запуск: api, worker, postgres, redis, minio
├── Dockerfile                 # сборка образа приложения
├── .env.example               # пример переменных окружения
├── alembic.ini                # конфиг Alembic
└── README.md                  # документация
```
---


## Быстрый старт (Docker)

1) Создай `.env`:
```bash
cp .env.example .env
```

2) Поднимаем проект
```bash
docker compose up --build
```

3) Поднимаем миграции
```bash
docekr compose exec api alembic upgrade head
```

4) Проверта статуса
```bash
curl http://localhost:8000/health
```
---


## Авторизация

### Регистарация
```bash
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"user@test.com","password":"pass12345"}'
```

### Получение токена
```bash
TOKEN=$(curl -s -X POST http://localhost:8000/auth/token \
  -H "Content-Type: application/json" \
  -d '{"email":"user@test.com","password":"pass12345"}' | jq -r .access_token)
echo "$TOKEN"
```
---

## Создать импорт

### Контракт:
POST /imports?mode=insert_only|upsert
multipart form-data: поле файла называется file

Заголовки:
   Authorization: Bearer <token>
   Idempotency-Key: <любая непустая строка>
   Пример (файл customers_2000.csv):

```bash
IDEM_KEY="demo-1"

curl -X POST "http://localhost:8000/imports?mode=insert_only" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: $IDEM_KEY" \
  -F "file=@customers_2000.csv;type=text/csv"
```

Пример ответа:
```json
{
  "id": "...",
  "status": "pending",
  "mode": "insert_only",
  "filename": "customers_2000.csv",
  "total_rows": 0,
  "processed_rows": 0,
  "error": null,
  "created_at": "..."
}
```
---

## Поведение idempotency:
Первый POST → 201 Created
Повторный POST с тем же Idempotency-Key (для того же пользователя) → 200 OK и тот же id

### Проверить статус / прогресс
Во время выполнения processed_rows должен расти, а статус быть processing.
```bash
JOB_ID="..."

curl -s "http://localhost:8000/imports/$JOB_ID" \
  -H "Authorization: Bearer $TOKEN"
```
---

## Скачать отчёт об ошибках (errors.csv)
Если job завершился с ошибками, можно получить presigned URL:
```bash
curl -s "http://localhost:8000/imports/$JOB_ID/errors" \
  -H "Authorization: Bearer $TOKEN"
```

Ответ:
```json
{ "url": "http://localhost:9000/..." }
```

Скачать файл:
```json
curl -L -o errors.csv "<URL_ИЗ_ОТВЕТА>"
```
---


## Замечания:

409 Not ready — job ещё в процессе или отчёт не готов
404 Not found — отчёта нет

---


## Формат CSV

Первая строка (header) игнорируется. Колонки:
   `email` (обязательно)
   `first_name`
   `last_name`
   `phone`
   `city`

---


## Переменные окружения

Основные (см. .env.example):
`DATABASE_URL` — DSN Postgres
`REDIS_URL` — broker/backend для Celery
`S3_ENDPOINT_URL` — внутренний endpoint (docker-сеть), например http://minio:9000
`S3_PUBLIC_ENDPOINT_URL` — внешний endpoint для presigned URL, например http://localhost:9000

`S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, `S3_REGION`
`S3_PRESIGN_TTL_SECONDS` — TTL presigned ссылок
`JWT_SECRET`, `JWT_ALG`, `JWT_ACCESS_TTL_SECONDS`

### Тюнинг воркера:

`BATCH_SIZE` (по умолчанию 500)
`PROGRESS_EVERY` (по умолчанию 50)
`IMPORT_SLOW_MS` (по умолчанию 0)

### Лимит загрузки (опционально):

`MAX_UPLOAD_BYTES`

---

## Тесты

Тесты интеграционные и ожидают поднятый docker stack.
Важно: тесты делают TRUNCATE таблиц только в `APP_ENV=dev/test`.
Запуск:
```bash
cp .env.example .env
docker compose up -d --build
docker compose exec api alembic upgrade head
docker compose exec api pytest
```