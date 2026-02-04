# Bulk Import Service

Асинхронный импорт CSV в Postgres с прогрессом, режимами `insert_only` / `upsert` и отчётом об ошибках в S3 (MinIO).

## Содержание
- [Стек](#стек)
- [Возможности](#возможности)
- [Сервисы](#сервисы)
- [Структура проекта](#структура-проекта)
- [Быстрый старт](#быстрый-старт)
- [Авторизация](#авторизация)
- [Импорт CSV](#импорт-csv)
- [Отчёт об ошибках (errors.csv)](#отчёт-об-ошибках-errorscsv)
- [Формат CSV](#формат-csv)
- [Переменные окружения](#переменные-окружения)
- [Тесты](#тесты)
- [Troubleshooting](#troubleshooting)

## Стек
- FastAPI
- Celery
- Postgres
- Redis
- MinIO
- Alembic

## Возможности
- Загрузка CSV → создание задачи импорта (асинхронно)
- Статусы job: `pending → processing → done/failed`
- Прогресс: `processed_rows` растёт во время обработки
- Режимы:
  - `insert_only` — дубли (в БД или внутри файла) считаются ошибкой
  - `upsert` — `ON CONFLICT (email) DO UPDATE`
- `errors.csv`: полный отчёт по битым строкам загружается в MinIO
- `GET /imports/{id}/errors` возвращает presigned URL на скачивание отчёта
- JWT авторизация: регистрация/логин
- Idempotency: `Idempotency-Key` уникален на пользователя — повторный `POST /imports` возвращает тот же job

## Сервисы
- API: `http://localhost:8000` (Swagger: `/docs`)
- MinIO Console: `http://localhost:9001`
- MinIO S3 endpoint: `http://localhost:9000`

## Структура проекта
```text
bulk_import_service/
├── app/                      # FastAPI приложение (HTTP API)
│   ├── main.py               # создание FastAPI + подключение роутеров
│   ├── api/                  # роуты, зависимости, схемы
│   │   ├── deps.py           # зависимости (current_user и т.п.)
│   │   └── routers/
│   │       ├── auth.py       # регистрация/логин (JWT)
│   │       └── imports.py    # POST /imports, GET /imports/{id}, /errors
│   ├── core/                 # конфиг, безопасность, клиенты
│   │   ├── config.py         # Settings (.env/env vars)
│   │   ├── security.py       # пароль/хеш + JWT utils
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
├── alembic/                  # миграции БД
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

## Быстрый старт

Требования: Docker + Docker Compose.

1) Создай `.env`:
```bash
cp .env.example .env
```

2) Подними проект:
```bash
docker compose up -d --build
```

3) Примени миграции:
```bash
docker compose exec api alembic upgrade head
```

4) Проверь статус:
```bash
curl http://localhost:8000/health
```

## Авторизация

### Регистрация
```bash
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"user@test.com","password":"pass12345"}'
```

### Получение токена
Требует `jq` (опционально, но удобно):
```bash
TOKEN=$(curl -s -X POST http://localhost:8000/auth/token \
  -H "Content-Type: application/json" \
  -d '{"email":"user@test.com","password":"pass12345"}' | jq -r .access_token)

echo "$TOKEN"
```

## Импорт CSV

### Контракт
- `POST /imports?mode=insert_only|upsert`
- `multipart/form-data`: поле файла называется `file`
- Заголовки:
  - `Authorization: Bearer <token>`
  - `Idempotency-Key: <любая непустая строка>`

### Пример запроса
```bash
IDEM_KEY="demo-1"

curl -X POST "http://localhost:8000/imports?mode=insert_only" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: $IDEM_KEY" \
  -F "file=@customers_2000.csv;type=text/csv"
```

### Поведение idempotency
- Первый `POST` → `201 Created`
- Повторный `POST` с тем же `Idempotency-Key` (для того же пользователя) → `200 OK` и тот же `id`

### Проверить статус / прогресс
Во время выполнения `processed_rows` должен расти, а статус быть `processing`.
```bash
JOB_ID="..."

curl -s "http://localhost:8000/imports/$JOB_ID" \
  -H "Authorization: Bearer $TOKEN"
```

## Отчёт об ошибках (errors.csv)

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
```bash
curl -L -o errors.csv "<URL_ИЗ_ОТВЕТА>"
```

Коды ответов:
- `409 Not ready` — job ещё выполняется или отчёт не готов
- `404 Not found` — отчёта нет

## Формат CSV
Первая строка (header) игнорируется. Колонки:
- `email` (обязательно)
- `first_name`
- `last_name`
- `phone`
- `city`

## Переменные окружения

Основные (см. `.env.example`):
- `DATABASE_URL` — DSN Postgres
- `REDIS_URL` — broker/backend для Celery
- `S3_ENDPOINT_URL` — внутренний endpoint (docker-сеть), например `http://minio:9000`
- `S3_PUBLIC_ENDPOINT_URL` — внешний endpoint для presigned URL, например `http://localhost:9000`
- `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, `S3_REGION`
- `S3_PRESIGN_TTL_SECONDS` — TTL presigned ссылок
- `JWT_SECRET`, `JWT_ALG`, `JWT_ACCESS_TTL_SECONDS`

Тюнинг воркера:
- `BATCH_SIZE` (по умолчанию 500)
- `PROGRESS_EVERY` (по умолчанию 50)
- `IMPORT_SLOW_MS` (по умолчанию 0)

Лимит загрузки (опционально):
- `MAX_UPLOAD_BYTES`

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

## Troubleshooting

### `NoSuchBucket` / MinIO не готов
Проверь логи:
```bash
docker compose logs minio minio_init
```
`minio_init` одноразовый и должен завершаться `exit 0`.

### Presigned URL работает в браузере, но не работает из контейнера
Внутри контейнера `localhost` указывает на сам контейнер. Presigned URL подписывается под `S3_PUBLIC_ENDPOINT_URL`,
поэтому скачивание должно выполняться с хоста/клиента по этой ссылке.
