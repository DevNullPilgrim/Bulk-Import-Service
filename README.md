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
