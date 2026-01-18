## Bulk Import Service

### Run
1) Copy env:
   - cp .env.example .env

2) Start:
   - docker compose up --build

### Health
- http://localhost:8000/health

### MinIO Console
- http://localhost:9001
- user/pass: from .env (S3_ACCESS_KEY / S3_SECRET_KEY)