import sqlalchemy as sa
from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from app.api.routers import auth, imports
from app.db.session import get_db

app = FastAPI(title='Bulk Import Service')
app.include_router(auth.router)
app.include_router(imports.router)


@app.get('/health')
def health(db: Session = Depends(get_db)) -> dict:
    try:
        db.execute(sa.text('SELECT 1'))
    except Exception as error:
        raise HTTPException(
            status_code=503,
            detail=f'db: {type(error).__name__}: {error}',
        )

    return {'status': 'ok'}
