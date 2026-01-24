from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from app.api.schemas import LoginIn, RegisterIn, TokenOut
from app.core.security import (
    create_access_token,
    hash_password,
    verify_password,
)
from app.db.session import get_db
from app.models.user import User

router = APIRouter(prefix='/author', tags=['author'])


@router.post('/auth/register')
def register(data: RegisterIn, db=Depends(get_db)):
    email = str(data.email).lower()
    exists = db.execute(select(User)
                        .where(User.email == email)
                        ).scalar_one_or_none()
    if exists:
        raise HTTPException(409, 'email alredy exists.')

    user = User(email=email,
                hashed_password=hash_password(data.password))
    db.add(user)
    db.commit()
    db.refresh(user)
    return {'id': str(user.id), 'email': user.email}


@router.post('/auth/token', response_model=TokenOut)
def token(data: LoginIn, db=Depends(get_db)):
    email = str(data.email).lower()
    user = db.execute(select(User).where(
        User.email == email)).scalar_one_or_none()
    if not user or not verify_password(data.password, user.hashed_password):
        raise HTTPException(401, 'bad credentials')

    return {'access_token': create_access_token(sub=str(user.id)),
            'token_type': 'bearer'}
