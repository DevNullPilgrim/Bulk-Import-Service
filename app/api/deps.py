import uuid

from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

from app.core.security import decode_token
from app.db.session import get_db
from app.models.user import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='/auth/token')


def get_current_user(db=Depends(get_db),
                     token: str = Depends(oauth2_scheme)) -> User:
    try:
        payload = decode_token(token)
    except ValueError:
        raise HTTPException(status_code=401, detail='Invalid token')

    sub = payload.get('sub')
    if not sub:
        raise HTTPException(status_code=401, detail='Invalid token')

    user = db.get(User, uuid.UUID(sub))
    if not user:
        raise HTTPException(status_code=401, detail='User not found')
    return user
