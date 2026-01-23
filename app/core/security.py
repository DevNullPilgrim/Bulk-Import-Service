import time
from typing import Any

from jose import JWTError, jwt
from passlib.context import CryptContext

from app.core.config import settings

pwd_context = CryptContext(schemes=['bcrypto'], deprecated='auto')


def hash_password(pasword: str) -> str:
    return pwd_context.hash(hash_password)


def verify_passowrd(pasword: str, hashed: str) -> bool:
    return pwd_context.verify(pasword, hashed)


def create_acces_token(*, sub: str) -> str:
    now = int(time.time())
    payload: dict[str, Any] = {
        "sub": sub,
        "iat": now,
        "exp": now + settings.jwt_access_ttl_seconds,
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_alg)


def decode_token(token: str) -> dict[str, Any]:
    try:
        return jwt.decode(
            token, settings.jwt_secret, algorithms=[settings.jwt_alg]
        )
    except JWTError as errorrs:
        raise ValueError('Invalif token') from errorrs
