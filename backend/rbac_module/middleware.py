from collections.abc import Callable

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from .database import get_db_session
from .models import User, UserRole
from .security import AuthError, decode_access_token


ROLE_ACCESS = {
    UserRole.SUPER_ADMIN: {UserRole.SUPER_ADMIN, UserRole.ROOT_ADMIN, UserRole.ADMIN},
    UserRole.ROOT_ADMIN: {UserRole.ROOT_ADMIN},
    UserRole.ADMIN: {UserRole.ADMIN},
}


def _parse_token(auth_header: str | None) -> str:
    if not auth_header:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing Authorization header")
    parts = auth_header.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid auth scheme")
    return parts[1].strip()


def get_current_user(
    authorization: str | None = Header(default=None, alias="Authorization"),
    db: Session = Depends(get_db_session),
) -> User:
    token = _parse_token(authorization)
    try:
        payload = decode_access_token(token)
    except AuthError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

    user = db.query(User).filter(User.email == payload["sub"]).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid user")
    return user


def require_roles(*allowed_roles: UserRole) -> Callable:
    def dependency(current_user: User = Depends(get_current_user)) -> User:
        reachable = ROLE_ACCESS.get(current_user.role, {current_user.role})
        if not set(allowed_roles).intersection(reachable):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient role privileges")
        return current_user

    return dependency
