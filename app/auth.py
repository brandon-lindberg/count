from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBasic, HTTPBasicCredentials, HTTPBearer

from app.config import get_settings

basic_auth = HTTPBasic(auto_error=False)
bearer_auth = HTTPBearer(auto_error=False)


async def require_admin_auth(credentials: HTTPBasicCredentials | None = Depends(basic_auth)) -> None:
    settings = get_settings()
    if settings.disable_auth or not settings.admin_username or not settings.admin_password:
        return

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )

    valid = secrets.compare_digest(credentials.username, settings.admin_username or "") and secrets.compare_digest(
        credentials.password,
        settings.admin_password or "",
    )
    if not valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


async def require_service_auth(credentials: HTTPAuthorizationCredentials | None = Depends(bearer_auth)) -> None:
    settings = get_settings()
    if settings.disable_auth or not settings.service_api_token:
        return

    if credentials is None or not secrets.compare_digest(credentials.credentials, settings.service_api_token or ""):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid service token",
            headers={"WWW-Authenticate": "Bearer"},
        )
