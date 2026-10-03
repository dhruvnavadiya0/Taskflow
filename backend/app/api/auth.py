"""Phase 4: Authentication API — registration and login.

POST /api/auth/register  → create a new user account
POST /api/auth/login     → authenticate and receive a JWT
"""

import logging
import re
from typing import Annotated

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from app.database.connection import DbSession
from app.database.models import User, UserRole
from app.core.security import create_access_token
from app.core.rate_limit import check_rate_limit, rate_limit_key
from app.queue.redis_queue import job_queue

logger = logging.getLogger("taskflow.auth")

router = APIRouter(prefix="/api/auth", tags=["auth"])

_EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")


class RegisterRequest(BaseModel):
    email: str = Field(..., min_length=5, max_length=255)
    password: str = Field(..., min_length=6, max_length=128)
    role: UserRole = UserRole.USER


class LoginRequest(BaseModel):
    email: str = Field(..., min_length=1, max_length=255)
    password: str = Field(..., min_length=1, max_length=128)


class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    email: str
    role: str


@router.post("/register", response_model=AuthResponse, status_code=201)
def register(data: RegisterRequest, db: DbSession) -> AuthResponse:
    """Create a new user account."""
    # Validate email format
    if not _EMAIL_REGEX.match(data.email):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid email format",
        )

    # Check for duplicate email
    existing = db.query(User).filter(User.email == data.email).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered",
        )

    user = User(
        email=data.email,
        role=data.role,
    )
    user.set_password(data.password)
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_access_token(user.id, user.role.value)
    logger.info("User registered: %s (role=%s)", user.email, user.role.value)

    return AuthResponse(
        access_token=token,
        user_id=user.id,
        email=user.email,
        role=user.role.value,
    )


@router.post("/login", response_model=AuthResponse)
def login(data: LoginRequest, db: DbSession, request: Request) -> AuthResponse:
    """Authenticate and return a JWT access token."""
    # Rate limit login attempts by client IP
    client_ip = request.client.host if request.client else "unknown"
    try:
        check_rate_limit(
            job_queue.client,
            rate_limit_key("login", client_ip),
        )
    except Exception:
        # If rate limit check itself fails, still try to process
        pass

    user = db.query(User).filter(User.email == data.email).first()
    if user is None or not user.verify_password(data.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    token = create_access_token(user.id, user.role.value)

    return AuthResponse(
        access_token=token,
        user_id=user.id,
        email=user.email,
        role=user.role.value,
    )
