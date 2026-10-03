"""Pytest configuration — provides a test database and FastAPI test client."""

import os

# Point to an in-memory SQLite database for tests (must be set before imports)
os.environ["DATABASE_URL"] = "sqlite:///./test_taskflow.db"
os.environ["REDIS_URL"] = "redis://localhost:6379/15"  # use db 15 for test isolation
os.environ["RETRY_BASE_DELAY"] = "0"  # instant retries in tests

import pytest
from sqlalchemy import create_engine, StaticPool, event
from sqlalchemy.orm import sessionmaker, Session
from fastapi.testclient import TestClient

from app.database.connection import Base, get_db
from app.main import app


# Use an in-memory SQLite database for fast, isolated tests
TEST_ENGINE = create_engine(
    "sqlite://",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

# SQLite doesn't enforce CHECK constraints or foreign keys by default
@event.listens_for(TEST_ENGINE, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()

TestingSessionLocal = sessionmaker(bind=TEST_ENGINE, autocommit=False, autoflush=False)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(autouse=True)
def setup_database():
    """Create fresh tables before each test, tear them down after."""
    Base.metadata.create_all(bind=TEST_ENGINE)
    yield
    Base.metadata.drop_all(bind=TEST_ENGINE)


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def admin_client() -> TestClient:
    from app.core.security import get_current_user, require_admin
    from app.database.models import User, UserRole
    from app.database.connection import get_db
    from app.main import app
    
    mock_user = User(id="mock-admin", email="admin@example.com", password_hash="hash", role=UserRole.ADMIN)
    
    # Insert into the test DB
    db = next(app.dependency_overrides.get(get_db, get_db)())
    db.add(mock_user)
    db.commit()
    
    def mock_get_current_user():
        return mock_user
    
    app.dependency_overrides[get_current_user] = mock_get_current_user
    app.dependency_overrides[require_admin] = mock_get_current_user
    yield TestClient(app)
    app.dependency_overrides.pop(get_current_user, None)
    app.dependency_overrides.pop(require_admin, None)


@pytest.fixture
def db_session() -> Session:
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()
