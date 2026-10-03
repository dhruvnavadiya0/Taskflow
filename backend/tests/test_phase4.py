"""Phase 4: Security, Observability, and Reliability tests."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.database.models import User, UserRole, Job
from app.queue.redis_queue import RedisQueue


# ---------------------------------------------------------------------------
# Authentication Tests
# ---------------------------------------------------------------------------
class TestAuthAPI:
    def test_register_user_success(self, client: TestClient):
        response = client.post(
            "/api/auth/register",
            json={"email": "test@taskflow.local", "password": "securepassword123"}
        )
        assert response.status_code == 201
        data = response.json()
        assert "access_token" in data
        assert data["email"] == "test@taskflow.local"
        assert data["role"] == "USER"

    def test_register_duplicate_email(self, client: TestClient):
        client.post(
            "/api/auth/register",
            json={"email": "duplicate@taskflow.local", "password": "securepassword123"}
        )
        response = client.post(
            "/api/auth/register",
            json={"email": "duplicate@taskflow.local", "password": "securepassword124"}
        )
        assert response.status_code == 409

    def test_login_success(self, client: TestClient):
        client.post(
            "/api/auth/register",
            json={"email": "login@taskflow.local", "password": "securepassword123"}
        )
        response = client.post(
            "/api/auth/login",
            json={"email": "login@taskflow.local", "password": "securepassword123"}
        )
        assert response.status_code == 200
        assert "access_token" in response.json()

    def test_login_invalid_password(self, client: TestClient):
        client.post(
            "/api/auth/register",
            json={"email": "badpwd@taskflow.local", "password": "securepassword123"}
        )
        response = client.post(
            "/api/auth/login",
            json={"email": "badpwd@taskflow.local", "password": "wrongpassword"}
        )
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# Authorization and Job Ownership
# ---------------------------------------------------------------------------
class TestJobAuthorization:
    @pytest.fixture
    def user_token(self, client: TestClient):
        client.post(
            "/api/auth/register",
            json={"email": "user1@taskflow.local", "password": "securepassword123"}
        )
        resp = client.post("/api/auth/login", json={"email": "user1@taskflow.local", "password": "securepassword123"})
        return resp.json()["access_token"]

    @pytest.fixture
    def admin_token(self, client: TestClient):
        client.post(
            "/api/auth/register",
            json={"email": "admin@taskflow.local", "password": "securepassword123", "role": "ADMIN"}
        )
        resp = client.post("/api/auth/login", json={"email": "admin@taskflow.local", "password": "securepassword123"})
        return resp.json()["access_token"]

    def test_create_and_get_own_job(self, client: TestClient, user_token: str):
        headers = {"Authorization": f"Bearer {user_token}"}
        resp1 = client.post(
            "/api/jobs",
            json={"job_type": "SUM_NUMBERS", "payload": {"numbers": [1, 2]}},
            headers=headers
        )
        assert resp1.status_code == 201
        job_id = resp1.json()["id"]

        resp2 = client.get(f"/api/jobs/{job_id}", headers=headers)
        assert resp2.status_code == 200

    def test_cannot_access_other_users_job(self, client: TestClient, user_token: str):
        # Create user 2
        client.post(
            "/api/auth/register",
            json={"email": "user2@taskflow.local", "password": "securepassword123"}
        )
        resp = client.post("/api/auth/login", json={"email": "user2@taskflow.local", "password": "securepassword123"})
        user2_token = resp.json()["access_token"]

        # User 1 creates job
        headers1 = {"Authorization": f"Bearer {user_token}"}
        resp1 = client.post(
            "/api/jobs",
            json={"job_type": "SUM_NUMBERS", "payload": {"numbers": [1]}},
            headers=headers1
        )
        job_id = resp1.json()["id"]

        # User 2 tries to access
        headers2 = {"Authorization": f"Bearer {user2_token}"}
        resp2 = client.get(f"/api/jobs/{job_id}", headers=headers2)
        assert resp2.status_code == 403

    def test_admin_can_access_any_job(self, client: TestClient, user_token: str, admin_token: str):
        # User 1 creates job
        headers1 = {"Authorization": f"Bearer {user_token}"}
        resp1 = client.post(
            "/api/jobs",
            json={"job_type": "SUM_NUMBERS", "payload": {"numbers": [1]}},
            headers=headers1
        )
        job_id = resp1.json()["id"]

        # Admin tries to access
        headers_admin = {"Authorization": f"Bearer {admin_token}"}
        resp2 = client.get(f"/api/jobs/{job_id}", headers=headers_admin)
        assert resp2.status_code == 200

    def test_workers_api_requires_admin(self, client: TestClient, user_token: str, admin_token: str):
        # User gets 403
        resp1 = client.get("/api/workers", headers={"Authorization": f"Bearer {user_token}"})
        assert resp1.status_code == 403

        # Admin gets 200
        resp2 = client.get("/api/workers", headers={"Authorization": f"Bearer {admin_token}"})
        assert resp2.status_code == 200


# ---------------------------------------------------------------------------
# Validation and Rate Limiting
# ---------------------------------------------------------------------------
class TestSecurityAndValidation:
    def test_payload_size_limit(self, client: TestClient):
        # Create a user
        client.post("/api/auth/register", json={"email": "large@taskflow.local", "password": "securepassword123"})
        token = client.post("/api/auth/login", json={"email": "large@taskflow.local", "password": "securepassword123"}).json()["access_token"]
        
        # 70KB payload
        large_payload = {"data": "x" * 70000}
        
        resp = client.post(
            "/api/jobs",
            json={"job_type": "SUM_NUMBERS", "payload": large_payload},
            headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 422
        assert "Payload too large" in resp.json()["detail"]
