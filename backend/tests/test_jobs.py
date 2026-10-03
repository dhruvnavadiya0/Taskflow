"""Tests for the Jobs API endpoints — Phase 1 + Phase 2."""

from unittest.mock import patch

from fastapi.testclient import TestClient


class TestCreateJob:
    def test_create_sum_numbers_job(self, admin_client: TestClient):
        """POST /api/jobs should create a PENDING job and return its metadata."""
        with patch("app.api.jobs.job_queue") as mock_queue:
            response = admin_client.post("/api/jobs", json={
                "job_type": "SUM_NUMBERS",
                "priority": "HIGH",
                "payload": {"numbers": [5, 10, 15]},
            })

        assert response.status_code == 201
        data = response.json()
        assert data["job_type"] == "SUM_NUMBERS"
        assert data["status"] == "PENDING"
        assert data["priority"] == "HIGH"
        assert "id" in data
        mock_queue.enqueue_priority.assert_called_once_with(data["id"], "HIGH")

    def test_create_text_length_job(self, admin_client: TestClient):
        with patch("app.api.jobs.job_queue"):
            response = admin_client.post("/api/jobs", json={
                "job_type": "TEXT_LENGTH",
                "payload": {"text": "hello"},
            })

        assert response.status_code == 201
        assert response.json()["job_type"] == "TEXT_LENGTH"
        assert response.json()["priority"] == "NORMAL"  # default

    def test_create_job_with_empty_type_returns_422(self, admin_client: TestClient):
        response = admin_client.post("/api/jobs", json={
            "job_type": "",
            "payload": {},
        })
        assert response.status_code == 422

    def test_create_job_queue_failure_returns_503(self, admin_client: TestClient):
        """If Redis enqueue fails, the API should return 503 and mark the job FAILED."""
        with patch("app.api.jobs.job_queue") as mock_queue:
            mock_queue.enqueue_priority.side_effect = Exception("Redis down")
            response = admin_client.post("/api/jobs", json={
                "job_type": "SUM_NUMBERS",
                "payload": {"numbers": [1]},
            })

        assert response.status_code == 503
        assert "enqueue" in response.json()["detail"].lower()

    def test_create_job_with_max_retries(self, admin_client: TestClient):
        """Phase 2: job creation should accept max_retries."""
        with patch("app.api.jobs.job_queue"):
            response = admin_client.post("/api/jobs", json={
                "job_type": "SUM_NUMBERS",
                "payload": {"numbers": [1]},
                "max_retries": 5,
            })

        assert response.status_code == 201

    def test_create_job_max_retries_negative_returns_422(self, admin_client: TestClient):
        """Phase 2: negative max_retries should be rejected."""
        response = admin_client.post("/api/jobs", json={
            "job_type": "SUM_NUMBERS",
            "payload": {"numbers": [1]},
            "max_retries": -1,
        })
        assert response.status_code == 422

    def test_create_job_max_retries_too_high_returns_422(self, admin_client: TestClient):
        """Phase 2: max_retries above limit should be rejected."""
        response = admin_client.post("/api/jobs", json={
            "job_type": "SUM_NUMBERS",
            "payload": {"numbers": [1]},
            "max_retries": 999,
        })
        assert response.status_code == 422


class TestGetJob:
    def test_get_existing_job(self, admin_client: TestClient):
        """GET /api/jobs/{id} should return the full job details."""
        with patch("app.api.jobs.job_queue"):
            create_resp = admin_client.post("/api/jobs", json={
                "job_type": "SUM_NUMBERS",
                "payload": {"numbers": [1, 2]},
            })
        job_id = create_resp.json()["id"]

        response = admin_client.get(f"/api/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == job_id
        assert data["payload"] == {"numbers": [1, 2]}
        assert data["status"] == "PENDING"
        assert data["created_at"] is not None
        # Phase 2 fields present
        assert data["retry_count"] == 0
        assert data["max_retries"] == 3

    def test_get_nonexistent_job_returns_404(self, admin_client: TestClient):
        response = admin_client.get("/api/jobs/nonexistent-id-12345")
        assert response.status_code == 404


class TestListJobs:
    def test_list_empty(self, admin_client: TestClient):
        response = admin_client.get("/api/jobs")
        assert response.status_code == 200
        data = response.json()
        assert data["jobs"] == []
        assert data["total"] == 0

    def test_list_with_jobs(self, admin_client: TestClient):
        with patch("app.api.jobs.job_queue"):
            for i in range(3):
                admin_client.post("/api/jobs", json={
                    "job_type": "SUM_NUMBERS",
                    "payload": {"numbers": [i]},
                })

        response = admin_client.get("/api/jobs")
        data = response.json()
        assert data["total"] == 3
        assert len(data["jobs"]) == 3

    def test_list_pagination(self, admin_client: TestClient):
        with patch("app.api.jobs.job_queue"):
            for i in range(5):
                admin_client.post("/api/jobs", json={
                    "job_type": "SUM_NUMBERS",
                    "payload": {"numbers": [i]},
                })

        response = admin_client.get("/api/jobs?page=1&limit=2")
        data = response.json()
        assert data["total"] == 5
        assert len(data["jobs"]) == 2
        assert data["page"] == 1
        assert data["limit"] == 2


class TestHealthAndRoot:
    def test_health(self, admin_client: TestClient):
        response = admin_client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}

    def test_root(self, admin_client: TestClient):
        response = admin_client.get("/")
        assert response.status_code == 200
        assert "TaskFlow" in response.json()["message"]
