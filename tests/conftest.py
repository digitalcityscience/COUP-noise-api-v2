import pytest
from fastapi.testclient import TestClient

from noise_api.api.main import app


@pytest.fixture
def unauthorized_api_test_client():
    yield TestClient(app)


class MockCache:
    def __init__(self, **kwargs):
        ...

    def get(self, *args, **kwargs):
        ...

    def put(self, *args, **kwargs):
        ...

    def delete(self, *args, **kwargs):
        ...


@pytest.fixture(autouse=True)
def mock_cache(monkeypatch):
    monkeypatch.setattr("noise_api.api.endpoints.cache", MockCache())
