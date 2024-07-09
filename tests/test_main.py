import pytest
import docker
import time
from app.main import create_app
from clickhouse_connect import get_client
from tests.test_config import TEST_CONFIG


@pytest.fixture(scope="session")
def docker_container():
    client = docker.from_env()
    container = client.containers.run(
        "clickhouse/clickhouse-server",
        detach=True,
        ports={"8123/tcp": 8123, "9000/tcp": 9000},
        name="clickhouse-test",
    )
    time.sleep(10)  # Wait for ClickHouse to start
    yield container
    container.stop()
    container.remove()


@pytest.fixture(scope="session")
def ch_client(docker_container):
    for i in range(5):  # Try 5 times
        try:
            client = get_client(
                host=TEST_CONFIG["CH_HOST"],
                port=TEST_CONFIG["CH_PORT"],
                username=TEST_CONFIG["CH_USER"],
                password=TEST_CONFIG["CH_PASSWORD"],
            )
            # Test the connection
            client.query("SELECT 1 FROM system.one")
            return client  # If successful, return the client
        except Exception as e:
            print(f"Attempt {i+1} failed: {str(e)}")
            if i == 4:  # Last attempt
                raise
            time.sleep(2)

    raise Exception("Failed to connect to ClickHouse after 5 attempts")


@pytest.fixture(scope="session")
def app(ch_client):
    return create_app(config=TEST_CONFIG, client=ch_client)


@pytest.fixture
def client(app):
    return app.test_client()


class TestClickHouseIntegration:
    def test_get_databases(self, client):
        response = client.get("/api/databases")
        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, dict)
        assert "system" in data
        assert (
            "tables" in data["system"]
        ), "'tables' should be present in system database"
