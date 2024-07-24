import json
import random
import string
from unittest.mock import patch
from app.utils.helpers import is_sql_injection
import pytest
from app.main import create_app
from clickhouse_connect import get_client
from tests.test_config import TEST_CONFIG

# note in readme to install Clickhouse locally to run tests + make sure its running
# curl https://clickhouse.com/ | sh
# ./clickhouse server


@pytest.fixture(scope="session")
def ch_client():
    try:
        client = get_client(
            host=TEST_CONFIG["CH_HOST"],
            port=TEST_CONFIG["CH_PORT"],
            username=TEST_CONFIG["CH_USER"],
            password=TEST_CONFIG["CH_PASSWORD"],
        )
        result = client.query("SELECT 1")
        assert result.result_rows == [(1,)]
        yield client
    except Exception as e:
        pytest.fail(f"Failed to connect to ClickHouse: {e}")
    finally:
        client.close()


@pytest.fixture(scope="session")
def app(ch_client):
    app_instance = create_app(config=TEST_CONFIG, client=ch_client)
    yield app_instance


@pytest.fixture
def client(app):
    yield app.test_client()


class TestDatabasesRoute:
    """
    @api.route("/databases", methods=["GET"])
    db_table_map = {}
    for db in get_db_names(client):
        db_table_map[db] = get_tables_in_db(client, db)
    return jsonify(db_table_map)
    """

    def test_get_databases(self, client):
        response = client.get("/api/databases")
        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, dict)
        assert "system" in data

    def test_get_databases_error(self, client, monkeypatch):
        def mock_get_db_names(*args):
            raise Exception("Database error")

        monkeypatch.setattr("app.api.routes.get_db_names", mock_get_db_names)
        response = client.get("/api/databases")
        assert response.status_code == 400
        data = response.get_json()
        assert "Databases Route Error" in data


class TestQueryRoute:
    """
    @api.route('/query', methods=["POST"])
    response = {
        "metadata": {
            "query": query_string,
            "row_count": int(result.summary["read_rows"]),
            "column_names": result.column_names,
            "column_types": [t.base_type for t in result.column_types],
        },
        "data": data,
    }
    return jsonify(response)
    """

    def test_query_endpoint(self, client):
        mock_query = {"query": "SELECT 1"}
        response = client.post("/api/query", json=mock_query)
        assert response.status_code == 200
        data = response.get_json()
        assert "metadata" in data
        assert "data" in data

    def test_query_endpoint_error(self, client):
        mock_query = {"query": "INVALID SQL"}
        response = client.post("/api/query", json=mock_query)
        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data

    def test_query_endpoint_missing_query(self, client):
        response = client.post("/api/query", json={})
        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data


class TestAuthenticateRoute:
    """
    @api.route("/authenticate", methods=["POST"])
    return jsonify({
        "authenticated": True,
        "streamNames": stream_names
    })
    """

    @patch("boto3.Session")
    def test_authenticate_success(self, mock_boto3, client):
        mock_boto3.return_value.client.return_value.list_streams.return_value = {
            "StreamNames": ["stream1", "stream2"]
        }
        response = client.post(
            "/api/authenticate",
            json={"accessKey": "test_key", "secretKey": "test_secret"},
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["authenticated"] == True
        assert "streamNames" in data
        assert len(data["streamNames"]) == 2

    @patch("boto3.Session")
    def test_authenticate_failure(self, mock_boto3, client):
        mock_boto3.side_effect = Exception("Authentication failed")
        response = client.post(
            "/api/authenticate",
            json={"accessKey": "wrong_key", "secretKey": "wrong_secret"},
        )
        assert response.status_code == 400
        data = response.get_json()
        assert "Authentication Route Error" in data


class TestKinesisSampleRoute:
    """
    @api.route("/kinesis-sample", methods=["POST"])
    return jsonify({
        "sampleEvent": json.loads(record_data),
        "inferredSchema": schemaArray
    })
    """

    @patch("app.api.routes.global_boto3_session")
    def test_kinesis_sample_success(self, mock_global_session, client):
        mock_kinesis = mock_global_session.client.return_value
        mock_kinesis.get_records.return_value = {
            "Records": [{"Data": json.dumps({"event": "test"}).encode("utf-8")}]
        }

        response = client.post(
            "/api/kinesis-sample", json={"streamName": "test_stream"}
        )

        assert response.status_code == 200
        data = response.get_json()
        assert "sampleEvent" in data
        assert data["sampleEvent"] == {"event": "test"}

    @patch("app.api.routes.global_boto3_session")
    def test_kinesis_sample_no_records(self, mock_global_session, client):
        mock_kinesis = mock_global_session.client.return_value
        mock_kinesis.get_records.return_value = {"Records": []}

        response = client.post(
            "/api/kinesis-sample", json={"streamName": "test_stream"}
        )
        print(f"Response content: {response.get_data(as_text=True)}")

        assert response.status_code == 200
        data = response.get_json()
        assert "Unsuccessful" in data

    @patch("app.api.routes.global_boto3_session")
    def test_kinesis_sample_missing_stream_name(self, mock_global_session, client):
        response = client.post("/api/kinesis-sample", json={})
        print(f"Response content: {response.get_data(as_text=True)}")

        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data
        assert "streamName is required" in data["error"]


def generate_random_table_name(prefix="test_table_"):
    random_suffix = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=10)
    )
    return f"{prefix}{random_suffix}"


class TestCreateTableRoute:
    """
    @api.route("/create-table", methods=["POST"])
    return jsonify({
        "success": True,
        "create_table_query": query,
        "message": "Table created in Clickhouse. Lambda trigger added. Mapping added to dynamo",
        "tableUUID": table_id,
        "streamARN": stream_arn
    })
    """

    @patch("app.api.routes.global_boto3_session")
    @patch("app.api.routes.get_stream_arn")
    @patch("app.api.routes.get_table_id")
    @patch("app.api.routes.add_table_stream_dynamodb")
    def test_create_table_success(
        self,
        mock_add_table_stream,
        mock_get_table_id,
        mock_get_stream_arn,
        mock_global_session,
        client,
    ):

        mock_global_session.client.return_value.create_event_source_mapping.return_value = (
            {}
        )

        mock_get_stream_arn.return_value = (
            "arn:aws:kinesis:us-west-2:123456789012:stream/test_stream"
        )
        mock_get_table_id.return_value = "some-uuid"
        mock_add_table_stream.return_value = None

        mock_schema = [
            {"name": "id", "type": "Int32"},
            {"name": "name", "type": "String"},
        ]

        random_table_name = generate_random_table_name()

        response = client.post(
            "/api/create-table",
            json={
                "streamName": "test_stream",
                "tableName": random_table_name,
                "databaseName": "default",
                "schema": mock_schema,
            },
        )

        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] == True
        assert "create_table_query" in data
        assert "tableUUID" in data
        assert "streamARN" in data

    @patch("app.api.routes.global_boto3_session")
    @patch("app.api.routes.get_stream_arn")
    @patch("app.api.routes.get_table_id")
    @patch("app.api.routes.add_table_stream_dynamodb")
    def test_create_table_failure(
        self,
        mock_add_table_stream,
        mock_get_table_id,
        mock_get_stream_arn,
        mock_global_session,
        client,
    ):
        mock_global_session.client.return_value.create_event_source_mapping.side_effect = Exception(
            "AWS Lambda Error"
        )

        mock_get_stream_arn.return_value = (
            "arn:aws:kinesis:us-west-2:123456789012:stream/test_stream"
        )
        mock_get_table_id.return_value = "some-uuid"
        mock_add_table_stream.return_value = None

        mock_schema = [
            {"name": "id", "type": "Int32"},
            {"name": "name", "type": "String"},
        ]

        random_table_name = generate_random_table_name()

        response = client.post(
            "/api/create-table",
            json={
                "streamName": "test_stream",
                "tableName": random_table_name,
                "databaseName": "default",
                "schema": mock_schema,
            },
        )

        assert response.status_code == 400
        data = response.get_json()
        assert "Create Table Route Error" in data
        assert "AWS Lambda Error" in data["Create Table Route Error"]

    def test_is_sql_injection(self):
        assert is_sql_injection("SELECT * FROM table") == False
        assert is_sql_injection("DROP TABLE users") == True
        assert is_sql_injection("SELECT * FROM table; DROP TABLE users") == True

    def test_sql_injection_prevention(self, client):
        malicious_query = "SELECT * FROM users; DROP TABLE users"
        response = client.post("/api/query", json={"query": malicious_query})
        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data
        assert "Multi-statements are not allowed" in data["error"]

    @patch("app.api.routes.global_boto3_session", new=None)
    def test_create_table_unauthenticated(self, client):
        mock_schema = [{"name": "id", "type": "Int32"}]
        random_table_name = generate_random_table_name()

        response = client.post(
            "/api/create-table",
            json={
                "streamName": "test_stream",
                "tableName": random_table_name,
                "databaseName": "default",
                "schema": mock_schema,
            },
        )

        assert response.status_code == 401
        data = response.get_json()
        assert "Authentication Error" in data
        assert "User had not been authenticated" in data["Authentication Error"]
