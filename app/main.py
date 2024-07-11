import logging
import os

import clickhouse_connect
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS


def create_app(config=None, client=None):
    print(f"Creating app with config: {config}")
    app = Flask(__name__)
    CORS(app)

    load_dotenv()
    app.config["CH_HOST"] = os.getenv("CH_HOST", "localhost")
    app.config["CH_PORT"] = int(os.getenv("CH_PORT", 8123))
    app.config["CH_USER"] = os.getenv("CH_USER", "default")
    app.config["CH_PASSWORD"] = os.getenv("CH_PASSWORD", "")

    if config:
        print(f"Updating config: {config}")
        app.config.update(config)

    if client is None:
        print("Creating new ClickHouse client")
        client = clickhouse_connect.get_client(
            host=app.config["CH_HOST"],
            port=app.config["CH_PORT"],
            username=app.config["CH_USER"],
            password=app.config["CH_PASSWORD"],
        )

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    @app.route("/")
    @app.route("/api/databases", methods=["GET"])
    def get_databases():
        try:

            def get_db_names(client):
                return [
                    db["name"] for db in client.query("SHOW DATABASES").named_results()
                ]

            def get_tables_in_db(client, db_name):
                return [
                    table["name"]
                    for table in client.query(
                        f"SHOW TABLES FROM {db_name}"
                    ).named_results()
                ]

            db_table_map = {}
            for db in get_db_names(client):
                db_table_map[db] = get_tables_in_db(client, db)

            return jsonify(db_table_map)
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    # change from /api/query to /api/select?
    @app.route("/api/query", methods=["POST"])
    def query():
        try:
            query_string = request.json.get("query")
            result = client.query(query_string)  # returns a QueryReult Object

            data = [*result.named_results()]
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
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    return app


if __name__ == "__main__":
    app = create_app()
    app.run()
