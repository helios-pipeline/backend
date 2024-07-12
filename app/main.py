import base64
import logging
import json
import os
from time import sleep
import boto3

import clickhouse_connect
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS

global_boto3_session = None

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

    @app.route("/api/authenticate", methods=["POST"])
    def authenticate():
        global global_boto3_session
        
        try:
            data = request.json
            access_key = data.get("accessKey")
            secret_key = data.get("secretKey")

            # Use boto3 to validate credentials and list Kinesis streams
            global_boto3_session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            kinesis_client = global_boto3_session.client('kinesis')
            
            # List Kinesis streams
            response = kinesis_client.list_streams()
            stream_names = response.get('StreamNames', [])

            return jsonify({
                "authenticated": True,
                "streamNames": stream_names
            })
        except Exception as e:
            return jsonify({"Authentication Error": str(e)}), 400

    @app.route("/api/kinesis-sample", methods=["POST"])
    def kinesis_sample():
        try:
            if global_boto3_session is None:
                return jsonify({'Authentication Error': 'User had not been authenticated'}), 401

            data = request.json
            stream_name = data.get("streamName")

            # Use boto3 session to access Kinesis 
            # Using kinesis client to get specific shard interator
            kinesis_client = global_boto3_session.client('kinesis')
            shard_iterator = kinesis_client.get_shard_iterator(
                StreamName=stream_name,
                ShardId='shardId-000000000000',  # Assume single shard for simplicity
                ShardIteratorType='LATEST'
            )['ShardIterator']
            
            # Trying for 10 seconds to grab a kinesis stream record using kinesis client
            # If event record exists, decode to JSON
            # Use event record and clickhouse client to infer schema
            # Return same event and inferrerd schema
            seconds_to_try = 5
            times_per_second = 3
            count = 0
            while count < times_per_second * seconds_to_try:
                records = kinesis_client.get_records(
                  ShardIterator=shard_iterator,
                  Limit=1
                )
                if records['Records']:
                    record_data = records['Records'][0]['Data'].decode('utf-8')
                    
                    res = client.query(f"DESC format(JSONEachRow, '{record_data}');")
                    jsonify({"1": res.result_rows})

                    schemaArray = []
                    for row in res.result_rows:
                        schema = {
                            'name': row[0],
                            'type': row[1]
                        }
                        schemaArray.append(schema)

                    return jsonify({"sampleEvent": json.loads(record_data), "inferredSchema": schemaArray})

                count += 1
                sleep(1/times_per_second)
            return jsonify({"Unsucessful": "could not find any records"})
          
        except Exception as e:
            return jsonify({"Kinesis Sample Route Error": str(e)}), 400

    return app


if __name__ == "__main__":
    app = create_app()
    app.run()
