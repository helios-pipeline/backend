import base64
import logging
import json
import os
from time import sleep
import uuid
import boto3
from boto3.dynamodb.conditions import Key

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
    app.config["CH_HOST"] = os.getenv("CH_HOST", "ec2-13-57-48-113.us-west-1.compute.amazonaws.com")
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
        
    @app.route("/api/query", methods=["POST"])
    def query():
        try:
            query_string = request.json.get("query")
            page = int(request.json.get("page", 1))
            page_size = int(request.json.get("pageSize", 10))

            offset = (page - 1) * page_size

            paginated_query = f"{query_string} LIMIT {page_size} OFFSET {offset}"

            result = client.query(paginated_query)

            total_count_query = f"SELECT count(*) as total FROM ({query_string})"
            total_count = client.query(total_count_query).first_row[0]

            data = [*result.named_results()]
            response = {
                "metadata": {
                    "query": query_string,
                    "row_count": int(result.summary["read_rows"]),
                    "column_names": result.column_names,
                    "column_types": [t.base_type for t in result.column_types],
                    "total_count": total_count,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (total_count + page_size - 1) // page_size
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

            global_boto3_session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name='us-west-1'
            )
            kinesis_client = global_boto3_session.client('kinesis')
            
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
                print('Backend route Records: ', records)

                if records['Records']:
                    record_data = records['Records'][0]['Data'].decode('utf-8')
                    
                    client.command("SET schema_inference_make_columns_nullable = 0;")
                    client.command("SET input_format_null_as_default = 0;")
                    res = client.query(f"DESC format(JSONEachRow, '{record_data}');")

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
    
    @app.route("/api/create-table", methods=["POST"])
    def create_table():
        try:
            if global_boto3_session is None:
                return jsonify({'Authentication Error': 'User had not been authenticated'}), 401
            
            data = request.json
            
            stream_name = data["streamName"]
            table_name = data["tableName"]
            database_name = data.get('databaseName', 'default')
            schema = data["schema"]

            if not isinstance(schema, list) or len(schema) == 0:
                return jsonify({"error": "Invalid schema format"}), 400

            for col in schema:
                if not isinstance(col, dict) or "name" not in col or "type" not in col:
                    return jsonify({"error": "Invalid schema format"}), 400
            
            columns = ", ".join([f'{col["name"]} {col["type"]}' for col in schema])

            # Primary key is set to first column as default
            create_table_query = f"CREATE TABLE {database_name}.{table_name} "\
                                 f"({columns}"\
                                 f") ENGINE = MergeTree()"\
                                 f" PRIMARY KEY {schema[0]["name"]}"
            
            query = create_table_query.strip()

            print('create table query: ', query)

            client.command(query)
            
            def get_table_id(table_name):
                res = client.query(f"""
                    SELECT uuid
                    FROM system.tables
                    WHERE database = 'default'
                    AND name = '{table_name}'
                    """)
                return res.first_row[0]
            
            def add_table_stream_dynamodb(stream_arn, ch_table_id):
                dynamo_client = global_boto3_session.resource('dynamodb')
                dynamo_table = dynamo_client.Table('tables_streams')

                response = dynamo_table.query(
                    KeyConditionExpression=Key('stream_id').eq(stream_arn)
                )
                
                # TODO: this delete if exists is not working
                if len(response['Items']) == 1:
                    # If the item exists, delete it first
                    dynamo_table.delete_item(
                        Key={
                            "stream_id": stream_arn,
                            "table_id": response['Items'][0]['table_id']
                        }
                    )
                    print(f"Existing entry for stream_id {stream_arn} deleted.")

                dynamo_table.put_item(
                    Item={
                        "stream_id": stream_arn,
                        "table_id": str(ch_table_id)
                    }
                )

            kinesis_client = global_boto3_session.client('kinesis')
            stream_description = kinesis_client.describe_stream(StreamName=stream_name)
            stream_arn = stream_description['StreamDescription']['StreamARN']

            table_id = get_table_id(table_name)
            add_table_stream_dynamodb(stream_arn, table_id)

            return jsonify({
                "success": True,
                "create_table_query": query,
                "message": "Table created in Clickhouse. TODO: Insert tableUUID and streamARN into dynamodb",
                "tableUUID": table_id,
                "streamARN": stream_arn
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    return app


if __name__ == "__main__":
    app = create_app()
    app.run()
