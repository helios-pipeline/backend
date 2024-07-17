import boto3
from flask import (
    Blueprint, 
    jsonify, 
    request, 
    current_app
    )
from app.utils.helpers import (
    get_tables_in_db, 
    get_db_names, 
    get_table_id, 
    add_table_stream_dynamodb, 
    create_paginated_query, 
    destructure_query_request, 
    connect_to_stream, 
    destructure_create_table_request,
    get_stream_arn,
    validate_schema
    )

api = Blueprint('main', __name__)
global_boto3_session = None

@api.route("/databases", methods=["GET"])
def get_databases():
    try:
        client = current_app.get_ch_client()
        print(f"in database route, client, from current_app.get_ch_client(), is: {client}")

        db_table_map = {}
        for db in get_db_names(client):
            db_table_map[db] = get_tables_in_db(client, db)

        return jsonify(db_table_map)
    except Exception as e:
        return jsonify({"Databases Route Error": str(e)}), 400
    
@api.route("/query", methods=["POST"])
def query():
    try:
        client = current_app.get_ch_client()
        print(f"in query route, client, from current_app.get_ch_client(), is: {client}")
        
        query_string, page, page_size, offset = destructure_query_request(request)
        paginated_query = create_paginated_query(query_string, page_size, offset)

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
        return jsonify({"Query Route Error": str(e)}), 400

@api.route("/authenticate", methods=["POST"])
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
        return jsonify({"Authentication Route Error": str(e)}), 400

@api.route("/kinesis-sample", methods=["POST"])
def kinesis_sample():
    client = current_app.get_ch_client()
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
        
        
        connect_to_stream(client, kinesis_client, shard_iterator)

        return jsonify({"Unsuccessful": "could not find any records"})
      
    except Exception as e:
        return jsonify({"Kinesis Sample Route Error": str(e)}), 400

@api.route("/create-table", methods=["POST"])
def create_table():
    try:
        client = current_app.get_ch_client()

        if global_boto3_session is None:
            return jsonify({'Authentication Error': 'User had not been authenticated'}), 401
        
        stream_name, table_name, database_name, schema = destructure_create_table_request(request)

        validate_schema(schema)
        
        columns = ", ".join([f'{col["name"]} {col["type"]}' for col in schema])

        # Primary key is set to first column as default
        create_table_query = f"CREATE TABLE {database_name}.{table_name} "\
                              f"({columns}"\
                              f") ENGINE = MergeTree()"\
                              f" PRIMARY KEY {schema[0]["name"]}"
        
        query = create_table_query.strip()
        print('create table query: ', query)
        client.command(query)

        # TODO: Add Lambda Connection here to add a trigger for the Kinesis stream
        # Don't do it within other routes as it will stream events too early causing and error
        
        stream_arn = get_stream_arn(global_boto3_session, stream_name)
        table_id = get_table_id(client, table_name)
        add_table_stream_dynamodb(global_boto3_session, stream_arn, table_id)

        return jsonify({
            "success": True,
            "create_table_query": query,
            "message": "Table created in Clickhouse. TODO: Insert tableUUID and streamARN into dynamodb",
            "tableUUID": table_id,
            "streamARN": stream_arn
        })
    except Exception as e:
        return jsonify({"Create Table Route Error": str(e)}), 400
