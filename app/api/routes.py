from math import ceil
import boto3
from time import sleep
import json
from datetime import datetime
from flask import (
    Blueprint, 
    jsonify, 
    request, 
    current_app
    )
from app.utils.helpers import (
    get_table_info,
    get_tables_in_db, 
    get_db_names, 
    get_table_id, 
    add_table_stream_dynamodb, 
    destructure_create_table_request,
    get_stream_arn,
    is_sql_injection,
    parse_source_arn,
    )

api = Blueprint('main', __name__)
global_boto3_session = None

@api.route("/databases", methods=["GET"])
def get_databases():
    try:
        client = current_app.get_ch_client()

        db_table_map = {}
        for db in get_db_names(client):
            db_table_map[db] = get_tables_in_db(client, db)

        return jsonify(db_table_map)
    except Exception as e:
        return jsonify({"Databases Route Error": str(e)}), 400
    
@api.route('/query', methods=["POST"])
def query():
    try:
        client = current_app.get_ch_client()
        query_string = request.json.get("query")
        result = client.query(query_string)  

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

        if not stream_name:
            return jsonify({'error': 'streamName is required'}), 400

        kinesis_client = global_boto3_session.client('kinesis')
        shard_iterator = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId='shardId-000000000000',
            ShardIteratorType='LATEST'
        )['ShardIterator']

        """
        Trying for 10 seconds to grab a kinesis stream record using kinesis client
        If event record exists, decode to JSON
        Use event record and clickhouse client to infer schema
        Return same event and inferrerd schema
        """
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

        if not isinstance(schema, list) or len(schema) == 0:
            return jsonify({"Schema Error": "Invalid schema format"}), 400

        for col in schema:
            if not isinstance(col, dict) or "name" not in col or "type" not in col:
                return jsonify({"Schema Error": "Invalid schema format"}), 400
        
        columns = ", ".join([f'{col["name"]} {col["type"]}' for col in schema])

        create_table_query = f"CREATE TABLE {database_name}.{table_name} "\
                              f"({columns}"\
                              f") ENGINE = MergeTree()"\
                              f" PRIMARY KEY {schema[0]["name"]}"
        
        query = create_table_query.strip()

        if is_sql_injection(query, True):
            return jsonify({"Error": "Possible dangerous query operation"})

        client.command(query)
        
        stream_arn = get_stream_arn(global_boto3_session, stream_name)
        table_id = get_table_id(client, table_name)
        add_table_stream_dynamodb(global_boto3_session, stream_arn, table_id)

        lambda_client = global_boto3_session.client('lambda')

        lambda_client.create_event_source_mapping(
            EventSourceArn=stream_arn,
            FunctionName='kinesis-to-clickhouse-dev',
            StartingPosition='LATEST',
            BatchSize=3
        )

        return jsonify({
            "success": True,
            "createTableQuery": query,
            "message": "Table created in Clickhouse. Lambda trigger added. Mapping added to dynamo",
            "tableUUID": table_id,
            "streamARN": stream_arn
        })
    except Exception as e:
        return jsonify({"Create Table Route Error": str(e)}), 400


@api.route('/sources', methods=['GET'])
def view_sources():
    client = current_app.get_ch_client()
    global global_boto3_session

    try:
        dynamo_client = global_boto3_session.resource("dynamodb")
        dynamo_table = dynamo_client.Table("stream_table_map")
        response = dynamo_table.scan()
        items = response['Items']
        
        data_sources = []
        for item in items:
            stream_type, stream_name = parse_source_arn(item['stream_id'])
            table_name, created_on = get_table_info(client, item['table_id'])
            created_on = f"{created_on:%m-%d-%Y}"
            data_sources.append({
                'streamName': stream_name,
                'streamType': stream_type,
                'tableName': table_name,
                'createdOn': created_on
            })

        return jsonify(data_sources)
    except Exception as e:
        return jsonify({"Sources Route Error": str(e)}), 400