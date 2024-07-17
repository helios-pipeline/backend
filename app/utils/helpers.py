import json
from flask import jsonify
from time import sleep
from boto3.dynamodb.conditions import Key

# get-databases route
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


# query route
def destructure_query_request(request):
    query_string = request.json.get("query")
    page = int(request.json.get("page", 1))
    page_size = int(request.json.get("pageSize", 10))
    offset = (page - 1) * page_size
    return query_string, page, page_size, offset

def create_paginated_query(query_string, page_size, offset):
    paginated_query = f"{query_string} LIMIT {page_size} OFFSET {offset}"
    return paginated_query


# kinesis-sample route 
def connect_to_stream(client, kinesis_client, shard_iterator):
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


# create-table route

def destructure_create_table_request(request):
    data = request.json
    stream_name = data["streamName"]
    table_name = data["tableName"]
    database_name = data.get('databaseName', 'default')
    schema = data["schema"]
    return stream_name, table_name, database_name, schema

def validate_schema(schema):
    if not isinstance(schema, list) or len(schema) == 0:
        return jsonify({"Schema Error": "Invalid schema format"}), 400

    for col in schema:
        if not isinstance(col, dict) or "name" not in col or "type" not in col:
            return jsonify({"Schema Error": "Invalid schema format"}), 400

def get_stream_arn(global_boto3_session, stream_name):
    kinesis_client = global_boto3_session.client('kinesis')
    stream_description = kinesis_client.describe_stream(StreamName=stream_name)
    stream_arn = stream_description['StreamDescription']['StreamARN']
    return stream_arn

def get_table_id(client, table_name):
    res = client.query(f"""
        SELECT uuid
        FROM system.tables
        WHERE database = 'default'
        AND name = '{table_name}'
        """)
    return res.first_row[0]
        

def add_table_stream_dynamodb(session, stream_arn, ch_table_id):
    dynamo_client = session.resource('dynamodb')
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
