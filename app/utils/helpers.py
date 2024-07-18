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
    print(f"request to destructure: {request.json}")
    # page = request.json.get("page")
    # Trying to get page parameter
    # If page is None, keep it the same
    # If page is not None ie '3', convert to int
    page = request.json.get("page")
    page_size = request.json.get('pageSize')
    offset = None
    print('abc2', page, page_size)
    if page and page_size:
        page = int(page)
        page_size = int(page_size)
        offset = (page - 1) * page_size
    
    return query_string, page, page_size, offset

def create_paginated_query(query_string, page_size, offset):
    paginated_query = f"{query_string} LIMIT {page_size} OFFSET {offset}"
    return paginated_query




# create-table route

def destructure_create_table_request(request):
    data = request.json
    stream_name = data["streamName"]
    table_name = data["tableName"]
    database_name = data.get('databaseName', 'default')
    schema = data["schema"]
    return stream_name, table_name, database_name, schema


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
