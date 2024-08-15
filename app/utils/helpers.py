import json
import re
from time import sleep

from boto3.dynamodb.conditions import Key
from flask import jsonify


# get-databases route
def get_db_names(client):
    return [db["name"] for db in client.query("SHOW DATABASES").named_results()]


def get_tables_in_db(client, db_name):
    return [
        table["name"]
        for table in client.query(f"SHOW TABLES FROM {db_name}").named_results()
    ]


# query route
def destructure_query_request(request):
    query_string = request.json.get("query")
    page = request.json.get("page")
    page_size = request.json.get("pageSize")
    offset = None
    
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
    database_name = data.get("databaseName", "default")
    schema = data["schema"]
    return stream_name, table_name, database_name, schema


def get_stream_arn(global_boto3_session, stream_name):
    kinesis_client = global_boto3_session.client("kinesis")
    stream_description = kinesis_client.describe_stream(StreamName=stream_name)
    stream_arn = stream_description["StreamDescription"]["StreamARN"]
    return stream_arn


def get_table_id(client, table_name):
    res = client.query(
        f"""
        SELECT uuid
        FROM system.tables
        WHERE database = 'default'
        AND name = '{table_name}'
        """
    )
    return res.first_row[0]


def get_table_info(client, table_id):
    res = client.query(
        f"""
        SELECT name, metadata_modification_time
        FROM system.tables
        WHERE database = 'default'
        AND toString(uuid) = '{table_id}'
        """
    )
    return res.first_row


def parse_source_arn(stream_id):
    stream_type = parse_source_arn_type(stream_id)
    stream_name = parse_source_arn_name(stream_type, stream_id)
    return stream_type, stream_name


def parse_source_arn_type(stream_id):
    return re.search(r"arn:aws:(\w+):", stream_id).group(1)


def parse_source_arn_name(stream_type, stream_id):
    if stream_type == "kinesis":
        return stream_id.split("/")[-1]
    elif stream_type == "s3":
        return re.search(r":::(.+)", stream_id).group(0)


def is_sql_injection(query, create_table=False):
    dangerous_keywords = [
        "DROP",
        "DELETE",
        "TRUNCATE",
        "ALTER",
        "INSERT",
        "EXEC",
        "EXECUTE",
        "UPDATE",
        "UNION",
        "--",
    ]

    if create_table:
        dangerous_keywords.append(";")

    pattern = (
        r"\b(" + "|".join(re.escape(keyword) for keyword in dangerous_keywords) + r")\b"
    )

    if re.search(pattern, query, re.IGNORECASE):
        return True
    return False


def add_table_stream_dynamodb(session, stream_arn, ch_table_id):
    dynamo_client = session.resource("dynamodb")
    dynamo_table = dynamo_client.Table(
        "stream_table_map"
    )

    response = dynamo_table.query(
        KeyConditionExpression=Key("stream_id").eq(stream_arn)
    )

    if len(response["Items"]) == 1:
        # If the item exists, delete it first
        dynamo_table.delete_item(
            Key={"stream_id": stream_arn, "table_id": response["Items"][0]["table_id"]}
        )
        print(f"Existing entry for stream_id {stream_arn} deleted.")

    dynamo_table.put_item(Item={"stream_id": stream_arn, "table_id": str(ch_table_id)})
