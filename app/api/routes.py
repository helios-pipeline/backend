from math import ceil
import boto3
import asyncio
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
    fetch_openai_output,
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
"""
receive from frontend:
- how many per page (10 for now)
- offset (page number)
- query

Input: query, offset, page num
Output: 1000 rows

Assumption:
- No overfetching(request from frontend every time press next or prev)



Examples:
table has 100,000 rows

request 1: select * from table, {limit: 100}
request 2: select * from table limit 300

===
OFFSET EXAMPLES:
rows_per_page = 10, page_number = 1
query = SELECT * FROM table OFFSET 0 LIMIT 10

rows_per_page = 10, page_number = 2
query = SELECT * FROM table OFFSET 10 LIMIT 10

rows_per_page = 10, page_number = 3
query = SELECT * FROM table OFFSET 20 LIMIT 10

rows_per_page = 10, page_number = 3, query = SELECT * FROM table LIMIT 100
<logic>
- extract limit number from original query
- use that to determine total rows?
<logic>
query = SELECT * FROM table OFFSET 20 LIMIT 10
===
COUNT QUERY EXAMPLES:
select * from table limit 100


PSEUDOCODE FOR CONSTRUCTING QUERY:
query_string = request.json.get("query")
rows_per_page = request.json.get("rowsPerPage")
offset = (request.json.get("pageNumber") - 1) * rows_per_page
limit = request.json.get("limit")

- extract limit_number from query string
  - if no limit, set limit to infinity
- get query_string_with_no_limit_and_offset
- query = f"query_string_with_no_limit_and_offset LIMIT {min(limit_number, _)} OFFSET {if offset_number then offset + offset_number else offset}"

-------------
import re

def extract_limit_and_offset(query):
    NO_LIMIT = float("inf")
    limit_match = re.search(r'\bLIMIT\s+(\d+)(?!\s*,)', query, re.IGNORECASE)
    offset_match = re.search(r'\bOFFSET\s+(\d+)(?!\s*,)', query, re.IGNORECASE)
    
    limit = int(limit_match.group(1)) if limit_match else NO_LIMIT
    offset = int(offset_match.group(1)) if offset_match else 0
    
    return limit, offset


@api.route('/paginate, methods=["POST"]')
async def paginate():
  try:
    client = current_app.get_ch_client()
    query = request.json.get("query")
    rows_per_page = request.json.get("rowsPerPage")
    current_page = request.json.get("pageNumber")
    offset = (current_page - 1) * rows_per_page
    
    query_limit, query_offset = extract_limit_and_offset(query)
    
    cleaned_query = re.sub(r'\s+(LIMIT\s+\d+)?\s*(OFFSET\s+\d+)?\s*$', '', query, flags=re.IGNORECASE).strip()

    paginated_query = f"cleaned_query LIMIT {min(rows_per_page, query_limit)} OFFSET {offset + query_offset}"
    
    count_query = f"SELECT COUNT(*) FROM ({query})"

    paginated_result = client.query(paginated_query)
    row_count = client.query(count_query)
    paginated_result, row_count = await asyncio.gather(
            client.query(paginated_query),
            client.query(count_query)
        )

    total_pages = ceil(row_count / rows_per_page)

    data = [*paginated_result.named_results()]

    return jsonify({
        "metadata": {
          "query": query,
          "columnNames": paginated_result.column_names,
          "columnTypes": [t.base_type for t in paginated_result.column_types],
        },
        "data": data,
        'pagination': {
          'totalItems': total_items,
          'totalPages': total_pages,
          "nextPage": f"/api/paginate?pageNumber={pageNumber + 1}&rowsPerPage=10",
          "prevPage": "/api/paginate?pageNumber=1&rowsPerPage=10"
        }
    })
  except Exception as e:
    return jsonify({"error": str(e)}), 400

    
FRONTEND EXAMPLE
await data = axios.post('/api/paginate', {
  rowsPerPage: 10,
  query: "SELECT * FROM table",
  pageNumber: 1,
})

TODO:
Branch: feat/server-side-pagination

commit 1

"""
@api.route('/query', methods=["POST"])
def query():
    try:
        client = current_app.get_ch_client()
        query_string = request.json.get("query")
        result = client.query(query_string)  

        data = [*result.named_results()]
        rows_count = len(data)

        response = {
            "metadata": {
                "query": query_string,
                "row_count": rows_count,
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

        print(query)
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

@api.route('/api-key', methods=["GET"])
def get_api_key():
    api_key = current_app.config.get("CHAT_GPT_API_KEY")
    if not api_key:
        return jsonify({"error": "ChatGPT API is not configured"}), 503
    
    return jsonify({"api_key": api_key})

@api.route('/api-response', methods=["POST"])
def view_api_output():
    api_key = current_app.config.get("CHAT_GPT_API_KEY")
    try:
        prompt = request.json.get("prompt")
        if not prompt:
            return jsonify({"error": "No prompt provided"}), 400
        
        text = fetch_openai_output(prompt, api_key)
        if text is None:
            return jsonify({"error": "Failed to get response from OpenAI"}), 500

        return jsonify({"response": text})
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred"}), 500