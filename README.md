# Poetry Installation

- [Install pipx](https://pipx.pypa.io/stable/installation/)
- Install poetry: `pipx install poetry`
- Activate virutal env: `poetry shell`
- Initialize Poetry project: `poetry install`

# Flask Route Documentation

## Kinesis Connector

### Setup
- Use the example requests provided and send them to routes in order
- Make sure to use team4 secret keys

### POST /api/authenticate

#### Authenticates the user with AWS credentials.

Request
```json
{
  "secretKey": "YOUR_AWS_SECRET_KEY",
  "accessKey": "YOUR_AWS_ACCESS_KEY"
}
```

Response
```json
{
  "authenticated": true,
  "streamNames": [
    "MyKinesisDataStream"
  ]
}
```

### POST /api/kinesis-sample

#### Retrieves a sample event from the specified Kinesis stream and infers its schema.

Request
```json
{
  "streamName": "MyKinesisDataStream"
}
```

Response
```json
{
  "inferredSchema": [
    {
      "name": "user_id",
      "type": "Nullable(Int64)"
    },
    {
      "name": "session_id",
      "type": "Nullable(String)"
    },
  ],
  "sampleEvent": {
    "event_timestamp": 1,
    "event_type": "purchase",
    "page_url": "https://www.example.com/",
    "product_id": 84,
    "session_id": "af29b7b3-b1a4-4562-84ae-3447b1b2bcd8",
    "user_id": 426
  }
}
```

- `inferredSchema` (array of objects): Inferred schema of the sample event
  - `name` (string): Column name
  - `type` (string): ClickHouse data type
- `sampleEvent` (object): A sample event from the Kinesis stream

### POST /api/create-table

#### Sends request to create table in clickhouse

#### Note
- TODO: Need to add streamARN and tableUUID to DynamoDB

Request
```json
{
  "streamName": "MyKinesisDataStream",
  "tableName": "example_table",
  "schema": [
      {"name": "column_name_1", "type": "String"},
      {"name": "column_name_2", "type": "Int32"},
      {"name": "columns_name_3", "type": "Boolean"}
  ]
}
```

- `streamName` (string): Name of the Kinesis stream
- `tableName` (string): Name for the new ClickHouse table
- `schema` (array of objects): Schema definition for the table
  - `name` (string): Column name
  - `type` (string): ClickHouse data type

Response
```json
{
  "create_table_query": "CREATE TABLE default.example_table(column_name_1 String, column_name_2 Int32, columns_name_3 Boolean) ENGINE = MergeTree() PRIMARY KEY column_name_1",
  "message": "Table created in ClickHouse. TODO: Insert tableUUID and streamARN into dynamodb",
  "streamARN": "arn:aws:kinesis:us-west-1:ACCOUNT_ID:stream/MyKinesisDataStream",
  "success": true,
  "tableUUID": "e2cc2c0e-2463-4d04-845f-58fa4d7f4df0"
}
```

- `create_table_query` (string): The ClickHouse query used to create the table
- `message` (string): Status message
- `streamARN` (string): ARN of the Kinesis stream
- `success` (boolean): Operation status
- `tableUUID` (string): Unique identifier for the created table