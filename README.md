## Overview

Helios' backend is comprised of a Flask server connected to a ClickHouse database through the ClickhHouse Connect database driver. It utilizes the Amazon Web Service's Software Development Kit (SDK) to communicate with select parts of Helios' infrastructure, including Lambda and DynamoDB.

<details>
  <summary><strong>⚠️ Warning: Running Locally</strong></summary>
  <div>

Please note that running this backend application locally is not recommended, as it is designed to work with specific AWS infrastructure. To provision the complete Helios system and utilize this backend effectively, please refer to the [Helios Deploy](https://github.com/helios-pipeline/deploy) documentation.

  </div>
</details>

## Backend API

### Databases Endpoint

- **URL:** `/api/databases`
- **Method:** GET
- **Description:** Retrieves a list of databases and their tables.
- **Response:**
  - **Status Code:** 200 OK
  - **Body:** JSON object with database names as keys and arrays of table names as values.

**Example Response:**

```json
{
  "default": ["events", "users"],
  "analytics": ["daily_metrics", "user_activity"]
}
```

### Query Endpoint

- **URL**: /api/query
- **Method**: POST
- **Description**: Executes a SQL query on the ClickHouse database.
- **Request Body**: JSON object with a `query` key containing the SQL query string.
- **Response**:
  - **Status Code**: 200 OK
  - **Body**: JSON object with query results and metadata.

**Example Request:**

```json
{
  "query": "SELECT COUNT(*) FROM events WHERE event_type = 'login'"
}
```

**Example Response:**

```json
{
  "metadata": {
    "query": "SELECT COUNT(*) FROM events WHERE event_type = 'login'",
    "row_count": 1,
    "column_names": ["count"],
    "column_types": ["UInt64"]
  },
  "data": [{ "count": 15234 }]
}
```

### Authenticate Endpoint

- **URL**: /api/authenticate
- **Method**: POST
- **Description**: Authenticates AWS credentials for Kinesis access.
- **Request Body**: JSON object with `accessKey` and `secretKey`.
- **Response**:-
  - **Status Code**: 200 OK
  - **Body**: JSON object with authentication status and available stream names.

**Example Request:**

```json
{
  "accessKey": "AKIAIOSFODNN7EXAMPLE",
  "secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
}
```

**Example Response:**

```json
{
  "authenticated": true,
  "streamNames": ["user_activity", "purchase_events"]
}
```

### Kinesis Sample Endpoint

- **URL**: /api/kinesis-sample
- **Method**: POST
- **Description**: Retrieves a sample event from a specified Kinesis stream and infers the schema.
- **Request Body**: JSON object with a `streamName` key.
- **Response**:-
  - **Status Code**: 200 OK
  - **Body**: JSON object with a sample event and inferred schema.

**Example Request:**

```json
{
  "streamName": "user_activity"
}
```

**Example Response:**

```json
{
  "sampleEvent": {
    "user_id": "12345",
    "event_type": "login",
    "timestamp": "2023-04-15T14:30:00Z"
  },
  "inferredSchema": [
    { "name": "user_id", "type": "String" },
    { "name": "event_type", "type": "String" },
    { "name": "timestamp", "type": "DateTime" }
  ]
}
```

### Create Table Endpoint

- **URL**: /api/create-table
- **Method**: POST
- **Description**: Creates a new table in ClickHouse and sets up a Kinesis stream mapping.
- **Request Body**: JSON object with table details and schema.
- **Response**:-
  - **Status Code**: 200 OK
  - **Body**: JSON object with creation status and details.

**Example Request:**

```json
{
  "streamName": "user_activity",
  "tableName": "user_events",
  "databaseName": "default",
  "schema": [
    { "name": "user_id", "type": "String" },
    { "name": "event_type", "type": "String" },
    { "name": "timestamp", "type": "DateTime" }
  ]
}
```

**Example Response:**

```json
{
  "success": true,
  "createTableQuery": "CREATE TABLE default.user_events (user_id String, event_type String, timestamp DateTime) ENGINE = MergeTree() ORDER BY timestamp",
  "message": "Table created in Clickhouse. Lambda trigger added. Mapping added to dynamo",
  "tableUUID": "550e8400-e29b-41d4-a716-446655440000",
  "streamARN": "arn:aws:kinesis:us-west-2:123456789012:stream/user_activity"
}
```

### Sources Endpoint

- **URL**: /api/sources
- **Method**: GET
- **Description**: Retrieves a list of all configured data sources (Kinesis streams) and their associated ClickHouse tables.
- **Response**:
  - **Status Code**: 200 OK
  - **Body**: Array of JSON objects, each representing a data source.

**Example Response:**

```json
[
  {
    "streamName": "user_activity",
    "streamType": "kinesis",
    "tableName": "user_events",
    "createdOn": "04-15-2023"
  },
  {
    "streamName": "purchase_events",
    "streamType": "kinesis",
    "tableName": "purchases",
    "createdOn": "04-10-2023"
  }
]
```
