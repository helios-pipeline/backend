#!/bin/bash
set -e

until clickhouse-client --query "SELECT 1"; do
    echo "Waiting for ClickHouse server to be ready..."
    sleep 1
done

clickhouse-client -n <<-EOSQL
    CREATE TABLE default.events
    (
        user_id Int32,
        session_id UUID,
        event_type String,
        event_timestamp DateTime,
        page_url String,
        product_id Int32
    )
    ENGINE = MergeTree()
    ORDER BY (user_id, event_timestamp);

    CREATE TABLE default.pypi (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String
    )
    ENGINE = MergeTree
    PRIMARY KEY TIMESTAMP;
EOSQL

echo "ClickHouse initialization completed."

