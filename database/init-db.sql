CREATE DATABASE IF NOT EXISTS my_database;

CREATE TABLE my_database.events
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

