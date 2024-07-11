#!/bin/bash
. database/scripts/global-vars.sh

sh database/scripts/kill-docker.sh

# Pull the Docker image
docker pull ${USERNAME}/helios-clickhouse-amd:dev

# Run the Docker container
docker run -d --name helios-clickhouse-container -p 8123:8123 -p 8443:8443 -p 9000:9000 -p 9440:9440 ${USERNAME}/helios-clickhouse-amd:dev

# Wait a few seconds for the container to start up
sleep 5

# Execute the SQL command
docker exec -i helios-clickhouse-container clickhouse-client <<-EOF
INSERT INTO default.events (user_id, session_id, event_type, event_timestamp, page_url, product_id) VALUES
(12345, generateUUIDv4(), 'click', now(), 'https://example.com', 67890),
(6789, generateUUIDv4(), 'view', now(), 'http://example.com/items', 87891),
(12345, generateUUIDv4(), 'purchase', now(), 'https://example.com', 73489),
(23452, generateUUIDv4(), 'click', now(), 'https://example.com', 30423),
(13489, generateUUIDv4(), 'view', now(), 'https://example.com', 23789);
EOF

docker exec -i helios-clickhouse-container clickhouse-client <<-EOF
INSERT INTO pypi
SELECT TIMESTAMP, COUNTRY_CODE, URL, PROJECT
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');
EOF
# For inserting data into pypi from s3
# CREATE TABLE pypi (
#     TIMESTAMP DateTime,
#     COUNTRY_CODE String,
#     URL String,
#     PROJECT String
# )
# ENGINE = MergeTree
# PRIMARY KEY TIMESTAMP;

# INSERT INTO pypi
#     SELECT TIMESTAMP, COUNTRY_CODE, URL, PROJECT
#     FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');