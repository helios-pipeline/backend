#!/bin/bash
source scripts/development/global-vars.sh

sh scripts/development/kill-docker.sh

docker pull ${USERNAME}/helios-clickhouse-arm:dev

docker run -d --name helios-clickhouse-container -p 8123:8123 -p 8443:8443 -p 9000:9000 -p 9440:9440 ${USERNAME}/helios-clickhouse-arm:dev

sleep 5

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

