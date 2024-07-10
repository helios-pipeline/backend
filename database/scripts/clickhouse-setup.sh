#!/bin/bash
username="gcochran"

# Pull the Docker image
docker pull ${username}/my-clickhouse-image:dev

# Run the Docker container
docker run -d --name my-clickhouse-container -p 8123:8123 -p 9000:9000 ${username}/my-clickhouse-image:dev

# Wait a few seconds for the container to start up
sleep 5

# Execute the SQL command
docker exec -i my-clickhouse-container clickhouse-client <<EOF
INSERT INTO my_database.events (user_id, session_id, event_type, event_timestamp, page_url, product_id) VALUES
(12345, generateUUIDv4(), 'click', now(), 'https://example.com', 67890),
(6789, generateUUIDv4(), 'view', now(), 'http://example.com/items', 87891),
(12345, generateUUIDv4(), 'purchase', now(), 'https://example.com', 73489),
(23452, generateUUIDv4(), 'click', now(), 'https://example.com', 30423),
(13489, generateUUIDv4(), 'view', now(), 'https://example.com', 23789);
EOF