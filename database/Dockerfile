# Use the official ClickHouse image as a base
FROM clickhouse/clickhouse-server:latest

# Copy the initialization script and docker config into the container
COPY init-db.sql /docker-entrypoint-initdb.d/

# Expose the ClickHouse ports
EXPOSE 8123 9000

