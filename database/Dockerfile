# Use the official ClickHouse image as a base
FROM clickhouse/clickhouse-server:latest

# Copy the initialization script and docker config into the container
COPY init-db.sh /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/init-db.sh
# Expose the ClickHouse ports
EXPOSE 8123 8443 9000 9440

