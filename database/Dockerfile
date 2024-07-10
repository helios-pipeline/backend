# Use the official ClickHouse image as a base
FROM clickhouse/clickhouse-server:latest

# Copy the initialization script and docker config into the container
COPY init-db.sql /docker-entrypoint-initdb.d/
# COPY custom-config.xml /etc/clickhouse-server/config.d/

# Expose the ClickHouse ports
# default ports already used by tests/ CH docker instance
# note that couldnt get non-default ports working so went back to using 8123 and 9000, there may be conflicts with the tests/ instance
EXPOSE 8123 9000

