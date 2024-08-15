FROM clickhouse/clickhouse-server:latest

COPY init-db.sh /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/init-db.sh

EXPOSE 8123 8443 9000 9440

