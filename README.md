# ClickHouse-FlatFile Ingestion Tool

## Setup
1. Install Java 17+, Maven, and Docker.
2. Run `mvn clean install` to build the project.
3. Start ClickHouse:
   ```bash
   docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 -p 8443:8443 -p 9440:9440 -v $(pwd)/clickhouse-config/users.xml:/etc/clickhouse-server/users.d/users.xml clickhouse/clickhouse-server:latest