"""
Настройки подключения к источникам данных.
"""
# Postgres
POSTGRES_URL = "jdbc:postgresql://postgres:5432/salesdb"
POSTGRES_PROPS = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# ClickHouse
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/default"
CLICKHOUSE_PROPS = {
    "user": "default",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

# Cassandra
CASSANDRA_KEYSPACE = "saleskeyspace"

# MongoDB
MONGO_URI = "mongodb://mongo:27017/salesdb"

# Neo4j
NEO4J_URL = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Valkey (Redis)
VALKEY_HOST = "valkey"
VALKEY_PORT = 8081