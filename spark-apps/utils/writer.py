import redis

from neo4j import GraphDatabase
from config import (
    CLICKHOUSE_URL,
    CLICKHOUSE_PROPS,
    CASSANDRA_KEYSPACE,
    MONGO_URI,
    NEO4J_URL,
    NEO4J_USER,
    NEO4J_PASSWORD,
    VALKEY_HOST,
    VALKEY_PORT
)


# ========== ClickHouse ==========
def write_to_clickhouse(df, table_name: str):
    """
    Записывает DataFrame в ClickHouse.
    """
    df.write.jdbc(
        CLICKHOUSE_URL,
        table_name,
        mode="append",
        properties=CLICKHOUSE_PROPS
    )


# ========== Cassandra ===========
def write_to_cassandra(df, table: str, keyspace: str = CASSANDRA_KEYSPACE):
    """
    Записывает DataFrame в Cassandra.
    """
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .mode("append") \
        .save()


# ========== MongoDB ==========
def write_to_mongodb(df, collection: str):
    """
    Записывает DataFrame в MongoDB.
    """
    df.write \
        .format("mongodb") \
        .option("collection", collection) \
        .mode("append") \
        .save()


# ========== Neo4j ==========
_driver = GraphDatabase.driver(NEO4J_URL, auth=(NEO4J_USER, NEO4J_PASSWORD))

def write_to_neo4j(df, node_label: str, key_field: str):
    """
    Записывает строки DataFrame как узлы в Neo4j.
    """
    with _driver.session() as session:
        for row in df.collect():
            props = ", ".join([f"{k}: ${k}" for k in row.asDict().keys()])
            query = f"""
                MERGE (n:{node_label} {{ {key_field}: ${key_field} }})
                SET n += {{{props}}}
            """
            session.run(query, **row.asDict())


# ========== Valkey  ==========
def write_to_valkey(df, key_prefix: str):
    """
    Записывает строки DataFrame в Valkey (Redis) как хэши.
    """
    def write_partition(partition):
        r = redis.Redis(host="valkey", port=8081)
        for row in partition:
            data = row.asDict()
            key = f"{key_prefix}:{data.get('id', 'unknown')}"
            r.hset(key, mapping=data)

    df.foreachPartition(write_partition)

