"""
ClickHouse connection helper.

Uses the same NYC_TAXI database Spark writes into. Reads credentials
from environment variables so the same image works locally and in
docker-compose without code changes.

NOTE ON SECURITY: this reuses the existing `spark_user` by default to
keep the demo simple (see clickhouse/spark-user.xml). For anything
beyond a class project, create a dedicated read-only ClickHouse user
for the agent instead of reusing Spark's write user — the agent only
ever needs SELECT.
"""

import os

from langchain_community.utilities import SQLDatabase

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")  # HTTP interface, used by clickhouse-sqlalchemy
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "NYC_TAXI")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "spark_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "spark_pass")

# clickhouse-sqlalchemy dialect: clickhouse+http://user:pass@host:port/db
CLICKHOUSE_URI = (
    f"clickhouse+http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}"
    f"@{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
)

# Tables the agent is allowed to see/query. Keep this explicit — never let
# the agent introspect the whole database, only the analytics tables it
# needs to answer business questions.
AGENT_TABLES = ["taxi_trips", "hourly_demand", "zone_statistics"]


def get_db() -> SQLDatabase:
    """Return a LangChain SQLDatabase wrapper scoped to the allowed tables."""
    return SQLDatabase.from_uri(
        CLICKHOUSE_URI,
        include_tables=AGENT_TABLES,
        sample_rows_in_table_info=3,  # gives the LLM a few example rows for grounding
    )
