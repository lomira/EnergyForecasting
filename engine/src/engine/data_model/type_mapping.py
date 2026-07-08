from datetime import datetime

# Helper to dynamically map Python types to DuckDB types
TYPE_MAPPING = {
    int: "INTEGER",
    str: "VARCHAR",
    datetime: "TIMESTAMP",
    float: "DOUBLE",
    bool: "BOOLEAN",
}
