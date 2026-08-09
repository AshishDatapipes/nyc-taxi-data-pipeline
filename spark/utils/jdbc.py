from config.db_config import JDBC_URL, DB_PROPERTIES


def read_table(spark, table_name):
    """
    Read a PostgreSQL table into a Spark DataFrame.
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", DB_PROPERTIES["user"])
        .option("password", DB_PROPERTIES["password"])
        .option("driver", DB_PROPERTIES["driver"])
        .load()
    )


def write_table(df, table_name, mode="append"):
    """
    Write a Spark DataFrame to a PostgreSQL table.
    """
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", DB_PROPERTIES["user"])
        .option("password", DB_PROPERTIES["password"])
        .option("driver", DB_PROPERTIES["driver"])
        .mode(mode)
        .save()
    )


def read_query(spark, query):
    """
    Read the result of a SQL query from PostgreSQL.
    Spark JDBC requires the query to be wrapped and aliased.
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"({query}) AS temp")
        .option("user", DB_PROPERTIES["user"])
        .option("password", DB_PROPERTIES["password"])
        .option("driver", DB_PROPERTIES["driver"])
        .load()
    )
