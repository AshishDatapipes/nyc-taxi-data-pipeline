from pyspark.sql.functions import max as spark_max
import psycopg2

from utils.jdbc import read_query
from config.db_config import DB_HOST, DB_NAME, DB_PROPERTIES


def get_last_processed_id(spark, pipeline_name):
    """
    Returns the last processed ID for the given pipeline.
    """

    query = f"""
        SELECT last_processed_id
        FROM pipeline_metadata
        WHERE pipeline_name = '{pipeline_name}'
    """

    df = read_query(spark, query)

    if df.count() == 0:
        return 0

    return df.collect()[0]["last_processed_id"]


def get_max_id(df):
    """
    Returns the maximum ID from a DataFrame.
    """

    max_id = df.select(spark_max("id")).collect()[0][0]

    if max_id is None:
        return 0

    return max_id


def update_metadata(pipeline_name, last_processed_id, status):
    """
    Updates the metadata table after a successful pipeline run.
    """

    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_PROPERTIES["user"],
        password=DB_PROPERTIES["password"]
    )

    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE pipeline_metadata
        SET
            last_processed_id = %s,
            last_run = CURRENT_TIMESTAMP,
            status = %s
        WHERE pipeline_name = %s
        """,
        (last_processed_id, status, pipeline_name)
    )

    conn.commit()

    cursor.close()
    conn.close()

def get_last_processed_offset():
    """
    Returns the last processed Kafka offset for Bronze.
    """

    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_PROPERTIES["user"],
        password=DB_PROPERTIES["password"]
    )

    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT last_processed_offset
        FROM pipeline_metadata
        WHERE pipeline_name = 'bronze'
        """
    )

    result = cursor.fetchone()

    cursor.close()
    conn.close()

    if result is None or result[0] is None:
        return 0

    return result[0]


def update_bronze_offset(offset):
    """
    Updates the last successfully processed Kafka offset for Bronze.
    """

    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_PROPERTIES["user"],
        password=DB_PROPERTIES["password"]
    )

    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE pipeline_metadata
        SET
            last_processed_offset = %s,
            last_run = CURRENT_TIMESTAMP,
            status = 'SUCCESS'
        WHERE pipeline_name = 'bronze'
        """,
        (offset,)
    )

    conn.commit()

    cursor.close()
    conn.close()
