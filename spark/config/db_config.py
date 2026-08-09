# ==========================================================
# PostgreSQL Database Configuration
# ==========================================================

DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "taxi"

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_PROPERTIES = {
    "user": "airflow",
    "password": "airflowpassword",
    "driver": "org.postgresql.Driver"
}
