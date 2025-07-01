import snowflake.connector
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Snowflake credentials (same as in log_forwarder.py)
USER = 'keerthana'
PASSWORD = 'Quadrantkeerthana2025'
ACCOUNT = 'pcc86913.us-east-1'
ROLE = 'ACCOUNTADMIN'
WAREHOUSE = 'COMPUTE_WH'
DATABASE = 'SNOWFLAKE'
SCHEMA = 'ACCOUNT_USAGE'

def connect_to_snowflake():
    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
    logger.info("Connected to Snowflake")
    return conn

def run_invalid_query(conn):
    cursor = conn.cursor()
    try:
        # Invalid query: Selecting from a non-existent table
        invalid_query = "SELECT * FROM NON_EXISTENT_TABLE"
        logger.info(f"Executing invalid query: {invalid_query}")
        cursor.execute(invalid_query)
        rows = cursor.fetchall()
        logger.info("Query executed successfully (unexpected)")
    except snowflake.connector.errors.ProgrammingError as e:
        logger.error(f"Expected error occurred: {e}")
    finally:
        cursor.close()

def main():
    conn = connect_to_snowflake()
    try:
        run_invalid_query(conn)
    finally:
        conn.close()
        logger.info("Snowflake connection closed")

if __name__ == "__main__":
    main()