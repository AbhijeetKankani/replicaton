import hdbcli.dbapi as dbapi
import logging
from utils.logger import setup_logger  # Assuming setup_logger is in utils.logger

# Set up a logger for HANA connections and operations
hana_logger = setup_logger('hana_logger', 'hana_operations.log', level=logging.DEBUG, console_output=True)

def connect_to_hana():
    """ 
    Connects to HANA Cloud using credentials and returns a connection object.
    Includes logging for success, failures, and other connection-related information.
    """
    hana_host = "your_hana_host"  # Replace with your HANA Cloud host
    hana_port = 443  # Default port for HANA Cloud
    hana_user = "your_username"  # Replace with HANA Cloud username
    hana_password = "your_password"  # Replace with HANA Cloud password
    
    try:
        # Attempting to establish a connection
        connection = dbapi.connect(
            address=hana_host,
            port=hana_port,
            user=hana_user,
            password=hana_password
        )
        hana_logger.info(f"Successfully connected to HANA Cloud at {hana_host}:{hana_port}")
        return connection
    except dbapi.Error as e:
        # Log any connection errors
        hana_logger.error(f"Failed to connect to HANA Cloud: {str(e)}")
        raise

def execute_query(connection, query, data=None):
    """
    Executes an SQL query on the HANA Cloud instance.
    Includes logging for query execution and any errors.
    
    Args:
        connection: The active HANA connection.
        query: The SQL query to execute.
        data: Optional data to insert (for parameterized queries).
    """
    try:
        cursor = connection.cursor()
        hana_logger.info(f"Executing query: {query}")

        if data:
            cursor.executemany(query, data)
            hana_logger.debug(f"Data provided for query: {data}")
        else:
            cursor.execute(query)
        
        connection.commit()
        hana_logger.info("Query executed and committed successfully")
    except dbapi.Error as e:
        # Log any errors during query execution
        hana_logger.error(f"Failed to execute query: {str(e)}")
        raise
    finally:
        cursor.close()

def close_connection(connection):
    """
    Closes the connection to HANA Cloud.
    Includes logging for connection close.
    """
    try:
        connection.close()
        hana_logger.info("Successfully closed the connection to HANA Cloud")
    except dbapi.Error as e:
        hana_logger.error(f"Error while closing the connection: {str(e)}")
