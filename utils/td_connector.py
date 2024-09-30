# from ..utils.logger import global_logger
import pyodbc

from pda.connection.teradata import Teradata
from ..run_config import td_config
import logging


# Setup logger for the td_connector
logger = logging.getLogger("td_connector")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Initialize Teradata connection
td = Teradata(config_base=td_config)

def open_dwh_session():
    """
    Opens a session to Teradata Data Warehouse using pyodbc.
    Enhanced with logging and error handling.
    """
    logger.info("Initializing Teradata connection using pyodbc")
    try:
        # Log pyodbc version and driver details
        logger.info(f"pyodbc version: {pyodbc.version}")
        pyodbc.pooling = False
        pyodbc.dataSources()
        pyodbc.drivers()

        # Teradata connection parameters
        user = 'OA1B_BI_AWB19_PRD'  # Replace with your username
        pwd = '$tdwallet('+user+')'  # Using Teradata Wallet for secure password storage
        host = 'TDP-N0101.deutschepost.dpwn.com'  # Teradata host
        driver = '{/opt/teradata/client/16.20/lib64/tdataodbc_sb64.so}'  # ODBC driver for Teradata

        logger.info(f"Connecting to Teradata host: {host}")

        # Establishing the connection to Teradata
        session = pyodbc.connect(
            "driver=" + driver + ";dbcname=" + host + ";uid=" + user + ";pwd=" + pwd + ";charset=utf8;",
            autocommit=True
        )
        
        # Setting the encoding and decoding for session
        session.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        session.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        session.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-16le')
        session.setencoding(encoding='utf-8')

        logger.info("DWH session opened successfully")
        return session
    
    except pyodbc.Error as e:
        # Log and raise an error if connection fails
        logger.error(f"Error occurred while connecting to DWH: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        raise e
