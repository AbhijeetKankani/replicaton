import pandas as pd
from pda.connection.teradata import Teradata


def log_minmax_date(session, db_table, date_column, logger):
    """
    Logs the minimum and maximum dates for a specified column in a table.
    """
    query = (
        f"""SELECT
            min({date_column}) as min_{date_column},
            max({date_column}) as max_{date_column}
        FROM {db_table}"""
    )

    try:
        if isinstance(session, Teradata):
            min_max_date = session.download_table_odbc(query)
        else:
            min_max_date = pd.read_sql(query, session)
        logger.info(f"\nQuery: {db_table} \n{min_max_date}")
    except Exception as e:
        logger.error(f"Error retrieving min/max dates from {db_table}: {e}")
    # Optional assertion check
    # assert min_max_date <= expected_date


def log_table_shape(td, db_table, logger):
    """
    Logs the number of rows and columns of a table.
    """
    db_rows = f"""SELECT count(1) as size FROM {db_table}"""
    schema = db_table.split(".")[0]
    table = db_table.split(".")[1]

    db_cols = f"""SELECT Count(columnname) As ColumnCount
        FROM DBC.Columns WHERE DatabaseName='{schema}' AND TableName='{table}';
    """

    try: 
        rows = td.download_table_odbc(db_rows)
        cols = td.download_table_odbc(db_cols)
        shape = (rows.iloc[0, 0], cols.iloc[0, 0])
        logger.info(f"\n{db_table} shape: {shape}")
        return shape
    except Exception as e:
        logger.error(f"Error retrieving shape for {db_table}: {e}")
        return None


def log_table_sample(td, db_table, logger, sample=5):
    """
    Logs a sample of rows from a table.
    """
    q = f"""SELECT * FROM {db_table} SAMPLE {str(sample)}"""
    try:
        df = td.download_table_odbc(q)
        logger.info(f"\n{db_table} sample: \n{df}")
        return df
    except Exception as e:
        logger.error(f"Error retrieving sample for {db_table}: {e}")
        return None


def create_table(td, tmp_table, query, index, logger):
    """
    Drops and recreates a table with the specified query and primary index.
    """
    try:
        td.execute_sql(f''' DROP TABLE {tmp_table} ''')
        logger.info(f"Dropped existing {tmp_table}")
    except Exception as e:
        logger.warning(f"Failed dropping {tmp_table} \nError: {e}")

    try:
        sql = f'''
        CREATE TABLE {tmp_table} AS ({query}) WITH DATA
        PRIMARY INDEX {index}
        '''
        td.execute_sql(sql)
        logger.info(f"Created {tmp_table}")
    except Exception as e:
        logger.error(f"Failed to create {tmp_table}: {e}")
