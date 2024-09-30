import logging
import pandas as pd

pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 20)

def setup_logger(name, log_file, level=logging.DEBUG, console_output=False):
    """
    Set up as many loggers as you want. 
    
    Args:
        name (str): The name of the logger.
        log_file (str): The file where logs will be written.
        level (int): Logging level (e.g., logging.DEBUG, logging.INFO).
        console_output (bool): If True, also output logs to the console (stdout).
    """
    
    formatter = logging.Formatter(
        fmt="[%(asctime)s %(levelname)s - {%(filename)s:%(lineno)4s - %(funcName)s()}]: %(message)s", 
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(file_handler)

    # Optional console output
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
