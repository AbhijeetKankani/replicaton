import logging
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from ..utils.dwh_tables import STATIC_TABLES, CalculatedTables
from ..utils.dwh_utils import (
    create_table,
    log_minmax_date,
    log_table_sample,
    log_table_shape,
)
from ..utils.files import DwhFiles
from ..utils.td_connector import td
from ..utils.utils import log_df_string, monthdelta, normalize_code, save_df
from ..utils.hana_connector import connect_to_hana  # Assuming hana_connector has connect_to_hana method


def input_dwh(
    logger: logging.Logger,
    dwh_files: DwhFiles,
    calc_tables: CalculatedTables,
    reference_date: datetime,
) -> None:
    logger.info("Start processing DWH and KPR data")
    dwh_paket_gewicht(logger, dwh_files, reference_date)

    # Insert aggregated data into HANA Cloud after fetching from DWH
    connection = connect_to_hana()
    cursor = connection.cursor()

    sql = """
        INSERT INTO DWH_HANA_TABLE (column1, column2, ...)
        VALUES (?, ?, ...)
    """
    cursor.executemany(sql, data_to_insert)  # Replace data_to_insert with actual data for HANA
    connection.commit()
    cursor.close()
    connection.close()

    logger.info("DWH data inserted into HANA Cloud successfully")
    logger.info("Finished processing input_dwh")


def dwh_paket_gewicht(logger: logging.Logger, files: DwhFiles, reference_date: datetime):
    """
    Queries weight data for all ekps from the DWH and saves it to a file for processing.
    """

    pze_table = STATIC_TABLES["pze_table"]
    pan_table = STATIC_TABLES["pan_table"]

    delta_12_month = pd.to_datetime(monthdelta(-12, reference_date), format="%Y%m", errors="coerce").date()
    delta_11_month = pd.to_datetime(monthdelta(-11, reference_date), format="%Y%m", errors="coerce").date()
    delta_n1_month = pd.to_datetime(monthdelta(1, reference_date), format="%Y%m", errors="coerce").date()

    logger.info(f"Dates: +1 month {delta_n1_month}, -11 month {delta_11_month}, -12 month {delta_12_month}")

    log_minmax_date(td, pze_table, "ereignis_datum", logger)
    log_minmax_date(td, pan_table, "load_dtm", logger)

    def weight_query(start_date, end_date):
        sql = f"""
             SELECT
                COALESCE(PAN.ekpnr, PZE.ekpnr) as ekpnr,
                COALESCE(PAN.verf, PZE.verf) as verf,
                COALESCE(PAN.teiln, PZE.teiln) as teiln,
                SUM(PZE.GEWICHT) as gewicht_sum,
                SUM(CASE WHEN (PZE.GEWICHT > 0 AND PZE.GEWICHT <= 1) THEN 1 ELSE 0 END) AS gewicht_bis01kg,
                SUM(CASE WHEN (PZE.GEWICHT > 1 AND PZE.GEWICHT <= 2) THEN 1 ELSE 0 END) AS gewicht_bis02kg,
                ...
                -- Continue with all required weight ranges
            FROM {pze_table} PZE
            LEFT JOIN {pan_table} PAN ON PZE.SENDUNGS_CODE = PAN.SHIPMENT_CODE
            WHERE PZE.ereignis_datum BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        """
        return sql

    dfs_gewicht = []
    start_date = delta_11_month
    while start_date < delta_n1_month:
        end_date = start_date + relativedelta(day=31)
        query = weight_query(start_date=start_date, end_date=end_date)
        logger.info(query)
        df_gewicht_month = td.download_table_odbc(query)
        dfs_gewicht.append(df_gewicht_month)
        start_date += relativedelta(months=1)

    df_gewicht = pd.concat(dfs_gewicht)
    normalize_code(df_gewicht, {"ekpnr": 10})
    df_gewicht["abrnr"] = df_gewicht["ekpnr"] + df_gewicht["verf"] + df_gewicht["teiln"]
    df_gewicht = df_gewicht.drop(columns=["verf", "teiln"])
    df_prod_gewicht = df_gewicht.groupby(["abrnr", "ekpnr"], as_index=False, dropna=False).sum()

    logger.info(f"\n{df_prod_gewicht.head()}")
    logger.info(log_df_string(df_prod_gewicht, ["ekpnr"]))

    save_df(files.df_prod_gewicht, df_prod_gewicht)
