import datetime 
import logging
from dataclasses import asdict

import pandas as pd

from src.utils.dwh_tables import STATIC_TABLES, CalculatedTables
from src.utils.dwh_utils import log_minmax_date
from src.utils.files import KprFiles
from src.utils.td_connector import td
from src.utils.utils import log_df_string, monthdelta, normalize_code, read_df, save_df, cast_types

KPR_COLUMN_TYPES = {
    "abrnr": str,
    "ekpnr": str,
    "prozessebene_id": int,
    "Prozessmenge": float,
    "Fixkosten": float,
    "Varkosten": float,
    "Absatz_6m": float,
    "Umsatz_6m": float,
    "Umsatz_Kunde_6m": float,
    "Absatz_Kunde_6m": float,
    "DBII_Kunde_6m": float,
    "KDGII_Kunde_6m": float,
    "Umsatz_paket_6m": float,
    "Absatz_paket_6m": float,
    "DB2_paket_6m": float,
    "KDG2_paket_6m": float,
    "abhol_pz_einlieferung": float,
    "abhol_bz_einlieferung": float,
    "abhol_regiov": float,
    "abhol_filiale_einlieferung": float,
    "abhol_zusteller": float,
    "kpr_monat": int,
    "kpr_raummass": float,
    "kpr_menge": float,
    "kpr_menge_checked": float,
    "kpr_globuss_check": bool,
    "KDGII_Kunde": float,
    "DBII_Kunde": float,
    "KDG2_paket": float,
    "DB2_paket": float,
    "Umsatz_Kunde": float,
    "Absatz_Kunde": float,
    "Umsatz_paket": float,
    "Absatz_paket": float,
}


def input_kpr(
    logger: logging.Logger,
    kpr_files: KprFiles,
    calc_tables: CalculatedTables,
    reference_date: datetime.datetime,
    product: str,
) -> None:
    logger.info("Starting KPR data input...")
    kpr_rv_ekpnr_mapping(kpr_files=kpr_files, reference_date=reference_date)
    kpr_kosten(logger, kpr_files=kpr_files, calc_tables=calc_tables, reference_date=reference_date, product=product)
    kpr_kosten_merge(logger, kpr_files, product=product)
    kpr_treiber(logger, kpr_files=kpr_files, calc_tables=calc_tables, reference_date=reference_date, product=product)
    kpr_zustellung(logger, kpr_files=kpr_files, calc_tables=calc_tables, product=product, reference_date=reference_date)
    logger.info("Finished KPR data input.")


def kpr_rv_ekpnr_mapping(
    kpr_files: KprFiles,
    reference_date: datetime.datetime,
) -> None:
    mapping_rv_abrnr = _download_rv_abrnr_mapping(reference_date=reference_date)
    save_df(kpr_files.df_mapping_rv_abrnr, mapping_rv_abrnr)


def _download_rv_abrnr_mapping(reference_date: datetime.date) -> pd.DataFrame:
    mapping_sql = f"""SELECT RV_NR as rahmenvertrag,
                                    LEFT(AB_NR, 10) as ekpnr,
                                    AB_NR as abrnr,
                                    RV_Name as kundenname
                            FROM {STATIC_TABLES["kpr_rv"]}
                            WHERE RV_BEGIN <= {reference_date.strftime("%Y%m%d")}
                                and RV_ENDE >= {reference_date.strftime("%Y%m%d")}
    """
    return td.download_table_odbc(mapping_sql)


def kpr_kosten(
    logger: logging.Logger,
    kpr_files: KprFiles,
    calc_tables: CalculatedTables,
    reference_date: datetime.datetime,
    product: str,
):
    """
    Queries KPR data and saves it to dataframes in the output location for KPR files.
    Different products can be specified, e.g., "Paket" or "Warenpost".
    """
    product_id = "1" if product.lower() == "paket" else "34, 35"
    month_now = monthdelta(0, reference_date)
    delta_1_month = monthdelta(-1, reference_date)
    delta_6_month = monthdelta(-6, reference_date)
    delta_12_month = monthdelta(-12, reference_date)

    logger.info(f"Processing KPR costs for dates: now {month_now}, -1 month {delta_1_month}, -6 month {delta_6_month}, -12 month {delta_12_month}")
    query_costs_report15 = get_query_costs_report15(delta_12_month, delta_1_month, product_id, calc_tables)

    # Execute queries
    df_costs_report15 = execute_kpr_queries(logger, kpr_files, product, query_costs_report15)

    # Start of Abhijeet: Inserting the KPR costs data into HANA Cloud
    if df_costs_report15 is not None and not df_costs_report15.empty:
        logger.info("Inserting KPR data from DataFrame into HANA Cloud...")
        try:
            connection = connect_to_hana()  #  connect_to_hana is available in  utils
            cursor = connection.cursor()

            # Assuming your HANA table has corresponding columns to your DataFrame
            sql = """
                INSERT INTO KPR_HANA_TABLE (ekpnr, Prozessmenge, Fixkosten, Varkosten, ...)
                VALUES (?, ?, ?, ?, ...)
            """

            # Map the DataFrame to tuples for insertion
            data_to_insert = [
                (
                    row['ekpnr'], row['Prozessmenge'], row['Fixkosten'], row['Varkosten'], ...
                )
                for index, row in df_costs_report15.iterrows()
            ]
            

            # Execute the insert statement with the DataFrame data
            cursor.executemany(sql, data_to_insert)
            connection.commit()

            logger.info(f"Inserted {len(data_to_insert)} rows into HANA Cloud.")
        except Exception as e:
            logger.error(f"Error inserting KPR data into HANA Cloud: {str(e)}")
        finally:
            cursor.close()
            connection.close()
    else:
        logger.warning("DataFrame is empty. No data to insert into HANA Cloud.")
    
    # End of Abhijeet


def get_query_costs_report15(since_month, until_month, product_id, calc_tables) -> str:
    return f"""
        SELECT
            TMP.abr as abrnr,
            substr(TMP.abr,1,10) as ekpnr,
            TMP.prozessebene_id,
            sum(TMP.pmenge) Prozessmenge,
            sum(TMP.fix_kosten) Fixkosten,
            sum(TMP.var_kosten) Varkosten
        FROM
        (
            SELECT abr, prozessebene_id, pmenge, fix_kosten, var_kosten
            FROM {STATIC_TABLES["kpr_kosten"]}
            WHERE (Monat between '{since_month}' and '{until_month}')
                and produkt_id in ({product_id})
                and abr not in (sel abrnr from {calc_tables.get_table("kt_abr_aktionsgeschaeft")} where abrnr is not null)
        ) AS TMP
        GROUP BY 1,2,3
    """


def execute_kpr_queries(logger, kpr_files, product, query_costs_report15) -> pd.DataFrame:
    """
    Execute the main KPR queries and store the results in relevant dataframes.
    """
    df_costs_report15 = td.download_table_odbc(query_costs_report15)
    logger.info(f"KPR costs report for product '{product}' fetched successfully.")
    cast_types(df_costs_report15, KPR_COLUMN_TYPES)
    save_df(kpr_files.df_kpr_costs_report15, df_costs_report15)
    return df_costs_report15


def kpr_kosten_merge(logger: logging.Logger, files: KprFiles, product: str):
    """
    Merges KPR data into a single dataframe and saves it.
    """
    logger.info("Merging KPR data...")
    df_kpr_kosten = read_df(files.df_kpr_costs_report15)

    logger.info(f"Merged KPR data with shape: {df_kpr_kosten.shape}")
    save_df(files.df_kpr_kosten, df_kpr_kosten)


def kpr_treiber(logger: logging.Logger, kpr_files: KprFiles, calc_tables: CalculatedTables, product: str, reference_date: datetime.datetime):
    """
    Queries the KPR treiber data and saves it to CSV.
    """
    logger.info("Processing KPR treiber data...")
    delta_1_month = monthdelta(-1, reference_date)
    kpr_treiber_table = STATIC_TABLES["kpr_treiber"] if product.lower() == "paket" else STATIC_TABLES["kpr_treiber_wapo"]
    query = get_kpr_treiber_query(kpr_treiber_table, delta_1_month)
    
    df_kpr_treiber = td.download_table_odbc(query)
    logger.info(f"KPR treiber data fetched successfully with shape: {df_kpr_treiber.shape}")
    save_df(kpr_files.df_kpr_treiber, df_kpr_treiber)


def get_kpr_treiber_query(treiber_table, delta_1_month):
    return f"""
        SELECT
            SUBSTR(abr,1,10) as ekpnr,
            abr as abrnr,
            monat,
            SUM( ZEROIFNULL(absatz)*ZEROIFNULL(volumen) ) / NULLIFZERO(SUM(CASE WHEN volumen IS NOT NULL THEN absatz ELSE 0 END )) Raummass,
            SUM( ZEROIFNULL(absatz) * ZEROIFNULL(volumen) ) as Volumen,
            NULLIFZERO( SUM( CASE WHEN volumen IS NOT NULL THEN absatz ELSE 0 END ) ) as Absatz,
            SUM(ZEROIFNULL(absatz)) Menge
        FROM {treiber_table}
        WHERE ( monat <= '{delta_1_month}' )
        GROUP BY 1, 2, 3
    """


def kpr_zustellung(logger: logging.Logger, kpr_files: KprFiles, calc_tables: CalculatedTables, product: str, reference_date: datetime.datetime):
    """
    Queries KPR zustellung data and saves it to CSV.
    """
    logger.info("Processing KPR zustellung data...")
    query = get_kpr_zustellung_query(product, reference_date)
    df_kpr_zustellung = td.download_table_odbc(query)

    logger.info(f"KPR zustellung data fetched with shape: {df_kpr_zustellung.shape}")
    save_df(kpr_files.df_kpr_zustellung, df_kpr_zustellung)


def get_kpr_zustellung_query(product, reference_date):
    delta_1_month = monthdelta(-1, reference_date)
    delta_12_month = monthdelta(-12, reference_date)

    return f"""
        SELECT
            abrnr,
            SUBSTR(abrnr,1,10) as ekpnr,
            SUM(CASE WHEN prozessstufe_id=6 THEN pmenge ELSE 0 END) as RZ
        FROM {STATIC_TABLES["kpr_kosten_drop"]}
        WHERE monat BETWEEN '{delta_12_month}' AND '{delta_1_month}'
            AND Produkt_id in ({'1' if product.lower() == 'paket' else '34,35'})
        GROUP BY abrnr
    """
