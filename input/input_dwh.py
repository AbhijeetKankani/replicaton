import logging
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from ..utils.dwh_tables import STATIC_TABLES, CalculatedTables
from ..utils.dwh_utils import create_table, log_minmax_date, log_table_sample, log_table_shape
from ..utils.files import DwhFiles
from ..utils.td_connector import td
from ..utils.utils import log_df_string, monthdelta, normalize_code, save_df


def input_dwh(
    logger: logging.Logger,
    dwh_files: DwhFiles,
    calc_tables: CalculatedTables,
    reference_date: datetime,
) -> None:
    logger.info("Start with data_input_kundenkonzern_vertragspartner")
    data_input_kundenkonzern_vertragspartner(logger, calc_tables=calc_tables)

    logger.info("Start with dwh_paket_gewicht")
    df_prod_gewicht = dwh_paket_gewicht(logger, dwh_files, reference_date)

    # Start of Abhijeet: Inserting extracted data into HANA Cloud
    if df_prod_gewicht is not None and not df_prod_gewicht.empty:
        logger.info("Inserting data from DataFrame into HANA Cloud...")
        try:
            connection = connect_to_hana()  #  connect_to_hana() is correctly implemented in your utils
            cursor = connection.cursor()

            # Assuming your HANA table has corresponding columns to your DataFrame
            sql = """
                INSERT INTO DWH_HANA_TABLE (ekpnr, gewicht_sum, gewicht_bis01kg, gewicht_bis02kg, ...)
                VALUES (?, ?, ?, ?, ...)
            """

            # Map the DataFrame to tuples for insertion
            data_to_insert = [
                (
                    row['ekpnr'], row['gewicht_sum'], row['gewicht_bis01kg'], row['gewicht_bis02kg'], ...
                )
                for index, row in df_prod_gewicht.iterrows()
            ]

            # Execute the insert statement with the DataFrame data
            cursor.executemany(sql, data_to_insert)
            connection.commit()

            logger.info(f"Inserted {len(data_to_insert)} rows into HANA Cloud.")
        except Exception as e:
            logger.error(f"Error inserting data into HANA Cloud: {str(e)}")
        finally:
            cursor.close()
            connection.close()

    else:
        logger.warning("DataFrame is empty. No data to insert into HANA Cloud.")

    logger.info("Finished inserting data into HANA Cloud.")
    # End of Abhijeet

    logger.info("!!!Finished input_dwh!!!")


def dwh_paket_gewicht(logger: logging.Logger, files: DwhFiles, reference_date: datetime) -> pd.DataFrame:
    """
    Queries weight data for all ekps and returns it as a DataFrame
    """
    logger.info("Starting extraction of parcel weight data")

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
                SUM(PZE.GEWICHT) as gewicht_sum,
                SUM(CASE WHEN (PZE.GEWICHT > 0 AND PZE.GEWICHT <= 1) THEN 1 ELSE 0 END) AS gewicht_bis01kg,
                SUM(CASE WHEN (PZE.GEWICHT > 1 AND PZE.GEWICHT <= 2) THEN 1 ELSE 0 END) AS gewicht_bis02kg,
                ...
            FROM
            (
                SELECT
                    SENDUNGS_CODE,
                    GEWICHT,
                    AUFTRAGGEBER_EKP as ekpnr
                FROM
                    {pze_table}
                WHERE
                    ereignis_datum BETWEEN DATE '{start_date}' AND DATE '{end_date}'
                    AND GEWICHT > 0
                qualify row_number() over (partition by SENDUNGS_CODE order by ereignis_datum desc)= 1
            ) AS PZE
            LEFT JOIN
            (
                SELECT DISTINCT
                    SHIPMENT_CODE,
                    EKP_NO as ekpnr
                FROM
                    {pan_table}
                WHERE
                    load_dtm BETWEEN DATE '{start_date}' - INTERVAL '30' DAY AND DATE '{end_date}'
                qualify row_number() over (partition by SHIPMENT_CODE order by load_dtm desc)= 1
            ) AS PAN
            ON
                PZE.SENDUNGS_CODE = PAN.SHIPMENT_CODE
            GROUP BY 1
            WHERE COALESCE(PAN.ekpnr, PZE.ekpnr) BETWEEN 5000000000 AND 7000000000
            """
        return sql

    dfs_gewicht = []
    start_date = delta_11_month
    end_date = start_date + relativedelta(day=31)
    while end_date < delta_n1_month:
        end_date = start_date + relativedelta(day=31)
        query = weight_query(start_date=start_date, end_date=end_date)
        logger.info(query)
        df_gewicht_month = td.download_table_odbc(query)
        dfs_gewicht.append(df_gewicht_month)
        start_date += relativedelta(months=1)

    df_gewicht = pd.concat(dfs_gewicht)
    normalize_code(df_gewicht, {"ekpnr": 10})
    df_gewicht["abrnr"] = df_gewicht["ekpnr"]
    df_gewicht = df_gewicht.drop(columns=["verf", "teiln"])
    df_prod_gewicht = df_gewicht.groupby(["abrnr", "ekpnr"], as_index=False, dropna=False).sum()

    logger.info(f"\n{df_prod_gewicht.head()}")
    logger.info(log_df_string(df_prod_gewicht, ["ekpnr"]))

    save_df(files.df_prod_gewicht, df_prod_gewicht)

    return df_prod_gewicht


def data_input_kundenkonzern_vertragspartner(logger: logging.Logger, calc_tables: CalculatedTables):
    """
    Creates input tables in DWH for kundenkonzern and vertragspartner
    """
    kpr_kunde_konzern = STATIC_TABLES["kpr_kunde_konzern"]
    vemo_vertragspartner_table = STATIC_TABLES["vemo_vertragspartner"]

    log_minmax_date(td, vemo_vertragspartner_table, "gueltig_von", logger)

    query_kunde_konzern = f"""
        SELECT DISTINCT ag_ekp,
            ag_name,
            mac_code,
            mac_name,
            konzern_code,
            konzern_name,
            kam_team
        FROM {kpr_kunde_konzern}
        WHERE (mac_code IS NOT NULL OR konzern_code IS NOT NULL)
            AND (mac_name IS NOT NULL OR konzern_name IS NOT NULL)
        """

    query_vemo_vertragspartner = f"""
        SELECT DISTINCT * FROM {vemo_vertragspartner_table}
        qualify (row_number() over
            (partition by ekpnr, vbeln, posnr order by gueltig_von desc, gueltig_bis desc) = 1)
        """

    query_dict = {"vemo_kunde_konzern": query_kunde_konzern, "vemo_vertragspartner": query_vemo_vertragspartner}

    for key in query_dict:
        table = calc_tables.get_table(key)
        query = query_dict[key]
        create_table(td, table, query, calc_tables.get_index(key), logger)
        logger.info(query)
        log_table_shape(td, table, logger)
        log_table_sample(td, table, logger)
