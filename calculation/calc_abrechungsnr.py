import logging
from datetime import date, datetime
from typing import List

import pandas as pd
import numpy as np

from src.utils.dwh_tables import STP_TABLES, CalculatedTables
from src.utils.dwh_utils import create_table
from src.utils.files import CalculatedFiles
from src.utils.td_connector import td
from src.utils.utils import log_df_string, monthdelta, save_df


def ist_abrechnungsnr(
    logger: logging.Logger,
    calc_files: CalculatedFiles,
    calc_tables: CalculatedTables,
    reference_date: datetime,
) -> None:
    """
    Merges monthly_volumes and kunden_seit tables, then checks, if multiple entries are in kunden_seit field and writes
    them to log. Only keeps the minimum of kunden_seit entries, if multiple entries exists.
    Calculates amount and average volume for different time deltas and saves to file.
    """

    delta_1_month = monthdelta(-1, date=reference_date)
    delta_12_month = monthdelta(-12, date=reference_date)

    df = __download_abrnr_base_data(logger, calc_tables, delta_1_month, delta_12_month)
    df_sh2pr_12M_abr = __pivot_months_to_cols(df)

    occurence_ekp_verf_teiln = pd.DataFrame(df_sh2pr_12M_abr.abrnr.value_counts())
    multiple_kundenseit = occurence_ekp_verf_teiln[occurence_ekp_verf_teiln > 1]

    """
    TODO Something is wrong with the kunden_seit column calculated from pem.
    Although pem dates cannot be more recent than those from the monthly_volume dates
    which uses the evt_sortierung_all table as source, they are and need correction.
    """
    if multiple_kundenseit.size > 0:
        logger.info(
            """
            several kunden_seit dates found for the same ekp + verfa + teiln,
            file with abrechnungsnr written to "warnings/ist_abrechnnr_multiple_kundenseit.csv",
            dataframe will be corrected!
        """
        )
        save_df(calc_files.df_ist_abrnr_multiple_kundenseit, multiple_kundenseit)

        df_sh2pr_12M_abr = __aggregate_data_from_multiple_kundenseit_abrnr(df_sh2pr_12M_abr)

    df_sh2pr_12M_abr.fillna(0, inplace=True)

    mnt_kpr_cols = [col for col in df_sh2pr_12M_abr.columns if "mnt_kpr" in col]
    vol_kpr_cols = [col for col in df_sh2pr_12M_abr.columns if "vol_kpr" in col]
    for delta in [1, 3, 6, 9, 12]:
        __calculate_amount_and_volume_for_time_horizon(
            reference_date, df_sh2pr_12M_abr, mnt_kpr_cols, vol_kpr_cols, delta
        )

    logger.info(log_df_string(df_sh2pr_12M_abr, ["abrnr"], "df_sh2pr_12M_abr "))
    logger.info(
        log_df_string(
            df_sh2pr_12M_abr[df_sh2pr_12M_abr["verfa"] == "01"],
            ["abrnr"],
            "df_sh2pr_12M_abr Verf=01",
        )
    )
    logger.info(
        log_df_string(
            df_sh2pr_12M_abr[df_sh2pr_12M_abr["verfa"] == "62"],
            ["abrnr"],
            "df_sh2pr_12M_abr Verf=62",
        )
    )

    logger.info(f"\ndf_sh2pr_12M_abr head: \n{df_sh2pr_12M_abr.head()}")

    save_df(calc_files.df_sh2pr_12M_abrnr, df_sh2pr_12M_abr)
    logger.info("!!!finished abrechnungsnr!!!")


def __aggregate_data_from_multiple_kundenseit_abrnr(df_pivot: pd.DataFrame) -> pd.DataFrame:
    agg_dict = {col: "sum" for col in df_pivot.columns if "kpr" in col}
    agg_dict["kunden_seit"] = "min"
    df_pivot = df_pivot.groupby(["abrnr", "ekpnr", "verfa", "teiln"], as_index=False).agg(agg_dict)

    return df_pivot


def __pivot_months_to_cols(df: pd.DataFrame):
    df_pivot = df.pivot_table(
        ["vol_ber", "num_sendung"],
        ["abrnr", "ekpnr", "verfa", "teiln", "kunden_seit"],
        "jahr_monat",
    ).reset_index()

    df_pivot.columns = [col[0] + "_" + str(col[1]) if col[1] != "" else col[0] for col in df_pivot.columns]
    df_pivot.columns = df_pivot.columns.str.replace("num_sendung", "mnt_kpr").str.replace("vol_ber", "vol_kpr")
    return df_pivot


def __download_abrnr_base_data(
    logger: logging.Logger, calc_tables: CalculatedFiles, delta_1_month: str, delta_12_month: str
) -> pd.DataFrame:
    tmp_table = "DBX_DWH_SBX_GB30_PRD.tmp_monthlyV_kundenseit"

    logger.info(f"using dates from {delta_12_month} to {delta_1_month} and tmp table {tmp_table}")

    sql_join = f"""
        SELECT
            a.jahr_monat
            , a.abrnr
            , a.AUFTRAGGEBER_EKP as ekpnr
            , a.AUFTRAGGEBER_VERFAHREN as verfa
            , a.AUFTRAGGEBER_TEILNAHME as teiln
            , case when a.jahr_monat < min(b.kunden_seit)
                then a.jahr_monat else min(b.kunden_seit) end as kunden_seit
            , sum(a.vol_ber * a.num_sendung) as vol_ber
            , sum(a.num_sendung) as num_sendung
        FROM
        (
            select
                AUFTRAGGEBER_EKP || AUFTRAGGEBER_VERFAHREN || AUFTRAGGEBER_TEILNAHME as abrnr,
                AUFTRAGGEBER_EKP,
                AUFTRAGGEBER_VERFAHREN,
                AUFTRAGGEBER_TEILNAHME,
                vol_ber,
                num_sendung,
                jahr * 100 + monat as jahr_monat
            from {STP_TABLES["monthly_volume"]}
            where jahr_monat >= {delta_12_month} and jahr_monat <= {delta_1_month}
        ) as a
        LEFT JOIN {calc_tables.get_table("kunden_seit")} as b
        ON a.abrnr = b.abrnr
        WHERE a.abrnr is not NULL
            and a.abrnr
                not in (sel abrnr from {calc_tables.get_table("kt_abr_aktionsgeschaeft")} where abrnr is not null)
            and a.abrnr
                not in (sel abrnr from {calc_tables.get_table("kt_abr_kleinpaket")} where abrnr is not null)
        GROUP BY jahr_monat, a.abrnr, AUFTRAGGEBER_EKP, AUFTRAGGEBER_VERFAHREN, AUFTRAGGEBER_TEILNAHME
    """

    logger.info(sql_join)

    create_table(td, tmp_table, sql_join, "(abrnr)", logger)

    df = td.download_table_odbc(f"select * from {tmp_table}")
    return df


def __calculate_amount_and_volume_for_time_horizon(
    reference_date: date, df_sh2pr_12M_abr: pd.DataFrame, mnt_kpr_cols: List[str], vol_kpr_cols: List[str], delta: int
):
    first_month = monthdelta(-delta, date=reference_date)
    mnt_kpr_period = [col for col in mnt_kpr_cols if int(col.split("_")[-1]) >= first_month]
    vol_kpr_period = [col for col in vol_kpr_cols if int(col.split("_")[-1]) >= first_month]

    df_sh2pr_12M_abr["kunden_seit"] = pd.to_numeric(df_sh2pr_12M_abr["kunden_seit"], errors="coerce")
    df_sh2pr_12M_abr[f"amount_{str(delta).zfill(2)}M"] = np.where(
        df_sh2pr_12M_abr["kunden_seit"] <= first_month, df_sh2pr_12M_abr[mnt_kpr_period].sum(axis=1), np.nan
    )

    df_sh2pr_12M_abr[f"vol_{str(delta).zfill(2)}M_avg"] = np.where(
        df_sh2pr_12M_abr["kunden_seit"] <= first_month,
        round((df_sh2pr_12M_abr[vol_kpr_period].sum(axis=1) / df_sh2pr_12M_abr[mnt_kpr_period].sum(axis=1)), 2),
        np.nan,
    )
