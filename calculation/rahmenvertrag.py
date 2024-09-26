import logging
import numpy as np
import pandas as pd
from src.utils.files import KprFiles, DwhFiles, SapFiles, CalculatedFiles
from src.utils.utils import read_df, save_df

def map_rahmenvertag_ekp(df_abr: pd.DataFrame, df_mapping: pd.DataFrame) -> pd.DataFrame:
    df_mapping = df_mapping.drop(columns="kalknr").drop_duplicates(keep="first")
    df_merged = df_abr.merge(df_mapping, left_on=["abrnr"], right_on=["abrnr"], how="left")
    df_merged["ag_ekp"] = df_merged.abrnr.str[:10]
    df_merged["ekpnr"] = np.where(df_merged.rv_ekp.notna(), df_merged.rv_ekp, df_merged.ag_ekp)
    df_merged.drop(columns=["rv_ekp", "rahmenvertrag", "pl"], inplace=True)
    return df_merged

def _overwrite_ekpnr_rv(logger: logging.Logger, file_path: str, df_kontrakt: pd.DataFrame, mapping_func):
    logger.info("reading file %s", file_path)
    df_abr = read_df(file_path)
    logger.info("shape before mapping rahmenvertrag %s", df_abr.shape)
    old_file_path = file_path + "_old"
    save_df(old_file_path, df_abr)
    logger.info("saved old file to %s", old_file_path)
    df_abr = mapping_func(df_abr, df_kontrakt)
    logger.info("shape before after rahmenvertrag %s", df_abr.shape)
    save_df(file_path, df_abr)
    logger.info("shape before after rahmenvertrag %s", df_abr.shape)

def _select_files_to_map_rv_ekpnr(
    sap_files: SapFiles,
    kpr_files: KprFiles,
    dwh_files: DwhFiles,
    calc_files: CalculatedFiles,
):
    # Only include relevant KPR and DWH files, removing Vemo and Globuss
    return [
        sap_files.df_fibu,
        sap_files.df_fibu_excl_a,
        sap_files.df_kt_report_excl_a,
        sap_files.df_kt_sh2pr,
        sap_files.df_kt_report,
        kpr_files.df_kpr_kosten,
        kpr_files.df_kpr_treiber,
        kpr_files.df_kpr_zustellung,
        dwh_files.df_prod_gewicht,
        calc_files.df_sh2pr_12M_abrnr,
    ]

def overwrite_files_with_ekpnr_rv(
    logger: logging.Logger,
    sap_files: SapFiles,
    kpr_files: KprFiles,
    dwh_files: DwhFiles,
    calc_files: CalculatedFiles,
):
    files_to_process = _select_files_to_map_rv_ekpnr(
        sap_files=sap_files,
        kpr_files=kpr_files,
        dwh_files=dwh_files,
        calc_files=calc_files,
    )

    logger.info("overwriting ekpnr with rahmenvertrag mapping, files %s", files_to_process)
    df_kontrakt = pd.read_parquet(sap_files.df_kontrakt)

    for f in files_to_process:
        _overwrite_ekpnr_rv(logger=logger, file_path=f, df_kontrakt=df_kontrakt, mapping_func=map_rahmenvertag_ekp)

def reset_files_to_state_before_rv_mapping(
    logger: logging.Logger,
    sap_files: SapFiles,
    kpr_files: KprFiles,
    dwh_files: DwhFiles,
    calc_files: CalculatedFiles,
):
    files_to_process = _select_files_to_map_rv_ekpnr(
        sap_files=sap_files,
        kpr_files=kpr_files,
        dwh_files=dwh_files,
        calc_files=calc_files,
    )

    for file_path in files_to_process:
        logger.info("reading file %s", file_path)
        old_file_path = file_path + "_old"
        rv_file_path = file_path + "_rv"
        df = pd.read_parquet(old_file_path)
        save_df(file_path, df)
        save_df(rv_file_path, df)
        logger.info("reset file %s to %s and backup to %s", old_file_path, file_path, rv_file_path)
