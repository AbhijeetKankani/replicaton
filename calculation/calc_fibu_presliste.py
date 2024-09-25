import logging
import pandas as pd
import numpy as np
from ..utils.files import SapFiles, CalculatedFiles
from ..utils.utils import read_df, save_df, exclude_abrnr
from .calc_constants import MATERIAL_LIST, PL_ENTRIES_PAKET, PL_ENTRIES_WAPO, PL_LETTERS, PRODUKT_VERFAHREN_MAPPING


def calc_fibu_preisliste(
    sap_files: SapFiles,
    calc_files: CalculatedFiles,
    product: str,
    logger: logging.Logger,
    pl_start_letters: str = PL_LETTERS,
):
    assert product.lower() in PRODUKT_VERFAHREN_MAPPING
    logger.info("calculating FIBU pricelist for %s", product)

    df_fibuabzug = read_df(sap_files.df_fibu_excl_a)
    df_kalknr_mapping = read_df(calc_files.df_mapping)
    df_kalknr_mapping = df_kalknr_mapping[["abrnr", "kalknr"]].copy()

    logger.info("excluding Kleinpaket from FIBU %s", product)
    df_kp = read_df(sap_files.df_kt_abr_kleinpaket)
    df_fibuabzug = exclude_abrnr(df_fibuabzug, df_kp, logger)

    for col in ["PL", "RV-Nr", "LCode", "PKZ", "KArt", "Material", "Waehrg"]:
        df_fibuabzug[col].fillna("", inplace=True)

    df_fibuabzug.rename(columns={"Auftr.geb.": "ekpnr", "Verf.": "Verf", "Teiln.": "Teiln"}, inplace=True)
    df_fibuabzug["abrnr"] = df_fibuabzug.ekpnr + df_fibuabzug.Verf + df_fibuabzug.Teiln

    # macht es hier nicht mehr Sinn, die Preislisten nach dem Gültigkeitsdatum zu filtern, anstelle der PL Bezeichnungen?
    if product.lower() == "paket":
        mask = (df_fibuabzug["Verf"].astype(str).str.strip() == "01") & (
            (df_fibuabzug.Material.isin(MATERIAL_LIST)) | df_fibuabzug.PL.astype(str).str.contains(pl_start_letters)
        )
        price_list = PL_ENTRIES_PAKET
    elif product.lower() == "warenpost":
        mask = (df_fibuabzug["Verf"].astype(str).str.strip() == "62") & (df_fibuabzug.Material.isin(MATERIAL_LIST))
        price_list = PL_ENTRIES_WAPO

    df_fibuabzug = df_fibuabzug[mask][
        ["ekpnr", "abrnr", "Gueltig_ab_l", "Gueltig_bis_l", "PL", "Gueltig_ab_r", "Gueltig_bis_r", "Material"]
    ]

    logger.info("calculating valid time periods for prices")
    df_fibuabzug["PL"] = np.where(df_fibuabzug.PL.isin(price_list), df_fibuabzug.PL, "")
    df_fibuabzug["Gueltig_ab"] = np.where(
        df_fibuabzug.PL.isin(price_list),
        df_fibuabzug.Gueltig_ab_l.combine_first(df_fibuabzug.Gueltig_ab_r),
        df_fibuabzug.Gueltig_ab_r.combine_first(df_fibuabzug.Gueltig_ab_l),
    )
    df_fibuabzug["Gueltig_bis"] = np.where(
        df_fibuabzug.PL.isin(price_list),
        df_fibuabzug.Gueltig_bis_l.combine_first(df_fibuabzug.Gueltig_bis_r),
        df_fibuabzug.Gueltig_bis_r.combine_first(df_fibuabzug.Gueltig_bis_l),
    )

    check_if_na = (df_fibuabzug["PL"].isna()) | (df_fibuabzug["PL"].str.strip() == "") | (pd.isnull(df_fibuabzug["PL"]))

    df_fibuabzug["Gueltig_ab"] = np.where(
        check_if_na, df_fibuabzug[["Gueltig_ab_l", "Gueltig_ab_r"]].max(axis=1), df_fibuabzug["Gueltig_ab"]
    )
    df_fibuabzug["Gueltig_bis"] = np.where(
        check_if_na & (df_fibuabzug["Gueltig_ab_l"] > df_fibuabzug["Gueltig_ab_r"]),
        df_fibuabzug["Gueltig_bis_l"],
        np.where(
            check_if_na & (df_fibuabzug["Gueltig_ab_l"] < df_fibuabzug["Gueltig_ab_r"]),
            df_fibuabzug["Gueltig_bis_r"],
            df_fibuabzug["Gueltig_bis"],
        ),
    )

    df_fibu_unique = df_fibuabzug[["ekpnr", "abrnr", "PL", "Gueltig_ab", "Gueltig_bis"]].copy()
    df_fibu_unique = df_fibu_unique.sort_values(
        by=["ekpnr", "abrnr", "Gueltig_ab", "Gueltig_bis", "PL"], ascending=[True, True, False, False, False]
    )
    df_fibu_unique.drop_duplicates(subset=["ekpnr", "abrnr"], keep="first", inplace=True)
    df_fibu_unique = df_fibu_unique.merge(df_kalknr_mapping, on=["abrnr"])
    df_fibu_unique = df_fibu_unique.groupby(["ekpnr", "kalknr"], as_index=False).agg(
        {"abrnr": "first", "Gueltig_ab": "first", "Gueltig_bis": max, "PL": "first"}
    )
     
# Start of Abhijeet Insert into HANA Cloud after the data is processed
    connection = connect_to_hana()
    cursor = connection.cursor()

    # Prepare SQL query to insert data into HANA Cloud table (example)
    sql = """
        INSERT INTO PRIMA_PRICE_DELTA (column1, column2, ...)
        VALUES (?, ?, ...)
    """
    cursor.execute(sql, [value1, value2, ...])
    connection.commit()
    cursor.close()
    connection.close()

    logger.info("Data inserted into HANA Cloud successfully")
    # End of Abhijeet 

    save_df(calc_files.df_fibu_preisliste_unique, df_fibu_unique)
