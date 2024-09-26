from src.utils.files import DwhFiles, CalculatedFiles
from src.utils.utils import read_df, save_df
import logging
import pandas as pd


def calc_weight_distribution(logger: logging.Logger, dwh_files: DwhFiles, calc_files: CalculatedFiles):
    logger.info("Starting weight distribution calculations related to DWH and KPR systems.")
    
    # Step 1: Prepare weight data by reading DWH data
    logger.info("Start preparing weight data using DWH files.")
    prod_gewicht_preparation(logger, dwh_files, calc_files)
    
    # Step 2: Calculate weight distribution (if necessary for KPR flow)
    logger.info("Start calculating weight distribution.")
    prod_gewicht2verteilung(logger, calc_files=calc_files)
    
    # Optional: Insert the results into HANA Cloud if needed for further analysis or reporting
    logger.info("Inserting the weight distribution data into HANA Cloud.")
    connection = connect_to_hana()
    cursor = connection.cursor()

    sql = """
        INSERT INTO SHIPTOPROFILE_KONTRAKT_SERV (column1, column2, ...)
        VALUES (?, ?, ...)
    """
    cursor.executemany(sql, data_to_insert)  # Assuming data_to_insert contains data for HANA insertion
    connection.commit()
    cursor.close()
    connection.close()

    logger.info("Weight distribution data successfully inserted into HANA Cloud.")
    logger.info("Finished weight distribution calculations.")


def prod_gewicht_preparation(logger: logging.Logger, dwh_files: DwhFiles, calc_files: CalculatedFiles) -> None:
    """
    Prepares weight distribution data by reading from DWH and merging it with relevant mappings for KPR processing.
    """
    logger.info("Reading product weight data from DWH files.")
    df_prod_gewicht = read_df(dwh_files.df_prod_gewicht)  # Reading from DWH
    
    logger.info("Reading and merging mapping files for KPR.")
    df_mapping = read_df(calc_files.df_mapping)  # Reading mapping for further processing
    df_mapping = df_mapping[['abrnr', 'kalknr']]  # Keep only relevant columns (abrnr, kalknr)
    
    # Merging DWH data with mapping
    df = pd.merge(df_prod_gewicht, df_mapping, how="left", on=["abrnr"])
    
    # Grouping by relevant fields (ekpnr, kalknr)
    logger.info("Grouping weight data by KPR and relevant identifiers.")
    df.groupby(["ekpnr", "kalknr"], as_index=False).sum()
    
    # Calculating weight distribution by staffel (weight classes)
    gewicht_staffel_cols = [c for c in df.columns if ("gewicht_bis" in c) or ("gewicht_ue" in c)]
    df["anz_sdg"] = df[gewicht_staffel_cols].sum(axis=1)
    
    df["gewicht_avg"] = round(df.gewicht_sum / df.anz_sdg, 1)  # Average weight
    
    # Calculating percentage contribution for each staffel
    for staffel in gewicht_staffel_cols:
        df[f"anteil_{staffel}"] = round(df[staffel] / df.anz_sdg, 6)
    
    df.dropna(subset="kalknr", inplace=True)  # Drop rows without kalknr
    
    # Save the prepared data
    logger.info("Saving the prepared weight distribution data.")
    save_df(calc_files.df_prod_gewicht_prepared, df)


def prod_gewicht2verteilung(logger: logging.Logger, calc_files: CalculatedFiles):
    """
    Calculates the weight distribution based on prepared data and stores the results.
    """
    logger.info("Reading the prepared weight data for distribution calculation.")
    df_prod_gewicht = read_df(calc_files.df_prod_gewicht_prepared)
    
    # Selecting relevant columns
    anteil_columns = df_prod_gewicht.columns[df_prod_gewicht.columns.str.contains("anteil_")]
    
    logger.info(f"Weight data sample: \n{df_prod_gewicht.head()}")

    # Grouping by average weight and calculating sum of contributions
    df_gewicht2verteilung = df_prod_gewicht.groupby(["gewicht_avg"], as_index=False)[anteil_columns].sum()
    df_gewicht2verteilung = df_gewicht2verteilung[~df_gewicht2verteilung["gewicht_avg"].isna()]
    df_gewicht2verteilung["anz_kunde"] = round(df_gewicht2verteilung[anteil_columns].sum(axis=1), 0)
    
    # Calculating estimated contribution for each staffel
    for col in anteil_columns:
        col_est = col + "_est"
        df_gewicht2verteilung[col_est] = round(df_gewicht2verteilung[col] / df_gewicht2verteilung["anz_kunde"], 6)
    
    df_gewicht2verteilung.rename(columns={"gewicht_avg": "gewicht_avg_est"}, inplace=True)
    df_gewicht2verteilung = df_gewicht2verteilung[
        df_gewicht2verteilung.columns[df_gewicht2verteilung.columns.str.contains("_est|anz_|ekpnr|kalknr")]
    ]
    
    logger.info(f"Calculated weight distribution sample: \n{df_gewicht2verteilung.head()}")
    logger.info(f"Total number of customers: {df_gewicht2verteilung['anz_kunde'].sum()}")

    # Save the calculated weight distribution
    logger.info("Saving the calculated weight distribution data.")
    save_df(calc_files.df_gewicht2verteilung, df_gewicht2verteilung)
