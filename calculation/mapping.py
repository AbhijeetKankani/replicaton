import pandas as pd
from src.utils.files import CalculatedFiles, KprFiles
from src.utils.utils import read_df, save_df


def prepare_abr_kalknr_mapping(calc_files: CalculatedFiles, sap_files: KprFiles) -> None:
    """
    This function prepares the mapping of Abrechnr to Kalknr using KPR files and saves the results.
    """

    # Reading relevant data from KPR files
    df_rv_abrnr_mapping = read_df(sap_files.df_kontrakt)

    # Mapping ekpnr and kalknr from the kontrakt file (KPR source)
    df_ref = df_rv_abrnr_mapping[["ekpnr", "kalknr", "abrnr"]].copy()
    df_ref.sort_values(["abrnr"], ascending=[True], inplace=True)

    # Filter based on ekpnr values (if necessary for your system)
    df_mapping = df_ref[
        (df_ref["ekpnr"].astype(int) <= 7000000000) & (df_ref["ekpnr"].astype(int) >= 5000000000)
    ]

    # Extracting Verfa (procedure code) from abrnr
    df_mapping['verfa'] = df_mapping.abrnr.str[10:12]

    # Save the prepared mapping data
    save_df(calc_files.df_mapping, df_mapping)

