import collections
import os
import re
from dataclasses import dataclass, field, fields
from glob import glob
from typing import Optional

@dataclass
class FileContainer:
    """
    File container to package files for a specific part of the KPR/DWH process
    """

    in_data_path: str
    df_data_path: str  # all Data with prefix "df_" are calculated during the process

    def __post_init__(self):
        """
        Checks if fieldname of class is a dict or not. If dict, then loops through all entries and joins filepath.
        """
        for entry in fields(self):
            filename = getattr(self, entry.name)
            if (entry.name != "in_data_path") & (entry.name != "df_data_path"):
                if isinstance(filename, dict):
                    for key, value in filename.items():
                        filename[key] = self.__join_filepath(value)
                else:
                    filename = self.__join_filepath(filename)
                setattr(self, entry.name, filename)

    def __join_filepath(self, file):
        """
        Joins the filepath and checks if the file exists.
        """
        filepath = ""
        if file is not None:
            if ("df" not in file) and (self.in_data_path):
                filepath = os.path.join(self.in_data_path, f"{file}")
                searchstring = os.path.join(self.in_data_path, f"*{file}*")
                filepath = glob(searchstring)[0] if glob(searchstring) else filepath + ".parquet"
                if not os.path.isfile(filepath):
                    raise FileNotFoundError(f"{filepath} does not exist!")
            elif "df" in file:
                filepath = os.path.join(self.df_data_path, f"{file}.parquet")
            return filepath

    def files_exist(self, logger):
        """
        Checks if all files exist and logs their presence.
        """
        for entry in fields(self):
            filename = getattr(self, entry.name)
            if entry.name != "path":
                if isinstance(filename, dict):
                    for key, value in filename.items():
                        logger.info(f"{value} exists: {os.path.isfile(value)}")
                else:
                    logger.info(f"{filename} exists: {os.path.isfile(filename)}")

    def log(self, logger):
        logger.info(f"{self}")


@dataclass
class KprFiles(FileContainer):
    df_kpr_costs_report15: str = "df_kpr_costs_report15"
    df_kpr_costs_report16: str = "df_kpr_costs_report16"
    df_kpr_costs_report16_6m: str = "df_kpr_costs_report16_6m"
    df_kpr_costs_report17: str = "df_kpr_costs_report17"
    df_kpr_costs_report17_6m: str = "df_kpr_costs_report17_6m"
    df_kpr_costs_report18: str = "df_kpr_costs_report18"
    df_kpr_kosten: str = "df_kpr_kosten"
    df_kpr_treiber: str = "df_kpr_treiber"
    df_kpr_zustellung: str = "df_kpr_zustellung"
    df_mapping_rv_abrnr: str = "df_mapping_rv_abrnr"


@dataclass
class DwhFiles(FileContainer):
    df_prod_gewicht: str = "df_prod_gewicht"
    df_kunden_seit: str = "df_kunden_seit"


@dataclass
class CalculatedFiles(FileContainer):
    df_sh2pr_12M_abrnr: str = "df_sh2pr_12M_abrnr"
    df_ist_abrnr_multiple_kundenseit: str = "df_ist_abrnr_multiple_kundenseit"
    df_fibu_preisliste_unique: str = "df_fibu_preisliste_unique"
    df_ist_kpr_abrnr: str = "df_ist_kpr_abrnr"
    df_sh2pr_calculation_reference: str = "df_sh2pr_calculation_reference"
    df_soll_estimate: str = "df_soll_estimate"
    df_saisonal_distribution: str = "df_saisonal_distribution"
    df_gewicht2verteilung: str = "df_gewicht2verteilung"
    df_raummass_check: str = "df_raummass_check"
    df_kpr_ekp_data: str = "df_kpr_ekp_data"

    df_mapping: str = "df_mapping"
    df_prod_gewicht_prepared: str = "df_prod_gewicht_prepared"


@dataclass
class ResultFiles(FileContainer):
    sh2pr_manual_selection: str = "Kunden mit Kalkulation"

    df_tables_joined: str = "df_sh2pr_joined"
    df_sh2pr_calculated: str = "df_sh2pr_calculated"
    df_sh2pr_calculated_export: str = "df_sh2pr_calculated_export"
    df_sh2pr_final: str = "df_sh2pr_final"
    df_kt_auto_calculation: str = "df_kt_auto_calculation"


@dataclass
class GeneralFiles(FileContainer):
    geo_distance: str = "geo_distance_dhl"

