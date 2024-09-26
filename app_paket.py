import logging
from src.project_path import DATA_ROOT_FOLDER
from src.run_config import run_name, reference_date
from src.utils import dwh_tables, files, logger

from src.input import input_dwh, input_kpr
from src.calculation import (
    calc_abrechungsnr,
    calc_ist_kpr,
    calc_soll_estimate_kpr,
    calc_weight_distribution,
    calc_kpr_ekp_data,
)


def run():
    product = "paket"
    product_data = DATA_ROOT_FOLDER / f"{product}"

    # Part 1 INPUT Data
    calc_tables = dwh_tables.CalculatedTables(run_name=run_name)

    # Logging setup for KPR
    kpr_files = files.KprFiles(in_data_path=None, df_data_path=f"{product_data}/data")
    kpr_logger = logger.setup_logger(
        f"{product}_kpr_logger", DATA_ROOT_FOLDER / f"{product}_{run_name}_kpr.log", level=logging.DEBUG
    )
    kpr_files.log(kpr_logger)

    # Load input data from KPR
    input_kpr.input_kpr(
        logger=kpr_logger,
        kpr_files=kpr_files,
        calc_tables=calc_tables,
        reference_date=reference_date,
        product=product,
    )

    # Logging setup for DWH
    dwh_files = files.DwhFiles(in_data_path="", df_data_path=f"{product_data}/data")
    dwh_logger = logger.setup_logger(
        f"{product}_dwh_logger", DATA_ROOT_FOLDER / f"{product}_{run_name}_dwh.log", level=logging.DEBUG
    )
    dwh_files.log(dwh_logger)

    # Load input data from DWH
    input_dwh.input_dwh(dwh_logger, dwh_files, calc_tables, reference_date)

    # PART 2 Data Aggregation and Calculation
    calc_logger = logger.setup_logger(
        f"{product}_calc_logger", DATA_ROOT_FOLDER / f"{product}_{run_name}_calc.log", level=logging.DEBUG
    )
    calc_files = files.CalculatedFiles(in_data_path="", df_data_path=f"{product_data}/data")

    # Perform calculations specific to KPR and DWH
    calc_abrechungsnr.ist_abrechnungsnr(
        logger=calc_logger, calc_files=calc_files, calc_tables=calc_tables, reference_date=reference_date
    )

    calc_ist_kpr.calc_ist_kpr(kpr_files=kpr_files, calc_files=calc_files, logger=calc_logger, level=["ekpnr", "kalknr"])

    calc_soll_estimate_kpr.calc_soll_estimate_kpr_ekp(
        kpr_files=kpr_files, calc_files=calc_files, logger=calc_logger, product=product
    )

    calc_weight_distribution.calc_weight_distribution(logger=calc_logger, dwh_files=dwh_files, calc_files=calc_files)

    calc_kpr_ekp_data.calc_kpr_ekp_data(kpr_files=kpr_files, calc_files=calc_files)

    # PART 3 Insert into HANA Cloud (if applicable)
    # Here you can add the insert operation into HANA cloud with the aggregated data
    # Example:
    # insert_into_prima_price_delta(data)

    # Additional result handling can be added as needed
