import logging
from pathlib import Path
from typing import Dict, List, Union

import numpy as np
import pandas as pd

from src.run_config import reference_date

def read_df(path: Union[Path, str], skiprows: int = None, header: str = "infer"):
    if ~isinstance(path, Path):
        path = Path(path)

    if path.suffix in (".csv", ".txt"):
        df = pd.read_csv(
            path,
            low_memory=False,
            quotechar='"',
            decimal=",",
            sep=";",
            encoding="utf-8",
            dtype=str,
            skiprows=skiprows,
            header=header,
        )
    elif ".parquet" == path.suffix:
        df = pd.read_parquet(path)

    return df


def save_df(path: Union[Path, str], df_to_save: pd.DataFrame):
    df_to_save.to_parquet(path, index=False)


def read_dfs(files: Dict[str, str], logger: logging.Logger) -> Dict[str, pd.DataFrame]:
    df_dict = {}
    for key, path in files.items():
        df_dict[key] = read_df(path)
        logger.info(f"df {key} shape {df_dict[key].shape}")
    return df_dict


def normalize_code(df: pd.DataFrame, columns: dict = {"ekpnr": 10}) -> pd.DataFrame:
    for col, width in columns.items():
        df[col] = df[col].apply(lambda x: str(x).zfill(width) if not str(x).isspace() else " ")
        df[col] = np.where(df[col] == ("0" * width), " ", df[col])
    return df


def strip(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        df[col] = df[col].astype(str).str.lstrip().str.rstrip()
    return df


def exclude_abrnr(df_input: pd.DataFrame, df_abrnr: pd.DataFrame, logger: logging.Logger):
    abrnr_set = set(df_abrnr["abrnr"].values)
    df = df_input.loc[~df_input.abrnr.isin(abrnr_set)]
    logger.info(f"Removed {df_input.shape[0] - df.shape[0]} entries")
    return df


def log_df_string(df, columns=[], start_string="df "):
    string = f"{start_string} "
    string += f"shape {df.shape}"
    for col in columns:
        if isinstance(col, str):
            string += f" | unique '{col}': {df[col].nunique()}"
        elif len(col) == 2:
            string += f" | unique '{col[0]}'+'{col[1]}': {(df[col[0]] + '|' + df[col[1]]).nunique()}"
        elif len(col) == 3:
            string += f" | unique '{col[0]}'+'{col[1]}'+'{col[2]}': {(df[col[0]] + '|' + df[col[1]] + '|' + df[col[2]]).nunique()}"
    return string


def monthdelta(delta, date=reference_date):
    month = (date.month + delta) % 12
    year = date.year + (date.month + delta - 1) // 12
    if month == 0:
        month = 12
    return year * 100 + month


def cast_types(df: pd.DataFrame, column_types: dict) -> None:
    for col in df.columns:
        if col in column_types.keys():
            df[col] = df[col].astype(column_types[col])


def setup_identifier_column(data: pd.DataFrame, identifier_columns: List) -> None:
    if len(identifier_columns) > 1:
        data["identifier"] = data[identifier_columns].astype(str).agg("-".join, axis=1)
    else:
        data["identifier"] = data[identifier_columns].astype(str)


def special_round(value: int):
    return np.around(value, -len(str(value)) + 2)
