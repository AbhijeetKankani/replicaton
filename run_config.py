import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# für Funktion des Codes immer auf den Monatsanfang setzen
reference_date = datetime(2024, 5, 1).date()

run_name = "ship2profile_202407_kalknr"

frozen_zone = pd.to_datetime(reference_date + relativedelta(months=-3), format="%Y-%m-%d")

td_config = {
    "driver": "{/opt/teradata/client/ODBC_64/lib/tdataodbc_sb64.so}",
    "dbcname": "tdprd",
    "user_name": "OA1B_BI_AWB19_PRD",
    "td_wallet": "$tdwallet(OA1B_BI_AWB19_PRD)",
    "project_dir": "./",
    "production_mode": "custom",
}
