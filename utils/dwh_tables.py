from dataclasses import dataclass

# Relevant static tables for KPR and DWH
STATIC_TABLES =  {
    "kpr_db": "DB_KPR_PLT.vw_f_db",
    "kpr_kosten": "DB_KPR_PLT.vw_f_kosten",
    "kpr_produkte": "DBT_KPR_PLT.tb_produkte",
    "kpr_treiber": "DB_KPR_PLT.vw_f_treiber",
    "kpr_treiber_wapo": "DBT_KPR_PLT.tb_f_WaPo_Treiber",
    "kpr_kunde_konzern": "DBT_KPR_PLT.tb_d_kunde_konzern",
    "kpr_kosten_drop": "DBT_KPR_PLT.vw_kosten_kt",
    "kpr_rv": "DBT_KPR_PLT.tb_globuss_rv",
    "pze_table": "DB_NEXTT.PZE_EVENT",
    "pan_table": "DB_NEXTT.VW_PAN_MSG_SHIP",
    "pem_table": "DBX_DWH_SBX_SAS_GB20_002_PRD.vw_piece_event_mapping",
    "sort_table": "DBX_DWH_SBX_SAS_GB20_002_PRD.VW_B_EVT_SORTIERUNG_ALL",
    "kunde": "DB_CDS_DWH.vw_kunde",
} 

# STP (Ship-to-Profile) tables related to volume metrics
STP_TABLES = {
    "monthly_volume": "DBX_DWH_SBX_GB30_PRD.stp_monthly_volume",
    "weekly_volume": "DBX_DWH_SBX_GB30_PRD.stp_weekly_volume",
}


@dataclass
class CalculatedTables:
    run_name: str
    schema: str = "DBX_DWH_SBX_GB30_PRD"

    # Only KPR and DWH-related calculated tables
    tables = {
        "kunde_unique",
        "kunde_konzern_unique",
        "kunde_konzern_neu",
        "kunden_seit",
        "kt_abr_aktionsgeschaeft",
        "kt_abr_kleinpaket",
    }

    # Primary index lookup for calculated tables
    primary_index_lookup = {
        "kunde_unique": "(kunnr)",
        "kunde_konzern_unique": "(ag_ekp)",
        "kunde_konzern_neu": "(ekpnr)",
        "kunden_seit": "(abrnr, ekpnr)",
        "kt_abr_aktionsgeschaeft": "(ekpnr)",
        "kt_abr_kleinpaket": "(ekpnr)",
    }

    def get_table(self, table):
        """
        Retrieve the full table name based on schema and run name.
        """
        if table in self.tables:
            return f"{self.schema}.{self.run_name}_{table}"
        else:
            raise ValueError(f"{table} does not exist!")

    def get_index(self, table):
        """
        Retrieve the primary index for a specific table.
        """
        if table in self.primary_index_lookup:
            return self.primary_index_lookup[table]
        else:
            raise ValueError(f"Primary index for {table} does not exist!")
