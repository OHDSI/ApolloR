from typing import Dict, Callable
import datetime as dt

import pandas as pd

OBSERVATION_PERIOD = "observation_period"
OBSERVATION_PERIOD_ID = "observation_period_id"
OBSERVATION_PERIOD_START_DATE = "observation_period_start_date"
OBSERVATION_PERIOD_END_DATE = "observation_period_end_date"
PERSON_ID = "person_id"
START_DATE_FIELDS = {
    "observation_period": "observation_period_start_date",
    "visit_occurrence": "visit_start_date",
    "condition_occurrence": "condition_start_date",
    "drug_exposure": "drug_exposure_start_date",
    "procedure_occurrence": "procedure_date",
    "device_exposure": "device_exposure_start_date",
    "measurement": "measurement_date",
    "observation": "observation_date",
    "death": "death_date",
}
DOMAIN_TABLES = [
    #    "visit_occurrence",
    "condition_occurrence",
    "drug_exposure",
    "procedure_occurrence",
    "device_exposure",
    "measurement",
    "observation",
    "death",
]
DEATH = "death"
DEATH_CONCEPT_ID = 4306655
CONCEPT_ID = "concept_id"
COLUMN_MAPPING = {
    "condition_occurrence": {
        "condition_concept_id": "concept_id",
        "condition_start_date": "start_date",
    },
    "drug_exposure": {
        "drug_concept_id": "concept_id",
        "drug_exposure_start_date": "start_date",
    },
    "procedure_occurrence": {
        "procedure_concept_id": "concept_id",
        "procedure_date": "start_date",
    },
    "device_exposure": {
        "device_concept_id": "concept_id",
        "device_exposure_start_date": "start_date",
    },
    "measurement": {
        "measurement_concept_id": "concept_id",
        "measurement_date": "start_date",
    },
    "observation": {
        "observation_concept_id": "concept_id",
        "observation_date": "start_date",
    },
    "death": {"death_date": "start_date"},
}
YEAR_OF_BIRTH = "year_of_birth"
MONTH_OF_BIRTH = "month_of_birth"
DAY_OF_BIRTH = "day_of_birth"


def call_per_observation_period(
    cdm_tables: Dict[str, pd.DataFrame],
    function: Callable[[int, Dict[str, pd.DataFrame]], None],
):
    """
    Calls the provided function for each observation period. CDM tables are filtered to only those events
    that fall in the observation period. he function should have two arguments: the observation_period_id,
    and a dictionary of CDM tables.
    """
    for index, observation_period in cdm_tables[OBSERVATION_PERIOD].iterrows():
        observation_period_start_date = observation_period[
            OBSERVATION_PERIOD_START_DATE
        ]
        observation_period_end_date = observation_period[OBSERVATION_PERIOD_END_DATE]
        new_cdm_tables = {}
        for table_name, table in cdm_tables.items():
            if table_name in START_DATE_FIELDS:
                start_dates = table[START_DATE_FIELDS[table_name]]
                table = table[
                    (start_dates >= observation_period_start_date)
                    & (start_dates <= observation_period_end_date)
                ]
            new_cdm_tables[table_name] = table
        function(observation_period[OBSERVATION_PERIOD_ID], new_cdm_tables)


def normalize_domain_table(table: pd.DataFrame, table_name: str) -> pd.DataFrame:
    mapping = COLUMN_MAPPING[table_name]
    table = table[list(mapping.keys()) + [PERSON_ID]]
    table = table.rename(columns=mapping)
    # Death has no concept ID field, but seems important to keep so assigning standard concept ID for 'Death'.
    if table_name == DEATH:
        table[CONCEPT_ID] = DEATH_CONCEPT_ID
    return table


def union_domain_tables(cdm_tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Combines all domain tables into a single table. For this, column names will be normalized first.
    """
    result = []
    for table_name in DOMAIN_TABLES:
        if table_name in cdm_tables:
            result.append(normalize_domain_table(cdm_tables[table_name], table_name))
    return pd.concat(result, ignore_index=True)


def get_date_of_birth(person: pd.DataFrame) -> dt.datetime:
    """
    Computes a date of birth from a single person entry
    """
    if person.shape[0] != 1:
        raise Exception(
            f"Expecting person to have 1 row, but found {person.shape[0]} rows"
        )
    year = person[YEAR_OF_BIRTH].iat[0]
    month = person[MONTH_OF_BIRTH].iat[0]
    day = person[DAY_OF_BIRTH].iat[0]
    if pd.isna(month):
        month = 1
    if pd.isna(day):
        day = 1
    return dt.date(year=int(year), month=int(month), day=int(day))
