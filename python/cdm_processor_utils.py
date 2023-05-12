from typing import Dict, List, Callable
import datetime as dt

import pandas as pd
import numpy as np

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
VISIT_OCCURRENCE = "visit_occurrence"
VISIT_OCCURRENCE_ID = "visit_occurrence_id"
VISIT_START_DATE = "visit_start_date"
VISIT_END_DATE = "visit_end_date"
VISIT_CONCEPT_ID = "visit_concept_id"


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
    if len(result) == 0:
        print("check")
    return pd.concat(result, ignore_index=True)


def get_date_of_birth(person: pd.DataFrame) -> dt.date:
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


class VisitData:
    """
    Class for grouping all CDM data for one visit.
    """
    visit: pd.Series
    visit_start_date: dt.date
    cdm_tables: Dict[str, pd.DataFrame]

    def __init__(self, visit: pd.Series):
        self.visit = visit
        self.cdm_tables = {}
        self.visit_start_date = visit[VISIT_START_DATE]

def group_by_visit(
    cdm_tables: Dict[str, pd.DataFrame],
    link_by_date: bool = True,
    create_missing_visits: bool = True,
    missing_visit_concept_id: int = 0,
) -> List[VisitData]:
    """
    Groups events by visit.

    Args:
        link_by_date: If true, events not linked to an existing visit by visit_occurrence_id
                      will be linked to an existing visit if the event date falls within the
                      visit start and end date.
        create_missing_visits: If no visit exists with dates corresponding to an event, a new
                               one-day visit will be created.
        missing_visit_concept_id: The visit_concept_id to be used for newly created visits if
                                  create_missing_visits is true.

    Yields:
        A list of type VisitData, sorted by visit start date.
    """
    if (VISIT_OCCURRENCE in cdm_tables):
        visits = cdm_tables[VISIT_OCCURRENCE]
    else:
        visits = pd.DataFrame()
    visit_indices = list(range(len(visits)))
    visit_datas = [VisitData(visits.iloc[i]) for i in range(len(visits))]
    for table_name in DOMAIN_TABLES:
        if table_name in cdm_tables:
            cdm_table = cdm_tables[table_name]
            if len(cdm_table) == 0:
                continue
            start_date_field = START_DATE_FIELDS[table_name]
            if (len(visits) == 0):
                event_visit_index = np.empty(shape=len(cdm_table), dtype=np.int32)
                event_visit_index.fill(-1)
            else:
                if link_by_date:
                    if VISIT_OCCURRENCE_ID in cdm_table:
                        #print(cdm_table[VISIT_OCCURRENCE_ID].values)
                        #print(visits[VISIT_OCCURRENCE_ID].values)
                        event_visit_index = np.piecewise(
                            [0] * len(cdm_table),
                            [
                                (
                                    cdm_table[VISIT_OCCURRENCE_ID].values
                                    == visit_occurrence_id
                                )
                                | ((cdm_table[start_date_field].values >= start_date)
                                & (cdm_table[start_date_field].values <= end_date))
                                for visit_occurrence_id, start_date, end_date in zip(
                                    visits[VISIT_OCCURRENCE_ID].values,
                                    visits[VISIT_START_DATE].values,
                                    visits[VISIT_END_DATE].values,
                                )
                            ],
                            np.append(visit_indices, -1),
                        )
                    else:
                        event_visit_index = np.piecewise(
                            [0] * len(cdm_table),
                            [
                                (cdm_table[start_date_field].values >= start_date)
                                & (cdm_table[start_date_field].values <= end_date)
                                for start_date, end_date in zip(
                                    visits[VISIT_START_DATE].values,
                                    visits[VISIT_END_DATE].values,
                                )
                            ],
                            np.append(visit_indices, -1),
                        )
                elif VISIT_OCCURRENCE_ID in cdm_table:
                    event_visit_index = np.piecewise(
                        [0] * len(cdm_table),
                        [
                            (cdm_table[VISIT_OCCURRENCE_ID].values == visit_occurrence_id)
                            for visit_occurrence_id in zip(
                                visits[VISIT_OCCURRENCE_ID].values
                            )
                        ],
                        np.append(visit_indices, -1),
                    )
            if create_missing_visits:
                idx = event_visit_index == -1
                if (any(idx)):
                    dates = cdm_table.loc[idx, start_date_field].unique()
                    person_id = cdm_table[PERSON_ID].iat[0]
                    missing_visit_indices = list(range(len(visits), len(visits) + len(dates)))
                    missing_visits = pd.DataFrame(
                        {
                            PERSON_ID: [person_id] * len(dates),
                            VISIT_OCCURRENCE_ID: [np.NAN] * len(dates),
                            VISIT_CONCEPT_ID: [missing_visit_concept_id] * len(dates),
                            VISIT_START_DATE: dates,
                            VISIT_END_DATE: dates
                        }
                    )
                    event_visit_index[idx] = np.piecewise(
                        [0] * sum(idx),
                        [
                            (cdm_table.loc[idx, start_date_field].values == start_date)
                            for start_date in zip(
                                missing_visits[VISIT_START_DATE].values
                            )
                        ],
                        missing_visit_indices,
                    )
                    visits = pd.concat([visits, missing_visits])
                    visit_indices.extend(missing_visit_indices)
                    visit_datas += [VisitData(missing_visits.iloc[i]) for i in range(len(missing_visits))]
            else:
                idx = event_visit_index != -1
                cdm_table = cdm_table[idx]
                event_visit_index = event_visit_index[idx]
            
            for visit_index, events in cdm_table.groupby(event_visit_index):
                visit_datas[visit_index].cdm_tables[table_name] = events 
    visit_datas.sort(
        key=lambda x: x.visit_start_date
    )
    return visit_datas
