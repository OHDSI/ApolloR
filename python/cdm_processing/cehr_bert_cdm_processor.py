from typing import Dict, List
import math
import datetime as dt
import cProfile
import os

import pandas as pd

from abstract_cdm_processor import AbstractToParquetCdmDataProcessor
import cdm_processor_utils as cpu

PERSON = "person"
START_DATE = "start_date"
CONCEPT_ID = "concept_id"
DRUG_EXPOSURE = "drug_exposure"
DRUG_CONCEPT_ID = "drug_concept_id"
VISIT_START = "VS"
VISIT_END = "VE"
EPOCH = dt.date(1970, 1, 1)

def _create_interval_token(days: int) -> str:
    if days < 0:
        return "W-1"
    if days < 28:
        return f"W{str(math.floor(days / 7))}"
    if days < 360:
        return f"M{str(math.floor(days / 30))}"
    return "LT"


def days_to_weeks(days: int) -> int:
    return math.floor(days / 7)


def days_to_months(days: int) -> int:
    return math.floor(days / 30.5)


class CehrBertCdmDataProcessor(AbstractToParquetCdmDataProcessor):
    """
    A re-implementation of the processor for CEHR-BERT (https://github.com/cumc-dbmi/cehr-bert)
    """

    def _prepare(self):
        super()
        self._drug_mapping = cpu.load_mapping_to_ingredients(self._cdm_data_path)

    def _process_person(self, person_id: int, cdm_tables: Dict[str, pd.DataFrame]):
        cpu.call_per_observation_period(
            cdm_tables=cdm_tables, function=self._process_observation_period
        )

    def _process_observation_period(
        self, observation_period: pd.Series, cdm_tables: Dict[str, pd.DataFrame]
    ):
        # Map drugs to ingredients:
        if DRUG_EXPOSURE in cdm_tables:
            cdm_tables[DRUG_EXPOSURE] = (
                cdm_tables[DRUG_EXPOSURE]
                .join(self._drug_mapping, on=DRUG_CONCEPT_ID, how="inner", rsuffix="_right")
                .drop([DRUG_CONCEPT_ID, DRUG_CONCEPT_ID+"_right"], axis=1)
                .rename(columns={"ingredient_concept_id": DRUG_CONCEPT_ID})
            )
        date_of_birth = cpu.get_date_of_birth(person=cdm_tables[PERSON].iloc[0])
        concept_ids = []
        visit_segments = []
        dates = []
        ages = []
        visit_concept_orders = []
        visit_concept_ids = []
        previous_visit_end_date: dt.date
        visit_rank = 0
        for visit_group in cpu.group_by_visit(
            cdm_tables=cdm_tables,
            link_by_date=True,
            create_missing_visits=True,
            missing_visit_concept_id=0,
        ):
            visit_rank += 1
            if visit_rank > 1:
                # Add interval token:
                interval_token = _create_interval_token(
                    (visit_group.visit_start_date - previous_visit_end_date).days
                )
                concept_ids.append(interval_token)
                visit_segments.append(0)
                dates.append(0)
                ages.append(-1)
                visit_concept_orders.append(visit_rank + 1)
                visit_concept_ids.append(0)
            visit_end_date = visit_group.visit["visit_end_date"]
            event_table = cpu.union_domain_tables(visit_group.cdm_tables)
            event_table.sort_values(
                [START_DATE, CONCEPT_ID], ascending=True, inplace=True
            )
            visit_token_len = len(event_table) + 2
            concept_ids.append(VISIT_START)
            concept_ids.extend(event_table[CONCEPT_ID].astype(str).to_list())
            concept_ids.append(VISIT_END)
            visit_segments.extend([visit_rank % 2 + 1] * visit_token_len)
            dates.append(days_to_weeks((visit_group.visit_start_date - EPOCH).days))
            dates.extend(
                event_table[START_DATE].apply(lambda x: days_to_weeks((x - EPOCH).days))
            )
            dates.append(days_to_weeks((visit_end_date - EPOCH).days))
            ages.append(
                days_to_months((visit_group.visit_start_date - date_of_birth).days)
            )
            ages.extend(
                event_table[START_DATE].apply(
                    lambda x: days_to_months((x - date_of_birth).days)
                )
            )
            ages.append(days_to_months((visit_end_date - date_of_birth).days))
            visit_concept_orders.extend([visit_rank] * visit_token_len)
            visit_concept_ids.extend(
                [visit_group.visit[cpu.VISIT_CONCEPT_ID]] * visit_token_len
            )
            previous_visit_end_date = visit_end_date

        orders = list(range(1, len(concept_ids)))
        output_row = pd.Series(
            {
                "cohort_member_id": observation_period[cpu.OBSERVATION_PERIOD_ID],
                "person_id": observation_period[cpu.PERSON_ID],
                "concept_ids": concept_ids,
                "visit_segments": visit_segments,
                "orders": orders,
                "dates": dates,
                "ages": ages,
                "visit_concept_orders": visit_concept_orders,
                "num_of_visits": visit_rank,
                "num_of_concepts": len(concept_ids),
                "visit_concept_ids": visit_concept_ids,
            }
        )
        self._output.append(output_row)


if __name__ == "__main__":
    print(os.getcwd())
    my_cdm_data_processor = CehrBertCdmDataProcessor(
        cdm_data_path="d:/GPM_MDCD",
        max_cores=-1,
        output_path="d:/GPM_MDCD/person_sequence",
    )
    my_cdm_data_processor.process_cdm_data()
    # Profiling code:
    # my_cdm_data_processor._max_cores = -1
    # cProfile.run("my_cdm_data_processor.process_cdm_data()", "stats")
