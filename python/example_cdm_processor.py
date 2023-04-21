from typing import Dict
import math
import cProfile

import pandas as pd

from abstract_cdm_processor import AbstractToParquetCdmDataProcessor 
import cdm_processor_utils as cpu

PERSON = "person"
START_DATE = "start_date"
CONCEPT_ID = "concept_id"

class ExampleCdmDataProcessor(AbstractToParquetCdmDataProcessor):
    """
    A silly implementation of AbstractToParquetCdmDataProcessor, to demonstrate
    its use.
    """

    def _process_person(self, person_id: int, cdm_tables: Dict[str, pd.DataFrame]):
        cpu.call_per_observation_period(cdm_tables=cdm_tables, function=self._process_observation_period)

    def _process_observation_period(self, observation_period_id: int, cdm_tables: Dict[str, pd.DataFrame]):
        date_of_birth = cpu.get_date_of_birth(person=cdm_tables[PERSON])
        event_table = cpu.union_domain_tables(cdm_tables=cdm_tables)
        event_table.sort_values([START_DATE, CONCEPT_ID], ascending=True, inplace=True)
        age_in_weeks = event_table[START_DATE].apply(lambda x: math.floor((x-date_of_birth).days / 7))
        print(age_in_weeks)
        #self._output.append(event_table)


if __name__ == "__main__":
    
    my_cdm_data_processor = ExampleCdmDataProcessor(
        cdm_data_path="d:/GPM_MDCD",
        max_cores=1,
        output_path="d:/GPM_MDCD/person_sequence",
    )
    my_cdm_data_processor.process_cdm_data()
    # Profiling code:
    # my_cdm_data_processor._max_cores = -1
    # cProfile.run("my_cdm_data_processor.process_cdm_data()", "stats")