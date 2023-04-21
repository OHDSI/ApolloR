from typing import Dict
import cProfile

import pandas as pd

from abstract_cdm_processor import AbstractToParquetCdmDataProcessor 
import cdm_processor_utils as cpu

PERSON = "person"

class ExampleCdmDataProcessor(AbstractToParquetCdmDataProcessor):
    """
    A silly implementation of AbstractToParquetCdmDataProcessor, to demonstrate
    its use.
    """

    def _process_person(self, person_id: int, cdm_tables: Dict[str, pd.DataFrame]):
        cpu.call_per_observation_period(cdm_tables=cdm_tables, function=self._process_observation_period)

    def _process_observation_period(self, observation_period_id: int, cdm_tables: Dict[str, pd.DataFrame]):
        self._output.append(cdm_tables[PERSON])
        #event_table = cpu.union_domain_tables(cdm_tables=cdm_tables)
        #self._output.append(event_table)
        # print(observation_period_id)

if __name__ == "__main__":
    
    my_cdm_data_processor = ExampleCdmDataProcessor(
        cdm_data_path="d:/GPM_CCAE",
        max_cores=10,
        output_path="d:/GPM_CCAE/person_sequence",
    )
    my_cdm_data_processor.process_cdm_data()
    # Profiling code:
    # my_cdm_data_processor._max_cores = -1
    # cProfile.run("my_cdm_data_processor.process_cdm_data()", "stats")