from abc import ABC, abstractmethod
from multiprocessing import Pool
import os
from typing import List, Dict
import logging

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from utils.logger import create_logger

PERSON = "person"
CDM_TABLES = [
    "observation_period",
    "visit_occurrence",
    "condition_occurrence",
    "drug_exposure",
    "procedure_occurrence",
    "device_exposure",
    "measurement",
    "observation",
    "death",
]
PERSON_ID = "person_id"
LOGGER_FILE_NAME = "cdm_processing_log.txt"


class AbstractCdmDataProcessor(ABC):
    """
    An abstract class that implements iterating over partitioned data as generated by the
    GeneralPretrainedModelTools R package. It divides the partitions over various threads,
    and calls the abstract _process_person() function with all data for a single person, for
    each person in the data.

    Args:
        cdm_data_path: The path where the CDM Parquet files are saved (using the GeneralPretrainModelTools packages).
        max_cores: The maximum number of CPU cores to use. If set to -1, all multihreading code will be bypassed for
                   easier debugging.
    """

    def __init__(self, cdm_data_path: str, output_path: str, max_cores: int = -1):
        self._cdm_data_path = cdm_data_path
        self._max_cores = max_cores
        self._person_partition_count = 0
        self._output_path = output_path
        self._configure_logger()

    def _configure_logger(self):
        create_logger(os.path.join(self._output_path, LOGGER_FILE_NAME))

    @abstractmethod
    def _prepare(self):
        # This function is called once overall, at the start, can be used to set up ancestry trees etc.
        pass

    @abstractmethod
    def _prepare_partition(self, partition_i: int):
        # This function is called once per partition, at the start. It is executed within a thread.
        pass

    @abstractmethod
    def _process_person(self, person_id: int, cdm_tables: Dict[str, pd.DataFrame]):
        # This functon is called for every person (It is executed within a thread.)
        pass

    @abstractmethod
    def _finish_partition(self, partition_i: int):
        # This function is called once per partition, at the end. It is executed within a thread.
        pass

    def process_cdm_data(self):
        """
        Process the CDM data in the provided cdm_data_path.
        """
        self._get_partition_counts()
        self._prepare()
        if self._max_cores == -1:
            # For profiling, run small set of partitions in main thread:
            self._person_partition_count = 1
            for partition_i in range(self._person_partition_count):
                self._process_partition(partition_i)
        else:
            with Pool(processes=self._max_cores) as pool:
                pool.map(self._process_partition, range(self._person_partition_count))
                pool.close()

    def _get_partition_counts(self):
        files = os.listdir(os.path.join(self._cdm_data_path, PERSON))
        self._person_partition_count = len(
            list(filter(lambda x: ".parquet" in x, files))
        )
        logging.info("Found %s partitions", self._person_partition_count)

    def _process_partition(self, partition_i: int):
        # This function is executed within a thread
        # Need to re-configure logger because we're in a thread:
        self._configure_logger()
        logging.debug("Starting partition %s of %s", partition_i, self._person_partition_count)
        self._prepare_partition(partition_i)
        table_iterators = dict()
        for table_name in CDM_TABLES:
            table_iterators[table_name] = self._create_table_iterator(
                table_name=table_name, partition_i=partition_i
            )
        table_person_datas = {}
        for table_name, table_iterator in table_iterators.items():
            table_person_datas[table_name] = next(table_iterator, None)
        for person in self._create_table_iterator(
                table_name=PERSON, partition_i=partition_i
        ):
            person_id = person[PERSON_ID].iat[0]
            cdm_tables = {PERSON: person}
            for table_name, table_person_data in table_person_datas.items():
                while (
                        table_person_data is not None
                        and table_person_data[PERSON_ID].iat[0] < person_id
                ):
                    table_person_data = next(table_iterators[table_name], None)
                if (
                        table_person_data is not None
                        and table_person_data[PERSON_ID].iat[0] == person_id
                ):
                    cdm_tables[table_name] = table_person_data
                else:
                    table_person_datas[table_name] = table_person_data
            self._process_person(person_id=person_id, cdm_tables=cdm_tables)
        self._finish_partition(partition_i)
        logging.info("Finished partition %s of %s", partition_i, self._person_partition_count)

    def _create_table_iterator(self, table_name: str, partition_i: int):
        """
        At each call returns a DataFrame containing all data from the table for 1 person.
        Assumes the tables are sorted by person ID
        """
        file_name = "part{:04d}.parquet".format(partition_i + 1)
        table = pq.read_table(
            source=os.path.join(self._cdm_data_path, table_name, file_name),
            use_threads=False,
        )
        # A person's data could be spread over multiple batches, so construct a buffer
        # that grows until we reach next person:
        buffer = None
        buffer_person_id = None
        for batch in table.to_batches():
            batch = batch.to_pandas()
            # batch.columns = batch.columns.str.lower()
            for person_id, group in batch.groupby(PERSON_ID, as_index=False):
                if buffer_person_id is None:
                    buffer = group
                    buffer_person_id = person_id
                else:
                    if buffer_person_id == person_id:
                        buffer = buffer.append(group)
                    else:
                        yield buffer
                        buffer = group
                        buffer_person_id = person_id

        if buffer_person_id is not None:
            yield buffer

    def __str__(self):
        return str(self.__class__.__name__)


class AbstractToParquetCdmDataProcessor(AbstractCdmDataProcessor):
    """
    Extends the AbstractCdmDataProcessor by providing a private _output DataFrame list that
    can be appended to after processing each patient. After a partition is finished, the
    content of _output is written to a Parquet file.

    Args:
        cdm_data_path: The path where the CDM Parquet files are saved (using the GeneralPretrainModelTools packages).
        max_cores: The maximum number of CPU cores to use. If set to -1, all multihreading code will be bypassed for
                   easier debugging.
    """

    def __init__(self, cdm_data_path: str, output_path: str, max_cores: int = -1):
        super(AbstractToParquetCdmDataProcessor, self).__init__(
            cdm_data_path=cdm_data_path, output_path=output_path, max_cores=max_cores
        )

    def _prepare_partition(self, partition_i: int):
        self._output: List[pa.DataFrame] = []

    def _finish_partition(self, partition_i: int):
        if len(self._output) > 0:
            file_name = "part{:04d}.parquet".format(partition_i + 1)
            logging.debug("Writing data for partition %s to '%s'", partition_i, file_name)
            pq.write_table(
                table=pa.Table.from_pandas(df=pd.concat(self._output), nthreads=1),
                where=os.path.join(self._output_path, file_name),
            )

    def _prepare(self):
        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)
