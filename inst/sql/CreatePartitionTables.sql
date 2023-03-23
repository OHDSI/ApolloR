{DEFAULT @partitions = 200}
{DEFAULT @sample_size = 1000000}

DROP TABLE IF EXISTS @work_database_schema.@person_id_partition_table;

SELECT person_id,
	rn % @partitions + 1 AS partition_id
INTO @work_database_schema.@person_id_partition_table
FROM (
	SELECT person_id,
		ROW_NUMBER() OVER (ORDER BY NEWID()) AS rn
	FROM @cdm_database_schema.person
	) tmp
WHERE rn <= @sample_size;

CREATE INDEX idx_@person_id_sample_table_person_id ON @work_database_schema.@person_id_partition_table (person_id);
CREATE INDEX idx_@person_id_sample_table_partition_id ON @work_database_schema.@person_id_partition_table (partition_id);

DROP TABLE IF EXISTS @work_database_schema.@concept_id_partition_table;

SELECT concept_id,
	rn % @partitions + 1 AS partition_id
INTO @work_database_schema.@concept_id_partition_table
FROM (
	SELECT concept_id,
		ROW_NUMBER() OVER (ORDER BY concept_id) AS rn
	FROM @cdm_database_schema.concept
	WHERE standard_concept = 'S'
	) tmp;

CREATE INDEX idx_@concept_id_sample_table_concept_id ON @work_database_schema.@concept_id_partition_table (concept_id);
CREATE INDEX idx_@concept_id_sample_table_partition_id ON @work_database_schema.@concept_id_partition_table (partition_id);

