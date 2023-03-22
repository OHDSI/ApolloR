{DEFAULT @partitions = 200}
{DEFAULT @sample_size = 1000000}

DROP TABLE IF EXISTS @work_database_schema.@sample_table;

SELECT person_id,
	rn % @partitions + 1 AS partition_id
INTO @work_database_schema.@sample_table
FROM (
	SELECT person_id,
		ROW_NUMBER() OVER (ORDER BY NEWID()) AS rn
	FROM @cdm_database_schema.person
	) tmp
WHERE rn <= @sample_size;

CREATE INDEX idx_@sample_table_person_id ON @work_database_schema.@sample_table (person_id);
CREATE INDEX idx_@sample_table_partition_id ON @work_database_schema.@sample_table (partition_id);