TRUNCATE TABLE @work_database_schema.@person_id_partition_table;
TRUNCATE TABLE @work_database_schema.@concept_id_partition_table;

DROP TABLE @work_database_schema.@person_id_partition_table;
DROP TABLE @work_database_schema.@concept_id_partition_table;