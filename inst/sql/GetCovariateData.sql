SELECT @fields
FROM @cdm_database_schema.@cdm_table
INNER JOIN #partition_table partition_table
  ON @cdm_table.person_id = partition_table.person_id
{@start_date_field != ""} ? {
    AND @cdm_table.@start_date_field >= observation_period_start_date
    AND @cdm_table.@start_date_field <= observation_period_end_date
}
WHERE partition_id = @partition_id;
    