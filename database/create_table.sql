--Creating table
CREATE EXTERNAL TABLE sf_fire_incidents_db.fire_incidents (
    incident_number STRING
    , exposure_number STRING
    , id STRING
    , address STRING
    , incident_date STRING
    , call_number STRING
    , alarm_dttm STRING
    , arrival_dttm STRING
    , close_dttm STRING
    , city STRING
    , zipcode STRING
    , station_area STRING
    , suppression_units STRING
    , suppression_personnel STRING
    , ems_units STRING
    , ems_personnel STRING
    , other_units STRING
    , other_personnel STRING
    , first_unit_on_scene STRING
    , fire_fatalities STRING
    , fire_injuries STRING
    , civilian_fatalities STRING
    , civilian_injuries STRING
    , number_of_alarms STRING
    , primary_situation STRING
    , mutual_aid STRING
    , action_taken_primary STRING
    , property_use STRING
    , supervisor_district STRING
    , neighborhood_district STRING
    , point STRING
    , data_as_of STRING
    , data_loaded_at STRING
    , estimated_property_loss STRING
    , estimated_contents_loss STRING
    , area_of_fire_origin STRING
    , ignition_cause STRING
    , ignition_factor_primary STRING
    , heat_source STRING
    , item_first_ignited STRING
    , human_factors_associated_with_ignition STRING
    , structure_type STRING
    , structure_status STRING
    , floor_of_fire_origin STRING
    , no_flame_spread STRING
    , number_of_floors_with_minimum_damage STRING
    , number_of_floors_with_significant_damage STRING
    , number_of_floors_with_heavy_damage STRING
    , number_of_floors_with_extreme_damage STRING
    , detectors_present STRING
    , detector_type STRING
    , detector_operation STRING
    , detector_effectiveness STRING
    , automatic_extinguishing_system_present STRING
    , number_of_sprinkler_heads_operating STRING
    , detector_alerted_occupants STRING
    , action_taken_secondary STRING
    , action_taken_other STRING
    , fire_spread STRING
    , detector_failure_reason STRING
    , box STRING
    , automatic_extinguishing_sytem_type STRING
    , automatic_extinguishing_sytem_perfomance STRING
    , ignition_factor_secondary STRING
)
PARTITIONED BY (battalion STRING)
STORED AS PARQUET
LOCATION 's3://<bucket_name>/processed/';
--Point the location to your bucket