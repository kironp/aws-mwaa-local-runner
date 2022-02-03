from orderlines_common import (
    ORDERLINES_HISTORY_DBS,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    repair_table_callable,
    STANDARD_FLEET_CORE_CONF,
    Source,
    Stage,
    Project,
    STANDARD_BACKFILL_FLEET_CORE_CONF
)
from pipeline_metadata import PipelineMetaData
from sensor_metadata import SensorMetaData

SPARK_JOB_NAME = 'etl-orderlines-generic-history'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '3.1.7'
output_db = ORDERLINES_HISTORY_DBS[TC_ENV.lower()]

PIPELINES = [
    'orderlines_history_flixbus_actions'
]
TABLES = [
    'hist_flixbus_actions'
]

flixbus_history_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.HISTORY,
    source=Source.FLIXBUS,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=1,
    pipeline_additional_args=[
        '--min-execution-range-date', '{{ ds }}',
        '--max-execution-range-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)


flixbus_history_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.HISTORY,
    source=Source.FLIXBUS,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_BACKFILL_FLEET_CORE_CONF,
    emr_core_instance_count=1,
    spark_instance_num_cores=96,
    spark_instance_memory_mb=786432,
    pipeline_additional_args=[
        '--min-execution-range-date', '1900-01-01',
        '--max-execution-range-date', '{{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)

flixbus_history_sensor_meta = SensorMetaData(
    project=Project.ORDERLINES,
    stage=Stage.HISTORY,
    source=Source.FLIXBUS
)
