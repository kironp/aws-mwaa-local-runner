from orderlines_common import (
    ORDERLINES_PIVOT_DBS,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    STANDARD_FLEET_CORE_CONF,
    STANDARD_BACKFILL_FLEET_CORE_CONF,
    Source,
    Stage,
    Project
)
from pipeline_metadata import PipelineMetaData
from sensor_metadata import SensorMetaData

SPARK_JOB_NAME = 'etl-orderlines-generic-pivot'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '3.2.1'
OUTPUT_DB = ORDERLINES_PIVOT_DBS[TC_ENV.lower()]

PIPELINES = [
    'pivot_orderlines_ttl_lookups',
    'pivot_orderlines_ttl_manual_bookings',
    'pivot_orderlines_ttl_assisted_travel',
    'pivot_orderlines_ttl_assisted_travel_codes'
]
TABLES = [
    'pivoted_ttl_lookups',
    'pivoted_ttl_manual_bookings',
    'pivoted_ttl_assisted_travel',
    'pivoted_ttl_assisted_travel_codes'
]

ttl_pivot_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.PIVOT,
    source=Source.TTL,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=OUTPUT_DB,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=3,
    spark_additional_properties={
        'spark.sql.legacy.timeParserPolicy': 'LEGACY',
        'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'LEGACY'
    },
    pipeline_additional_args=[
        '--execution-date {{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

ttl_pivot_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.PIVOT,
    source=Source.TTL,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=OUTPUT_DB,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_BACKFILL_FLEET_CORE_CONF,
    emr_core_instance_count=4,
    spark_instance_num_cores=96,
    spark_instance_memory_mb=786432,
    spark_additional_properties={
        'spark.sql.legacy.timeParserPolicy': 'LEGACY',
        'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'LEGACY'
    },
    pipeline_additional_args=[
        '--execution-date {{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)

ttl_pivot_sensor_meta = SensorMetaData(
    project=Project.ORDERLINES,
    stage=Stage.PIVOT,
    source=Source.TTL
)
