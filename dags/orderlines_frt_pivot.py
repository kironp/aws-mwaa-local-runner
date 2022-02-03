from orderlines_common import (
    ORDERLINES_PIVOT_DBS,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    STANDARD_FLEET_CORE_CONF,
    Stage,
    Source,
    Project,
    STANDARD_BACKFILL_FLEET_CORE_CONF
)
from pipeline_metadata import PipelineMetaData

SPARK_JOB_NAME = 'etl-orderlines-generic-pivot'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '3.2.1'
output_db = ORDERLINES_PIVOT_DBS[TC_ENV.lower()]

PIPELINES = [
    'pivot_orderlines_frt_tracs',
    'pivot_orderlines_frt_direct'
]
TABLES = [
    'pivoted_frt_direct_season_transactions',
    'pivoted_frt_tracs_season_transactions'
]

frt_pivot_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.PIVOT,
    source=Source.FRT,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=2,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=3,
    pipeline_additional_args=[
        '--execution-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)


frt_pivot_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.PIVOT,
    source=Source.FRT,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=2,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_BACKFILL_FLEET_CORE_CONF,
    emr_core_instance_count=1,
    spark_instance_num_cores=96,
    spark_instance_memory_mb=786432,
    pipeline_additional_args=[
        '--execution-date', '{{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)
