from orderlines_common import (
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    Source,
    Stage,
    Project,
    DAG_ID_EXTERNALDATA_SINGLE,
    STANDARD_FLEET_CORE_CONF,
    DEFAULT_ARGS,
    EXTERNAL_DATA_FEEDS_HISTORY_DBS,
    EXTERNAL_DATA_FEEDS_PIVOT_DBS,
    MAX_ACTIVE_DAG_RUNS
)
from pipeline_metadata import PipelineMetaData


HISTORY_SPARK_JOB_NAME = 'etl-orderlines-generic-history'
HISTORY_SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
HISTORY_SPARK_JOB_VERSION['data'] = '2.2.5'
HISTORY_SPARK_JOB_VERSION['dev'] = 'feature/BIDW-2948-d365-billing-history-table'
HISTORY_OUTPUT_DB = EXTERNAL_DATA_FEEDS_HISTORY_DBS[TC_ENV.lower()]

EXTERNAL_DATA_HISTORY_PIPELINES = [
    'externaldata_history_d365fo_general_ledger'
]

EXTERNAL_DATA_HISTORY_TABLES = [
    'hist_d365fo_general_ledger'
]

external_data_feeds_history_meta = PipelineMetaData(
    project=Project.EXTERNAL_DATAFEEDS,
    stage=Stage.HISTORY,
    source=Source.EXTERNAL_DATAFEEDS,
    spark_job_name=HISTORY_SPARK_JOB_NAME,
    spark_job_version=HISTORY_SPARK_JOB_VERSION[TC_ENV.lower()],
    spark_additional_properties={
        "spark.sql.broadcastTimeout": "600"
    },
    pipelines=EXTERNAL_DATA_HISTORY_PIPELINES,
    output_tables=EXTERNAL_DATA_HISTORY_TABLES,
    output_database=HISTORY_OUTPUT_DB,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=2,
    pipeline_additional_args=[
        '--min-execution-range-date', '{{ ds }}',
        '--max-execution-range-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)


external_data_feeds_history_backfill_meta = PipelineMetaData(
    project=Project.EXTERNAL_DATAFEEDS,
    stage=Stage.HISTORY,
    source=Source.EXTERNAL_DATAFEEDS,
    spark_job_name=HISTORY_SPARK_JOB_NAME,
    spark_job_version=HISTORY_SPARK_JOB_VERSION[TC_ENV.lower()],
    spark_additional_properties={
        "spark.sql.broadcastTimeout": "600"
    },
    pipelines=EXTERNAL_DATA_HISTORY_PIPELINES,
    output_tables=EXTERNAL_DATA_HISTORY_TABLES,
    output_database=HISTORY_OUTPUT_DB,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=2,
    pipeline_additional_args=[
        '--min-execution-range-date', '1900-01-01',
        '--max-execution-range-date', '{{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)
