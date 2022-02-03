import logging
from functools import partial
from airflow.operators.python import PythonOperator
from orderlines_common import  (
    ORDERLINES_MERGE_DBS,
    repair_table_callable_filtered_by_source,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    ORDERLINES_TABLE,
    ORDERLINES_CURRENT_TABLE,
    Stage,
    Source,
    STANDARD_FLEET_CORE_CONF,
    STANDARD_BACKFILL_FLEET_CORE_CONF,
    Project
)
from pipeline_metadata import PipelineMetaData

LOG = logging.getLogger(__name__)

SPARK_JOB_NAME = 'etl-orderlines-ttl-manual-order_line_fare_legs'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '2.0.0'
OUTPUT_DB = ORDERLINES_MERGE_DBS[TC_ENV.lower()]

full_repair_table_part = partial(PythonOperator,
                                 task_id='full_repair_table.{}.{}'.format(Source.TTL, ORDERLINES_TABLE),
                                 python_callable=repair_table_callable_filtered_by_source,
                                 op_kwargs={'aws_region': 'eu-west-1',
                                            'db_name': OUTPUT_DB,
                                            'source_system': 'TTL',
                                            'index_field': 'source_system',
                                            'table_name': ORDERLINES_TABLE})

source_repair_table_part = partial(PythonOperator,
                                   task_id='repair_table.{}.{}'.format(Source.TTL, ORDERLINES_CURRENT_TABLE),
                                   python_callable=repair_table_callable_filtered_by_source,
                                   op_kwargs={'aws_region': 'eu-west-1',
                                              'db_name': OUTPUT_DB,
                                              'source_system': 'TTL',
                                              'drop_invalid_partitions': True,
                                              'table_name': ORDERLINES_CURRENT_TABLE})

ttl_merge_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.MERGE,
    source=Source.TTL,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=['merge_orderlines_ttl_manual_order_line_fare_legs'],
    output_tables=[''],
    output_database=OUTPUT_DB,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=3,
    pipeline_additional_args=[
        '--execution-date {{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

ttl_merge_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.MERGE,
    source=Source.TTL,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=['merge_orderlines_ttl_manual_order_line_fare_legs'],
    output_tables=[''],
    output_database=OUTPUT_DB,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_BACKFILL_FLEET_CORE_CONF,
    emr_core_instance_count=4,
    spark_instance_num_cores=96,
    spark_instance_memory_mb=786432,
    pipeline_additional_args=[
        '--execution-date {{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)
