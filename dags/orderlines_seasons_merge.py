import logging
from functools import partial
from airflow.operators.python import PythonOperator
from orderlines_common import (
    ORDERLINES_MERGE_DBS,
    repair_table_callable_filtered_by_source,
    ORDERLINES_TABLE,
    ORDERLINES_FEES_TABLE,
    ORDERLINES_CURRENT_TABLE,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    STANDARD_FLEET_CORE_CONF,
    STANDARD_BACKFILL_FLEET_CORE_CONF,
    Source,
    Stage,
    Project
)
from pipeline_metadata import PipelineMetaData

SPARK_JOB_NAME = 'etl-orderlines-seasons-order_line_fare_legs'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '1.1.4'

data_source = 'tracs'
data_output = 'seasons'
output_table = ORDERLINES_TABLE

output_db = ORDERLINES_MERGE_DBS[TC_ENV.lower()]

full_repair_table_part = [
    partial(PythonOperator,
            task_id='full_repair_table.{}.{}'.format(Source.SEASONS, ORDERLINES_TABLE),
            python_callable=repair_table_callable_filtered_by_source,
            op_kwargs={'aws_region': 'eu-west-1',
                       'db_name': output_db,
                       'source_system': 'SEASONS',
                       'index_field': 'source_system',
                       'table_name': ORDERLINES_TABLE}),
    partial(PythonOperator,
            task_id='repair_table.{}.{}'.format(Source.SEASONS, ORDERLINES_FEES_TABLE),
            python_callable=repair_table_callable_filtered_by_source,
            op_kwargs={'aws_region': 'eu-west-1',
                       'db_name': output_db,
                       'source_system': Source.SEASONS.upper(),
                       'drop_invalid_partitions': True,
                       'table_name': ORDERLINES_FEES_TABLE})
]

source_repair_table_part = partial(PythonOperator,
                                   task_id='repair_table.{}.{}'.format(Source.SEASONS, ORDERLINES_TABLE),
                                   python_callable=repair_table_callable_filtered_by_source,
                                   op_kwargs={'aws_region': 'eu-west-1',
                                              'db_name': output_db,
                                              'source_system': Source.SEASONS.upper(),
                                              'drop_invalid_partitions': True,
                                              'table_name': ORDERLINES_CURRENT_TABLE})

seasons_merge_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.MERGE,
    source=Source.SEASONS,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=['merge_orderlines_seasons_order_line_fare_legs'],
    output_tables=[''],
    output_database=output_db,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    pipeline_additional_args=['--execution-date {{ ds }}'],
    tc_env=TC_ENV.lower()
)


seasons_merge_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.MERGE,
    source=Source.SEASONS,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=['merge_orderlines_seasons_order_line_fare_legs'],
    output_tables=[''],
    output_database=output_db,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_BACKFILL_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    pipeline_additional_args=['--execution-date {{ yesterday_ds }}',
                              '--is-initial-run'],
    tc_env=TC_ENV.lower()
)
