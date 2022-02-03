import logging
from functools import partial
from airflow.operators.python import PythonOperator
from orderlines_common import (
    ORDERLINES_MERGE_DBS,
    ORDERLINES_PIVOT_DBS,
    repair_table_callable_filtered_by_source,
    ORDERLINES_TABLE,
    repair_table_callable,
    ORDERLINES_CURRENT_TABLE,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    STANDARD_FLEET_CORE_CONF,
    STANDARD_BACKFILL_FLEET_CORE_CONF,
    Source,
    Stage,
    Project)
from pipeline_metadata import PipelineMetaData

LOG = logging.getLogger(__name__)

SPARK_JOB_NAME = 'etl-orderlines-25kv-order_line_fare_legs'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '2.1.6'

int_output_db = ORDERLINES_PIVOT_DBS[TC_ENV.lower()]
merge_output_db = ORDERLINES_MERGE_DBS[TC_ENV.lower()]

INT_PIPELINES = [
    'int_orderlines_25kv_order_invoice_lines',
    'int_orderlines_25kv_pnr_product_fare_legs'
]
MERGE_PIPELINES = [
    'merge_orderlines_25kv_order_line_fare_legs'
]
INT_TABLES = [
    'int_25kv_pnr_product_fare_legs',
    'int_25kv_order_invoice_lines'
]

repair_table_part_int = [partial(PythonOperator,
                                 task_id='repair_table.{}.{}'.format(Source._25KV, table),
                                 python_callable=repair_table_callable,
                                 op_kwargs={'aws_region': 'eu-west-1',
                                            'db_name': int_output_db,
                                            'table_name': table})
                         for table in INT_TABLES]


full_repair_table_part_merge = [partial(PythonOperator,
                                        task_id='full_repair_table.{}.{}'.format(Source._25KV, p),
                                        python_callable=repair_table_callable_filtered_by_source,
                                        op_kwargs={'aws_region': 'eu-west-1',
                                                   'db_name': merge_output_db,
                                                   'source_system': Source._25KV.upper(),
                                                   'index_field': 'source_system',
                                                   'table_name': ORDERLINES_TABLE})
                                for p in MERGE_PIPELINES]

source_repair_table_part_merge = partial(PythonOperator,
                                         task_id='repair_table.{}.{}'.format(Source._25KV, ORDERLINES_TABLE),
                                         python_callable=repair_table_callable_filtered_by_source,
                                         op_kwargs={'aws_region': 'eu-west-1',
                                                    'db_name': merge_output_db,
                                                    'source_system': Source._25KV.upper(),
                                                    'drop_invalid_partitions': True,
                                                    'table_name': ORDERLINES_CURRENT_TABLE})


_25kv_int_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.INT,
    source=Source._25KV,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=INT_PIPELINES,
    output_tables=[''],
    output_database=int_output_db,
    emr_step_concurrency=2,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=20,
    pipeline_additional_args=[
        '--execution-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

_25kv_merge_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.MERGE,
    source=Source._25KV,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=MERGE_PIPELINES,
    output_tables=[''],
    output_database=merge_output_db,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=20,
    pipeline_additional_args=[
        '--execution-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

_25kv_int_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.INT,
    source=Source._25KV,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=INT_PIPELINES,
    output_tables=[''],
    output_database=int_output_db,
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

_25kv_merge_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.MERGE,
    source=Source._25KV,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=MERGE_PIPELINES,
    output_tables=[''],
    output_database=merge_output_db,
    emr_step_concurrency=1,
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
