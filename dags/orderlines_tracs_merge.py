import logging
from functools import partial
from airflow.operators.python import PythonOperator
from orderlines_common import (
    ORDERLINES_MERGE_DBS,
    repair_table_callable_filtered_by_source,
    ORDERLINES_TABLE,
    ORDERLINES_FEES_TABLE,
    ORDERLINES_CURRENT_TABLE,
    ORDERLINES_RESERVATIONS_TABLE,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    STANDARD_FLEET_CORE_CONF,
    STANDARD_BACKFILL_FLEET_CORE_CONF,
    Source,
    Stage,
    Project
)
from pipeline_metadata import PipelineMetaData

LOG = logging.getLogger(__name__)

SPARK_JOB_NAME = 'etl-orderlines-tracs-order_line_fare_legs'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '1.1.7'
output_db = ORDERLINES_MERGE_DBS[TC_ENV.lower()]
pipeline_name = ORDERLINES_TABLE

full_repair_table_part = [
    partial(PythonOperator,
            task_id='full_repair_table.{}.{}'.format(Source.TRACS, pipeline_name),
            python_callable=repair_table_callable_filtered_by_source,
            op_kwargs={'aws_region': 'eu-west-1',
                       'db_name': output_db,
                       'source_system': 'TRACS',
                       'index_field': 'source_system',
                       'table_name': ORDERLINES_TABLE}),
    partial(PythonOperator,
            task_id='repair_table.{}.{}'.format(Source.TRACS, ORDERLINES_FEES_TABLE),
            python_callable=repair_table_callable_filtered_by_source,
            op_kwargs={'aws_region': 'eu-west-1',
                       'db_name': output_db,
                       'source_system': 'TRACS',
                       'drop_invalid_partitions': True,
                       'table_name': ORDERLINES_FEES_TABLE})
,
    partial(PythonOperator,
            task_id='repair_table.{}.{}'.format(Source.TRACS, ORDERLINES_RESERVATIONS_TABLE),
            python_callable=repair_table_callable_filtered_by_source,
            op_kwargs={'aws_region': 'eu-west-1',
                       'db_name': output_db,
                       'source_system': 'TRACS',
                       'drop_invalid_partitions': True,
                       'table_name': ORDERLINES_RESERVATIONS_TABLE})
]


source_repair_table_part = partial(PythonOperator,
                                   task_id='repair_table.{}.{}'.format(Source.TRACS, pipeline_name),
                                   python_callable=repair_table_callable_filtered_by_source,
                                   op_kwargs={'aws_region': 'eu-west-1',
                                              'db_name': output_db,
                                              'source_system': 'TRACS',
                                              'drop_invalid_partitions': True,
                                              'table_name': ORDERLINES_CURRENT_TABLE})

# Previous incarnation was explicitly enabling cross join
# Spark 3 has it enabled by default, hence omitted it for now
tracs_merge_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.MERGE,
    source=Source.TRACS,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=['merge_orderlines_tracs_order_line_fare_legs'],
    output_tables=[''],
    output_database=output_db,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=20,
    pipeline_additional_args=['--execution-date {{ ds }}'],
    tc_env=TC_ENV.lower()
)

tracs_merge_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.MERGE,
    source=Source.TRACS,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=['merge_orderlines_tracs_order_line_fare_legs'],
    output_tables=[''],
    output_database=output_db,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_BACKFILL_FLEET_CORE_CONF,
    emr_core_instance_count=12,
    spark_instance_num_cores=96,
    spark_instance_memory_mb=786432,
    spark_additional_properties={
        "spark.executor.heartbeatInterval": "60s",
        "spark.network.timeout": "600s",
        "spark.driver.memory": "10g",
        "spark.driver.maxResultSize": "4g",
    },
    pipeline_additional_args=['--execution-date {{ yesterday_ds }}',
                              '--is-initial-run'],
    tc_env=TC_ENV.lower()
)
