from orderlines_common import (
    ORDERLINES_HISTORY_DBS,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    JOB_BUCKET,
    Source,
    Stage,
    Project,
    STANDARD_FLEET_CORE_CONF,
    STANDARD_BACKFILL_FLEET_CORE_CONF
)
from pipeline_metadata import PipelineMetaData
from sensor_metadata import SensorMetaData

SPARK_JOB_NAME = 'etl-orderlines-generic-history'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '3.1.7'

output_db = ORDERLINES_HISTORY_DBS[TC_ENV.lower()]

PIPELINES = [
    'orderlines_history_25kv_invoices',
    'orderlines_history_25kv_invoice_lines_by_invoice_id',
    'orderlines_history_25kv_invoice_lines_by_payable_id',
    'orderlines_history_25kv_payments',
    'orderlines_history_25kv_channels',
    'orderlines_history_25kv_orders',
    'orderlines_history_25kv_pnrs_by_pnr_id',
    'orderlines_history_25kv_pnrs_by_header_id',
    'orderlines_history_25kv_folders',
    'orderlines_history_25kv_trips',
    'orderlines_history_25kv_segments',
    'orderlines_history_25kv_tickets',
    'orderlines_history_25kv_discounts',
    'orderlines_history_25kv_sources',
    'orderlines_history_25kv_subscriptions',
    'orderlines_history_25kv_after_sales_intents',
    'orderlines_history_25kv_refunds',
    'orderlines_history_25kv_pnr_headers'
]

TABLES = [
    'hist_25kv_invoices',
    'hist_25kv_invoice_lines_by_invoice_id',
    'hist_25kv_invoice_lines_by_payable_id',
    'hist_25kv_payments',
    'hist_25kv_channels',
    'hist_25kv_orders',
    'hist_25kv_pnrs_by_pnr_id',
    'hist_25kv_pnrs_by_header_id',
    'hist_25kv_folders',
    'hist_25kv_trips',
    'hist_25kv_segments',
    'hist_25kv_tickets',
    'hist_25kv_discounts',
    'hist_25kv_sources',
    'hist_25kv_subscriptions',
    'hist_25kv_after_sales_intents',
    'hist_25kv_refunds',
    'hist_25kv_pnr_headers'
]

_25kv_history_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.HISTORY,
    source=Source._25KV,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=5,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=20,
    spark_additional_properties={},
    pipeline_additional_args=[
        '--min-execution-range-date', '{{ ds }}',
        '--max-execution-range-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)


_25kv_history_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.HISTORY,
    source=Source._25KV,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=5,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_BACKFILL_FLEET_CORE_CONF,
    emr_core_instance_count=1,
    spark_instance_num_cores=96,
    spark_instance_memory_mb=786432,
    spark_additional_properties={},
    pipeline_additional_args=[
        '--min-execution-range-date', '1900-01-01',
        '--max-execution-range-date', '{{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)

_25kv_history_sensor_meta = SensorMetaData(
    project=Project.ORDERLINES,
    stage=Stage.HISTORY,
    source=Source._25KV
)
