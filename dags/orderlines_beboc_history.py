from orderlines_common import (
    ORDERLINES_HISTORY_DBS,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
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
    'orderlines_history_beboc_after_sales_intents',
    'orderlines_history_beboc_channels',
    'orderlines_history_beboc_discounts',
    'orderlines_history_beboc_folders',
    'orderlines_history_beboc_invoice_lines_by_invoice_id',
    'orderlines_history_beboc_invoice_lines_by_payable_id',
    'orderlines_history_beboc_invoices',
    'orderlines_history_beboc_orders',
    'orderlines_history_beboc_payments',
    'orderlines_history_beboc_pnrs_by_header_id',
    'orderlines_history_beboc_pnrs_by_pnr_id',
    'orderlines_history_beboc_segments',
    'orderlines_history_beboc_tickets',
    'orderlines_history_beboc_trips',
    'orderlines_history_beboc_pnr_headers',
    'orderlines_history_beboc_refunds',
    'orderlines_history_beboc_subscriptions'
]
TABLES = [
    'hist_beboc_after_sales_intents',
    'hist_beboc_channels',
    'hist_beboc_discounts',
    'hist_beboc_folders',
    'hist_beboc_invoice_lines_by_invoice_id',
    'hist_beboc_invoice_lines_by_payable_id',
    'hist_beboc_invoices',
    'hist_beboc_orders',
    'hist_beboc_payments',
    'hist_beboc_pnrs_by_header_id',
    'hist_beboc_pnrs_by_pnr_id',
    'hist_beboc_segments',
    'hist_beboc_tickets',
    'hist_beboc_trips',
    'hist_beboc_pnr_headers',
    'hist_beboc_refunds',
    'hist_beboc_subscriptions'
]

beboc_history_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.HISTORY,
    source=Source.BEBOC,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=5,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=7,
    spark_additional_properties={},
    pipeline_additional_args=[
        '--min-execution-range-date', '{{ ds }}',
        '--max-execution-range-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

beboc_history_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.HISTORY,
    source=Source.BEBOC,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=5,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=7,
    spark_additional_properties={},
    pipeline_additional_args=[
        '--min-execution-range-date', '1900-01-01',
        '--max-execution-range-date', '{{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)

beboc_history_sensor_meta = SensorMetaData(
    project=Project.ORDERLINES,
    stage=Stage.HISTORY,
    source=Source.BEBOC
)
