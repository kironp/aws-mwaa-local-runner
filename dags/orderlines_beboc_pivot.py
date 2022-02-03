from orderlines_common import (
    ORDERLINES_PIVOT_DBS,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    STANDARD_FLEET_CORE_CONF,
    Source,
    Stage,
    Project,
    STANDARD_BACKFILL_FLEET_CORE_CONF
)
from pipeline_metadata import PipelineMetaData

SPARK_JOB_NAME = 'etl-orderlines-generic-pivot'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '3.2.1'
output_db = ORDERLINES_PIVOT_DBS[TC_ENV.lower()]

PIPELINES = [
    'pivot_orderlines_beboc_channels',
    'pivot_orderlines_beboc_discounts',
    'pivot_orderlines_beboc_subscriptions',
    'pivot_orderlines_beboc_invoice_lines_by_invoice_id',
    'pivot_orderlines_beboc_invoice_lines_by_payable_id',
    'pivot_orderlines_beboc_invoices',
    'pivot_orderlines_beboc_orders',
    'pivot_orderlines_beboc_payments',
    'pivot_orderlines_beboc_pnrs_by_pnr_id',
    'pivot_orderlines_beboc_pnrs_by_header_id',
    'pivot_orderlines_beboc_tickets',
    'pivot_orderlines_beboc_stations',
    'pivot_orderlines_beboc_geo_locations',
    'pivot_orderlines_beboc_folders',
    'pivot_orderlines_beboc_trips',
    'pivot_orderlines_beboc_segments',
    'pivot_orderlines_beboc_after_sales_intents',
    'pivot_orderlines_beboc_refunds',
    'pivot_orderlines_beboc_pnr_headers'
]
TABLES = [
    'pivoted_beboc_channels',
    'pivoted_beboc_discounts',
    'pivoted_beboc_subscriptions',
    'pivoted_beboc_invoice_lines_by_invoice_id',
    'pivoted_beboc_invoice_lines_by_payable_id',
    'pivoted_beboc_invoices',
    'pivoted_beboc_orders',
    'pivoted_beboc_payments',
    'pivoted_beboc_pnrs_by_pnr_id',
    'pivoted_beboc_pnrs_by_header_id',
    'pivoted_beboc_tickets',
    'pivoted_beboc_stations',
    'pivoted_beboc_geo_locations',
    'pivoted_beboc_folders',
    'pivoted_beboc_trips',
    'pivoted_beboc_segments',
    'pivoted_beboc_after_sales_intents',
    'pivoted_beboc_refunds',
    'pivoted_beboc_pnr_headers'
]

beboc_pivot_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.PIVOT,
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
        '--execution-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

beboc_pivot_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.PIVOT,
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
        '--execution-date', '{{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)
