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

SPARK_JOB_NAME = 'etl-orderlines-generic-pivot'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '3.2.1'
output_db = ORDERLINES_PIVOT_DBS[TC_ENV.lower()]

PIPELINES = [
    'pivot_orderlines_25kv_channels',
    'pivot_orderlines_25kv_discounts',
    'pivot_orderlines_25kv_invoice_lines_by_invoice_id',
    'pivot_orderlines_25kv_invoice_lines_by_payable_id',
    'pivot_orderlines_25kv_invoices',
    'pivot_orderlines_25kv_orders',
    'pivot_orderlines_25kv_payments',
    'pivot_orderlines_25kv_pnrs_by_pnr_id',
    'pivot_orderlines_25kv_pnrs_by_header_id',
    'pivot_orderlines_25kv_sources',
    'pivot_orderlines_25kv_tickets',
    'pivot_orderlines_25kv_stations',
    'pivot_orderlines_25kv_holdings',
    'pivot_orderlines_25kv_organizations',
    'pivot_orderlines_25kv_stations_pacon_id',
    'pivot_orderlines_25kv_geo_locations',
    'pivot_orderlines_25kv_folders',
    'pivot_orderlines_25kv_trips',
    'pivot_orderlines_25kv_segments',
    'pivot_orderlines_25kv_subscriptions',
    'pivot_orderlines_25kv_after_sales_intents',
    'pivot_orderlines_25kv_refunds',
    'pivot_orderlines_25kv_pnr_headers',
    'pivot_orderlines_25kv_user_geo_locations',
    'pivot_orderlines_25kv_condition_descriptions',
    'pivot_orderlines_25kv_geo_locations_audit',
    'pivot_orderlines_25kv_users_locale_audit'
]

TABLES = [
    'pivoted_25kv_channels',
    'pivoted_25kv_discounts',
    'pivoted_25kv_invoice_lines_by_invoice_id',
    'pivoted_25kv_invoice_lines_by_payable_id',
    'pivoted_25kv_invoices',
    'pivoted_25kv_orders',
    'pivoted_25kv_payments',
    'pivoted_25kv_pnrs_by_pnr_id',
    'pivoted_25kv_pnrs_by_header_id',
    'pivoted_25kv_sources',
    'pivoted_25kv_tickets',
    'pivoted_25kv_stations',
    'pivoted_25kv_holdings',
    'pivoted_25kv_organizations',
    'pivoted_25kv_stations_pacon_id',
    'pivoted_25kv_geo_locations',
    'pivoted_25kv_folders',
    'pivoted_25kv_trips',
    'pivoted_25kv_segments',
    'pivoted_25kv_subscriptions',
    'pivoted_25kv_after_sales_intents',
    'pivoted_25kv_refunds',
    'pivoted_25kv_pnr_headers',
    'pivoted_25kv_user_geo_locations',
    'pivoted_25kv_condition_descriptions',
    'pivoted_25kv_geo_locations_audit',
    'pivoted_25kv_users_locale_audit'
]

_25kv_pivot_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.PIVOT,
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
        '--execution-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

_25kv_pivot_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.PIVOT,
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
        '--execution-date', '{{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)
