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
    'pivot_orderlines_sgp_atoc',
    'pivot_orderlines_sgp_eu',
    'pivot_orderlines_sgp_insurance',
    'pivot_orderlines_sgp_national_express_product_leg',
    'pivot_orderlines_sgp_national_express_product_offers_actual',
    'pivot_orderlines_sgp_national_express_product_pnrs_issued',
    'pivot_orderlines_sgp_orderevent',
    'pivot_orderlines_sgp_atoc_change_operation',
    'pivot_orderlines_sgp_eu_change_operation',
    'pivot_orderlines_sgp_rebook_refund_context',
    'pivot_orderlines_sgp_customer_origin_classified',
    'pivot_orderlines_sgp_payment_created',
    'pivot_orderlines_sgp_payment_details_created',
    'pivot_orderlines_sgp_order_invoices_payments',
    'pivot_orderlines_sgp_atoc_railcard_inventory_product',
    'pivot_orderlines_sgp_compensation_actions_product',
    'pivot_orderlines_sgp_atoc_travel_product_fares',
    'pivot_orderlines_sgp_atoc_travel_product_fares_fare_passengers',
    'pivot_orderlines_sgp_atoc_inventory_product_fares',
    'pivot_orderlines_sgp_atoc_inventory_product_legs',
    'pivot_orderlines_sgp_order_invoices_delivery_fees',
    'pivot_orderlines_sgp_order_invoices_booking_fee_breakdown_references',
    'pivot_orderlines_sgp_order_invoices_promos',
    'pivot_orderlines_sgp_eu_reservation_space_allocations',
    'pivot_orderlines_sgp_atoc_reservation_space_allocations',
    'pivot_orderlines_sgp_promotions',
    'pivot_orderlines_sgp_order_invoices_coj_admin_fees',
    'pivot_orderlines_sgp_eu_railcard_inventory_product',
    'pivot_orderlines_sgp_route_restrictions',
    'pivot_orderlines_sgp_order_context'
]
TABLES = [
    'pivoted_sgp_atoc',
    'pivoted_sgp_eu',
    'pivoted_sgp_insurance',
    'pivoted_sgp_national_express_product_leg',
    'pivoted_sgp_national_express_product_offers_actual',
    'pivoted_sgp_national_express_product_pnrs_issued',
    'pivoted_sgp_orderevent',
    'pivoted_sgp_atoc_change_operation',
    'pivoted_sgp_eu_change_operation',
    'pivoted_sgp_rebook_refund_context',
    'pivoted_sgp_customer_origin_classified',
    'pivoted_sgp_payment_created',
    'pivoted_sgp_payment_details_created',
    'pivoted_sgp_order_invoices_payments',
    'pivoted_sgp_atoc_railcard_inventory_product',
    'pivoted_sgp_compensation_actions_product',
    'pivoted_sgp_atoc_travel_product_fares',
    'pivoted_sgp_atoc_travel_product_fares_farepassengers_passengers',
    'pivoted_sgp_atoc_inventory_product_fares',
    'pivoted_sgp_atoc_inventory_product_legs',
    'pivoted_sgp_order_invoices_delivery_fees',
    'pivoted_sgp_order_invoices_booking_fee_breakdown_references',
    'pivoted_sgp_eu_reservation_space_allocations',
    'pivoted_sgp_atoc_reservation_space_allocations',
    'pivoted_sgp_order_invoices_promos',
    'pivoted_sgp_promotions',
    'pivoted_sgp_order_invoices_coj_admin_fees',
    'pivoted_sgp_eu_railcard_inventory_product',
    'pivoted_sgp_uk_route_restriction_reference_data_updated',
    'pivoted_sgp_order_context'
]

sgp_pivot_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.PIVOT,
    source=Source.SGP,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=5,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=25,
    pipeline_additional_args=[
        '--execution-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

sgp_pivot_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.PIVOT,
    source=Source.SGP,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=3,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_BACKFILL_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    spark_instance_num_cores=96,
    spark_instance_memory_mb=786432,
    pipeline_additional_args=[
        '--execution-date', '{{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)
