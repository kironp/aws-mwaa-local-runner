from orderlines_common import (
        ORDERLINES_HISTORY_DBS,
        TC_ENV,
        DEFAULT_SPARK_JOB_VERSION,
        STANDARD_FLEET_CORE_CONF,
        STANDARD_BACKFILL_FLEET_CORE_CONF,
        Source,
        Stage,
        Project
    )
from pipeline_metadata import PipelineMetaData
from sensor_metadata import SensorMetaData

SPARK_JOB_NAME = 'etl-orderlines-generic-history'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '3.1.7'
output_db = ORDERLINES_HISTORY_DBS[TC_ENV.lower()]

PIPELINES = [
    'orderlines_history_sgp_atoc',
    'orderlines_history_sgp_eu',
    'orderlines_history_sgp_insurance_created',
    'orderlines_history_sgp_insurance_issued',
    'orderlines_history_sgp_national_express_product_leg',
    'orderlines_history_sgp_national_express_product_offers_actual',
    'orderlines_history_sgp_national_express_product_pnrs_issued',
    'orderlines_history_sgp_order_invoices_products',
    'orderlines_history_sgp_customer_origin_classified',
    'orderlines_history_sgp_order_context',
    'orderlines_history_sgp_payment_created',
    'orderlines_history_sgp_payment_details_created',
    'orderlines_history_sgp_order_invoices_payments',
    'orderlines_history_sgp_atoc_railcard_inventory_product',
    'orderlines_history_sgp_compensation_actions_product',
    'orderlines_history_sgp_atoc_travel_product_fares',
    'orderlines_history_sgp_atoc_inventory_product_fares',
    'orderlines_history_sgp_atoc_inventory_product_legs',
    'orderlines_history_sgp_order_invoices_delivery_fees',
    'orderlines_history_sgp_order_invoices_promos',
    'orderlines_history_sgp_order_invoices_booking_fee_breakdown_references',
    'orderlines_history_sgp_eu_reservation_space_allocations',
    'orderlines_history_sgp_atoc_reservation_space_allocations',
    'orderlines_history_sgp_order_invoices_coj_admin_fees',
    'orderlines_history_sgp_eu_railcard_inventory_product',
    'orderlines_history_sgp_atoc_travel_product_fares_fare_passengers',
    # Keep the below at the end else the counters break
    'orderlines_history_sgp_atoc_change_operation',
    'orderlines_history_sgp_eu_change_operation',
    'orderlines_history_sgp_rebook_refund_context'
]

TABLES = [
    'hist_sgp_atoc',
    'hist_sgp_eu',
    'hist_sgp_insurance_created',
    'hist_sgp_insurance_issued',
    'hist_sgp_national_express_product_leg',
    'hist_sgp_national_express_product_offers_actual',
    'hist_sgp_national_express_product_pnrs_issued',
    'hist_sgp_order_invoices_products',
    'hist_sgp_customer_origin_classified',
    'hist_sgp_order_context',
    'hist_sgp_payment_created',
    'hist_sgp_payment_details_created',
    'hist_sgp_order_invoices_payments',
    'hist_sgp_atoc_railcard_inventory_product',
    'hist_sgp_compensation_actions_product',
    'hist_sgp_atoc_travel_product_fares',
    'hist_sgp_atoc_inventory_product_fares',
    'hist_sgp_atoc_inventory_product_legs',
    'hist_sgp_order_invoices_delivery_fees',
    'hist_sgp_order_invoices_booking_fee_breakdown_references',
    'hist_sgp_order_invoices_promos',
    'hist_sgp_eu_reservation_space_allocations',
    'hist_sgp_atoc_reservation_space_allocations',
    'hist_sgp_order_invoices_coj_admin_fees',
    'hist_sgp_eu_railcard_inventory_product',
    'hist_sgp_atoc_travel_product_fares_farepassengers_passengers',
    # Keep the below at the end else the counters break
    'hist_sgp_atoc_change_operation',
    'hist_sgp_eu_change_operation',
    'hist_sgp_rebook_refund_context'
]

sgp_history_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.HISTORY,
    source=Source.SGP,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=5,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=15,
    emr_additional_config={
        "Classification": "emrfs-site",
        "Properties": {"fs.s3.maxRetries": "20"}},
    pipeline_additional_args=[
        '--min-execution-range-date', '{{ ds }}',
        '--max-execution-range-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)


sgp_history_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.HISTORY,
    source=Source.SGP,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=3,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_BACKFILL_FLEET_CORE_CONF,
    emr_core_instance_count=8,
    emr_additional_config={
        "Classification": "emrfs-site",
        "Properties": {"fs.s3.maxRetries": "20"}},
    spark_instance_num_cores=96,
    spark_instance_memory_mb=786432,
    pipeline_additional_args=[
        '--min-execution-range-date', '1900-01-01',
        '--max-execution-range-date', '{{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)


sgp_history_sensor_meta = SensorMetaData(
    project=Project.ORDERLINES,
    stage=Stage.HISTORY,
    source=Source.SGP
)
