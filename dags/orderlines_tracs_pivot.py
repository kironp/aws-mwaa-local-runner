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
    'pivot_refunds_tracs_interim_refunds',
    'pivot_refunds_tracs_cg_ref_codes',
    'pivot_refunds_tracs_season_refunds',
    'pivot_orderlines_tracs_transaction_booking_actions',
    'pivot_orderlines_tracs_supplement_types',
    'pivot_orderlines_tracs_ticket_types',
    'pivot_orderlines_tracs_corporates',
    'pivot_orderlines_tracs_stations',
    'pivot_orderlines_tracs_season_bookings',
    'pivot_orderlines_tracs_customer_managed_groups',
    'pivot_orderlines_tracs_supplements',
    'pivot_orderlines_tracs_delivery_statuses',
    'pivot_orderlines_tracs_deliveries',
    'pivot_orderlines_tracs_contact_details',
    'pivot_orderlines_tracs_journey_legs',
    'pivot_orderlines_tracs_linked_bookings',
    'pivot_orderlines_tracs_payments',
    'pivot_orderlines_tracs_bookings',
    'pivot_orderlines_tracs_transactions',
    'pivot_orderlines_tracs_service_providers',
    'pivot_orderlines_tracs_reservations',
    'pivot_orderlines_tracs_reservation_attributes',
    'pivot_orderlines_tracs_adjustments',
    'pivot_orderlines_tracs_agents',
    'pivot_orderlines_tracs_agent_profiles',
    'pivot_orderlines_tracs_booking_details',
]
TABLES = [
    'pivoted_tracs_interim_refunds',
    'pivoted_tracs_cg_ref_codes',
    'pivoted_tracs_season_refunds',
    'pivoted_tracs_transaction_booking_actions',
    'pivoted_tracs_supplement_types',
    'pivoted_tracs_ticket_types',
    'pivoted_tracs_corporates',
    'pivoted_tracs_stations',
    'pivoted_tracs_season_bookings',
    'pivoted_tracs_customer_managed_groups',
    'pivoted_tracs_supplements',
    'pivoted_tracs_delivery_statuses',
    'pivoted_tracs_deliveries',
    'pivoted_tracs_contact_details',
    'pivoted_tracs_journey_legs',
    'pivoted_tracs_linked_bookings',
    'pivoted_tracs_payments',
    'pivoted_tracs_bookings',
    'pivoted_tracs_transactions',
    'pivoted_tracs_service_providers',
    'pivoted_tracs_reservations',
    'pivoted_tracs_reservation_attributes',
    'pivoted_tracs_adjustments',
    'pivoted_tracs_agents',
    'pivoted_tracs_agent_profiles',
    'pivoted_tracs_booking_details'
]

tracs_pivot_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.PIVOT,
    source=Source.TRACS,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=5,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=20,
    spark_additional_properties={
        'spark.sql.legacy.timeParserPolicy': 'LEGACY',
        'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'LEGACY'
    },
    pipeline_additional_args=[
        '--execution-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

tracs_pivot_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.PIVOT,
    source=Source.TRACS,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=5,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_BACKFILL_FLEET_CORE_CONF,
    emr_core_instance_count=4,
    spark_instance_num_cores=96,
    spark_instance_memory_mb=786432,
    spark_additional_properties={
        'spark.sql.legacy.timeParserPolicy': 'LEGACY',
        'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'LEGACY'
    },
    pipeline_additional_args=[
        '--execution-date', '{{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)
