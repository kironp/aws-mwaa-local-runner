from orderlines_common import (
    ORDERLINES_HISTORY_DBS,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    STANDARD_FLEET_CORE_CONF,
    STANDARD_BACKFILL_FLEET_CORE_CONF,
    Stage,
    Source,
    Project
)
from pipeline_metadata import PipelineMetaData
from sensor_metadata import SensorMetaData

SPARK_JOB_NAME = 'etl-orderlines-generic-history'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '3.1.7'
output_db = ORDERLINES_HISTORY_DBS[TC_ENV.lower()]

PIPELINES = [
    'orderlines_history_tracs_transactions',
    'orderlines_history_tracs_bookings',
    'orderlines_history_tracs_payments',
    'orderlines_history_tracs_contact_details',
    'orderlines_history_tracs_deliveries',
    'orderlines_history_tracs_delivery_statuses',
    'orderlines_history_tracs_season_bookings',
    'orderlines_history_tracs_transaction_booking_actions',
    'orderlines_history_tracs_journey_legs',
    'orderlines_history_tracs_linked_bookings',
    'orderlines_history_tracs_supplements',
    'orderlines_history_tracs_reservations',
    'orderlines_history_tracs_reservation_attributes',
    'orderlines_history_tracs_adjustments',
    'orderlines_history_tracs_booking_details'
]

TABLES = [
    'hist_tracs_transactions',
    'hist_tracs_bookings',
    'hist_tracs_payments',
    'hist_tracs_contact_details',
    'hist_tracs_deliveries',
    'hist_tracs_delivery_statuses',
    'hist_tracs_season_bookings',
    'hist_tracs_transaction_booking_actions',
    'hist_tracs_journey_legs',
    'hist_tracs_linked_bookings',
    'hist_tracs_supplements',
    'hist_tracs_reservations',
    'hist_tracs_reservation_attributes',
    'hist_tracs_adjustments',
    'hist_tracs_booking_details'
]

tracs_history_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.HISTORY,
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
    spark_additional_properties={},
    pipeline_additional_args=[
        '--min-execution-range-date', '{{ ds }}',
        '--max-execution-range-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)


tracs_history_backfill_meta = PipelineMetaData(
    project=Project.BACKFILL_ORDERLINES,
    stage=Stage.HISTORY,
    source=Source.TRACS,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=3,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_BACKFILL_FLEET_CORE_CONF,
    emr_core_instance_count=2,
    spark_instance_num_cores=96,
    spark_instance_memory_mb=786432,
    spark_additional_properties={
        'spark.network.timeout': '600s'
    },
    pipeline_additional_args=[
        '--min-execution-range-date', '1900-01-01',
        '--max-execution-range-date', '{{ yesterday_ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)

tracs_history_sensor_meta = SensorMetaData(
    project=Project.ORDERLINES,
    stage=Stage.HISTORY,
    source=Source.TRACS
)

