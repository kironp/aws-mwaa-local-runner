import logging
from orderlines_common import (
    REFUND_PIVOT_DBS,
    REFUND_MERGE_DBS,
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    STANDARD_FLEET_CORE_CONF,
    Stage,
    Source,
    Project
)
from pipeline_metadata import PipelineMetaData
from sensor_metadata import SensorMetaData

LOG = logging.getLogger(__name__)

PIVOT_SPARK_JOB_NAME = 'etl-orderlines-generic-pivot'
PIVOT_SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
PIVOT_SPARK_JOB_VERSION['data'] = '3.2.1'

MERGE_SPARK_JOB_NAME = 'etl-jobs'
MERGE_SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
MERGE_SPARK_JOB_VERSION['data'] = '2.2.8'


sgp_pivot_pipelines = [
    'pivot_refunds_sgp_discretionary_refunds',
    'pivot_refunds_sgp_void_context',
    'pivot_refunds_sgp_refundables',
    'pivot_refunds_sgp_compensation_reason_codes',
    'pivot_refunds_sgp_compensation_action_started_reimbursement_products',
    'pivot_refunds_sgp_compensation_action_started_reimbursement_payments',
    'pivot_refunds_sgp_compensation_action_started_reimbursement_delivery_fees',
    'pivot_refunds_sgp_compensation_action_complete',
    'pivot_refunds_sgp_compensation_action_complete_products',
    'pivot_refunds_sgp_compensation_action_complete_payments'
]
sgp_int_pipelines = [
    'int_refunds_sgp_std_refunds',
    'int_refunds_sgp_exchange_refunds',
    'int_refunds_sgp_compensations',
    'int_refunds_sgp_compensation_actions'
]
SGP_PIVINT_PIPELINES = sgp_pivot_pipelines + sgp_int_pipelines
SGP_PIVINT_TABLES = [
    'pivoted_sgp_compensation_reason_codes',
    'pivoted_sgp_discretionary_refunds',
    'pivoted_sgp_voidcontext',
    'pivoted_sgp_refundables',
    'pivoted_sgp_compensation_action_started_reimbursement_products',
    'pivoted_sgp_compensation_action_started_reimbursement_payments',
    'pivoted_sgp_compensation_action_started_reimbursement_delivery_fees',
    'pivoted_sgp_compensation_action_complete',
    'pivoted_sgp_compensation_action_complete_products',
    'pivoted_sgp_compensation_action_complete_payments',
    'int_sgp_exchange_refunds',
    'int_sgp_std_refunds',
    'int_sgp_compensations',
    'int_sgp_compensation_actions'
]

sgp_pivint_refunds_meta = PipelineMetaData(
    project=Project.REFUNDS,
    stage=Stage.PIVOT,
    source=Source.SGP,
    spark_job_name=PIVOT_SPARK_JOB_NAME,
    spark_job_version=PIVOT_SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=SGP_PIVINT_PIPELINES,
    output_tables=SGP_PIVINT_TABLES,
    output_database=REFUND_PIVOT_DBS[TC_ENV.lower()],
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    pipeline_additional_args=[
        '--execution-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

SEASONS_INT_PIPELINES = ['int_refunds_seasons_std_refunds']
SEASONS_INT_TABLES = ['int_seasons_std_refunds']

seasons_int_refunds_meta = PipelineMetaData(
    project=Project.REFUNDS,
    stage=Stage.INT,
    source=Source.SEASONS,
    spark_job_name=PIVOT_SPARK_JOB_NAME,
    spark_job_version=PIVOT_SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=SEASONS_INT_PIPELINES,
    output_tables=SEASONS_INT_TABLES,
    output_database=REFUND_PIVOT_DBS[TC_ENV.lower()],
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    pipeline_additional_args=[
        '--execution-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

FRT_INT_PIPELINES = ['int_refunds_sgp_frt_refunds']
FRT_INT_TABLES = ['int_sgp_frt_refunds']

frt_int_refunds_meta = PipelineMetaData(
    project=Project.REFUNDS,
    stage=Stage.INT,
    source=Source.FRT,
    spark_job_name=PIVOT_SPARK_JOB_NAME,
    spark_job_version=PIVOT_SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=FRT_INT_PIPELINES,
    output_tables=FRT_INT_TABLES,
    output_database=REFUND_PIVOT_DBS[TC_ENV.lower()],
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    pipeline_additional_args=[
        '--execution-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

_25KV_INT_PIPELINES = [
    'int_refunds_25kv_std_refunds',
    'int_refunds_25kv_compensation_and_reimbursement'
]
_25KV_INT_TABLES = [
    'int_25kv_std_refunds',
    'int_25kv_compensation_and_reimbursement'
]

_25kv_int_refunds_meta = PipelineMetaData(
    project=Project.REFUNDS,
    stage=Stage.INT,
    source=Source._25KV,
    spark_job_name=PIVOT_SPARK_JOB_NAME,
    spark_job_version=PIVOT_SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=_25KV_INT_PIPELINES,
    output_tables=_25KV_INT_TABLES,
    output_database=REFUND_PIVOT_DBS[TC_ENV.lower()],
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    pipeline_additional_args=[
        '--execution-date {{ ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)

TRACS_PIVOT_PIPELINES = [
    'pivot_refunds_tracs_requests',
    'pivot_refunds_tracs_request_bookings_tickets',
    'pivot_refunds_tracs_request_fixed_charges',
    'pivot_refunds_tracs_arrivals',
    'pivot_refunds_tracs_arrival_bookings_tickets',
    'pivot_refunds_tracs_discretionary_sundries',
    'pivot_refunds_tracs_refunded'
]
TRACS_PIVOT_TABLES = [
    'pivoted_tracs_refund_requests',
    'pivoted_tracs_refund_request_booking_tickets',
    'pivoted_tracs_refund_request_fixed_charges',
    'pivoted_tracs_refund_arrivals',
    'pivoted_tracs_refund_arrival_booking_tickets',
    'pivoted_tracs_refund_discretionary_sundries',
    'pivoted_tracs_refund_refunded'
]
tracs_pivot_refunds_meta = PipelineMetaData(
    project=Project.REFUNDS,
    stage=Stage.PIVOT,
    source=Source.TRACS,
    spark_job_name=PIVOT_SPARK_JOB_NAME,
    spark_job_version=PIVOT_SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=TRACS_PIVOT_PIPELINES,
    output_tables=TRACS_PIVOT_TABLES,
    output_database=REFUND_PIVOT_DBS[TC_ENV.lower()],
    emr_step_concurrency=5,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    spark_additional_properties={
        'spark.sql.legacy.timeParserPolicy': 'LEGACY',
        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'LEGACY',
        'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'LEGACY'
    },
    pipeline_additional_args=[
        '--execution-date {{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

tracs_pivot_refunds_sensor_meta = SensorMetaData(
    project=Project.REFUNDS,
    stage=Stage.PIVOT,
    source=Source.TRACS
)
# 'refunds_tracs_pivot': sensor_tables.dl_tracs_refunds_service,

TRACS_INT_PIPELINES = ['int_refunds_tracs_std_refunds']
TRACS_INT_TABLES = ['int_tracs_std_refunds']

tracs_int_refunds_meta = PipelineMetaData(
    project=Project.REFUNDS,
    stage=Stage.INT,
    source=Source.TRACS,
    spark_job_name=PIVOT_SPARK_JOB_NAME,
    spark_job_version=PIVOT_SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=TRACS_INT_PIPELINES,
    output_tables=TRACS_INT_TABLES,
    output_database=REFUND_PIVOT_DBS[TC_ENV.lower()],
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    pipeline_additional_args=[
        '--execution-date {{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

BEBOC_INT_PIPELINES = [
    'int_refunds_beboc_std_refunds',
    'int_refunds_beboc_compensation_and_reimbursement'
]
BEBOC_INT_TABLES = [
    'int_beboc_std_refunds',
    'int_beboc_compensation_and_reimbursement'
]

beboc_int_refunds_meta = PipelineMetaData(
    project=Project.REFUNDS,
    stage=Stage.INT,
    source=Source.BEBOC,
    spark_job_name=PIVOT_SPARK_JOB_NAME,
    spark_job_version=PIVOT_SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=BEBOC_INT_PIPELINES,
    output_tables=BEBOC_INT_TABLES,
    output_database=REFUND_PIVOT_DBS[TC_ENV.lower()],
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    pipeline_additional_args=[
        '--execution-date {{ ds }}',
        '--is-initial-run'
    ],
    tc_env=TC_ENV.lower()
)

TTL_PIVOT_PIPELINES = ['pivot_refunds_ttl_refunds']
TTL_PIVOT_TABLES = ['pivoted_ttl_refunds']

ttl_pivot_refunds_meta = PipelineMetaData(
    project=Project.REFUNDS,
    stage=Stage.PIVOT,
    source=Source.TTL,
    spark_job_name=PIVOT_SPARK_JOB_NAME,
    spark_job_version=PIVOT_SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=TTL_PIVOT_PIPELINES,
    output_tables=TTL_PIVOT_TABLES,
    output_database=REFUND_PIVOT_DBS[TC_ENV.lower()],
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    spark_additional_properties={
        'spark.sql.legacy.timeParserPolicy': 'LEGACY',
        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'LEGACY',
        'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'LEGACY'
    },
    pipeline_additional_args=[
        '--execution-date {{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

ttl_pivot_refunds_sensor_meta = SensorMetaData(
    project=Project.REFUNDS,
    stage=Stage.PIVOT,
    source=Source.TTL
)

TTL_INT_PIPELINES = ['int_refunds_ttl_refunds']
TTL_INT_TABLES = ['int_ttl_refunds']

ttl_int_refunds_meta = PipelineMetaData(
    project=Project.REFUNDS,
    stage=Stage.INT,
    source=Source.TTL,
    spark_job_name=PIVOT_SPARK_JOB_NAME,
    spark_job_version=PIVOT_SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=TTL_INT_PIPELINES,
    output_tables=TTL_INT_TABLES,
    output_database=REFUND_PIVOT_DBS[TC_ENV.lower()],
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    spark_additional_properties={
        'spark.sql.legacy.timeParserPolicy': 'LEGACY',
        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'LEGACY',
        'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'LEGACY'
    },
    pipeline_additional_args=[
        '--execution-date {{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)

REFUND_MERGE_PIPELINES = [
    'oracle_ref_mirror',
    'refunds_merge'
]
REFUND_MERGE_TABLES = ['refund_requests']

refunds_merge_meta = PipelineMetaData(
    project=Project.REFUNDS,
    stage=Stage.MERGE,
    source=Source.ALL,
    spark_job_name=MERGE_SPARK_JOB_NAME,
    spark_job_version=MERGE_SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=REFUND_MERGE_PIPELINES,
    output_tables=REFUND_MERGE_TABLES,
    output_database=REFUND_MERGE_DBS[TC_ENV.lower()],
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=5,
    spark_additional_properties={
        'spark.jars': '/home/hadoop/.ivy2/jars/ojdbc6.jar'
    },
    pipeline_additional_args=[
        '--execution-date {{ ds }}',
        f'--oracle-username-ssm',
        f'/bi/{TC_ENV.lower()}/oracle_emr_username',
        f'--oracle-password-ssm ',
        f'/bi/{TC_ENV.lower()}/oracle_emr_password'
    ],
    tc_env=TC_ENV.lower()
)
