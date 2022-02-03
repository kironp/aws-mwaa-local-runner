from orderlines_common import (
    TC_ENV,
    DEFAULT_SPARK_JOB_VERSION,
    ORDERLINES_MERGE_DBS,
    Project,
    Source,
    Stage,
    STANDARD_FLEET_CORE_CONF
)
from pipeline_metadata import PipelineMetaData
from sensor_metadata import SensorMetaData

SPARK_JOB_NAME = 'etl-orderlines-generic-pivot'
SPARK_JOB_VERSION = DEFAULT_SPARK_JOB_VERSION.copy()
SPARK_JOB_VERSION['data'] = '3.2.1'

output_db = ORDERLINES_MERGE_DBS[TC_ENV.lower()]
PIPELINES = ['pivot_orderlines_sgp_currency_conversion_rates']
TABLES = ['currency_conversion_rates']

currency_meta = PipelineMetaData(
    project=Project.ORDERLINES,
    stage=Stage.REFERENCE,
    source=Source.FX,
    spark_job_name=SPARK_JOB_NAME,
    spark_job_version=SPARK_JOB_VERSION[TC_ENV.lower()],
    pipelines=PIPELINES,
    output_tables=TABLES,
    output_database=output_db,
    emr_step_concurrency=1,
    emr_release_label='emr-6.3.0',
    emr_core_instance_config=STANDARD_FLEET_CORE_CONF,
    emr_core_instance_count=2,
    pipeline_additional_args=[
        '--execution-date', '{{ ds }}'
    ],
    tc_env=TC_ENV.lower()
)


currency_sensor_meta = SensorMetaData(
    project=Project.ORDERLINES,
    stage=Stage.REFERENCE,
    source=Source.FX
)
