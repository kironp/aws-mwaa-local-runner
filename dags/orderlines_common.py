import logging
import os
from database_handler.db_handler import GlueMetaStore, Database, Table
from partitions_utility.field import Field
from partitions_utility.repair_table import refresh_glue_partitions
from callbacks.slack import task_fail_slack_notification_bi_channel_callback
from callbacks.pagerduty import pager_duty_incident_dag_failure
from datetime import datetime, timedelta
from math import floor
# from emr import (
#     emr_template,
#     instance_fleet_template
# )
# from dag_stage_classes import Stage, Source
from utils.defaults import Defaults

LOG = logging.getLogger(__name__)

# TC_ENV = os.environ.get('TC_ENV')
TC_ENV = "DEV"

FORTY_FIVE_MINUTES_IN_SECONDS = 45 * 60
ONE_HOUR_IN_SECONDS = 60 * 60
TWO_HOUR_IN_SECONDS = 2 * ONE_HOUR_IN_SECONDS
THREE_HOUR_IN_SECONDS = 3 * ONE_HOUR_IN_SECONDS
FOUR_HOUR_IN_SECONDS = 4 * ONE_HOUR_IN_SECONDS
SIX_HOURS_IN_SECONDS = 6 * ONE_HOUR_IN_SECONDS
SEVEN_HOURS_IN_SECONDS = 7 * ONE_HOUR_IN_SECONDS
EIGHT_HOURS_IN_SECONDS = 8 * ONE_HOUR_IN_SECONDS
NINE_HOURS_IN_SECONDS = 9 * ONE_HOUR_IN_SECONDS
TWELVE_HOURS_IN_SECONDS = 12 * ONE_HOUR_IN_SECONDS
FIFTEEN_HOURS_IN_SECONDS = 15 * ONE_HOUR_IN_SECONDS
EIGHTEEN_HOURS_IN_SECONDS = 18 * ONE_HOUR_IN_SECONDS

COUNTER_DEFAULTS = Defaults.COUNTER_DEFAULTS

JOB_BUCKET = {'dev': 'tld-dev-priv-data-lab',
              'staging': 'tld-staging-priv-data-lab',
              'data': 'tld-prod-priv-data-lab'}

DEFAULT_SPARK_JOB_VERSION = {
    'dev': 'master',
    'staging': 'dev',
    'data': None
}
DAG_ID_25KV_HISTORY = 'orderlines_25kv_history'
DAG_ID_25KV_PIVOT = 'orderlines_25kv_pivot'
DAG_ID_25KV_MERGE = 'orderlines_25kv_merge'

DAG_ID_BEBOC_HISTORY = 'orderlines_beboc_history'
DAG_ID_BEBOC_PIVOT = 'orderlines_beboc_pivot'
DAG_ID_BEBOC_MERGE = 'orderlines_beboc_merge'

DAG_ID_FLIXBUS_HISTORY = 'orderlines_flixbus_history'
DAG_ID_FLIXBUS_PIVOT = 'orderlines_flixbus_pivot'
DAG_ID_FLIXBUS_MERGE = 'orderlines_flixbus_merge'

DAG_ID_SINGLE = 'orderlines_single_dag'
DAG_ID_EXTERNALDATA_SINGLE = 'externaldata_feeds'

POKE_INTERVAL_PROD = 60 * 10
POKE_INTERVAL_DEFAULT = 60 * 15
SENSOR_DEFAULTS = {'poke_interval': POKE_INTERVAL_PROD if TC_ENV.lower() == 'data'
else POKE_INTERVAL_DEFAULT,
                   'mode': 'reschedule', 'timeout': TWELVE_HOURS_IN_SECONDS,
                   'aws_conn_id': None, 'region_name': 'eu-west-1'}

BACKFILL_MIN_DATE = (datetime.today() - timedelta(days=31)) \
    .strftime('%Y-%m-%d') if TC_ENV.lower() == 'dev' else '1900-01-01'
# List dag names here where task failures should not raise a PagerDuty alert
# Use test_task_fail_callbacks_no_pd and test_task_fail_callbacks_pd to validate the list
# Counter dags added to list but Tableau dag will still alert if there are counter failures
# that affect delivery of gross sales report.
DAGS_NO_PD_ALERT = ['orderlines_reconciliation',
                    'orderlines_history_counters',
                    'orderlines_pivot_counters',
                    'orderlines_int_counters',
                    'orderlines_merge_counters']

SGP_BACKFILL_END_DATES = {'dev':
                              {'history': '2021-05-27',
                               'pivot': '2021-05-26',
                               'merge': '2021-05-26'},
                          'staging':

                              {'history': '2021-06-14',
                               'pivot': '2021-06-24',
                               'merge': '2021-06-14'},

                          'data':
                              {'history': '2021-01-20',
                               'pivot': '2021-05-18',
                               'merge': '2021-06-01'},
                          'test': {'merge': '1900-01-01'}
                          }
# NOTE: For TRACS, backfill date has to be today's date, not yesterday. This is due to the RTP migration and since TRACS
#  -is not really RTP, the processed date is always "today".

TRACS_BACKFILL_END_DATES = {'dev':
                                {'history': '2021-02-22',
                                 'pivot': '2021-02-22',
                                 'merge': '2021-01-05'},
                            'staging':
                                {'history': '2021-07-19',
                                 'pivot': '2021-07-19',
                                 'merge': '2021-07-26'},

                            'data':
                                {'history': '2021-01-20',
                                 'pivot': '2021-01-20',
                                 'merge': '2021-06-01'},
                            'test': {'merge': '1900-01-01'}
                            }
TTL_BACKFILL_END_DATES = TRACS_BACKFILL_END_DATES

FRT_BACKFILL_END_DATES = {'dev':
                              {'history': '2021-02-22',
                               'pivot': '2021-02-22'},
                          'staging':

                              {'history': '2021-06-14',
                               'pivot': '2021-06-14'},

                          'data':
                              {'history': '2021-01-20',
                               'pivot': '2021-01-20'},
                          }

SEASONS_BACKFILL_END_DATES = {'dev':
                                  {'merge': '2021-01-05'},
                              'staging':

                                  {'merge': '2021-06-14'},

                              'data':
                                  {'merge': '2021-06-01'},
                              'test': {'merge': '1900-01-01'}
                              }

_25KV_BACKFILL_END_DATES = {'dev':
                                {'history': '2021-02-22',
                                 'pivot': '2021-02-22',
                                 'merge': '2021-01-05'},
                            'staging':

                                {'history': '2021-07-13',
                                 'pivot': '2021-07-13',
                                 'merge': '2021-07-13'},

                            'data':
                                {'history': '2021-01-20',
                                 'pivot': '2021-03-23',
                                 'merge': '2021-04-08'},
                            'test': {'merge': '1900-01-01'}
                            }

BEBOC_BACKFILL_END_DATES = {'dev':
                                {'history': '2021-02-22',
                                 'pivot': '2021-02-22',
                                 'merge': '2021-01-04'},
                            'staging':

                                {'history': '2021-06-14',
                                 'pivot': '2021-06-14',
                                 'merge': '2021-06-14'},
                            'data':
                                {'history': '2021-01-20',
                                 'pivot': '2021-03-23',
                                 'merge': '2021-04-08'},
                            'test': {'merge': '1900-01-01'}
                            }

FLIXBUS_BACKFILL_END_DATES = {'dev':
                                  {'history': '2021-02-22',
                                   'pivot': '2021-02-22',
                                   'merge': '2021-01-05'},
                              'staging':

                                  {'history': '2021-06-14',
                                   'pivot': '2021-06-14',
                                   'merge': '2021-06-14'},

                              'data':
                                  {'history': '2021-01-20',
                                   'pivot': '2021-01-20',
                                   'merge': '2021-02-07'}
                              }


def task_fail_callbacks(context):
    dag = context.get('dag')
    pager_duty_incident = True
    for dag_name_no_pd in DAGS_NO_PD_ALERT:
        if dag_name_no_pd in dag.dag_id:
            pager_duty_incident = False

    if TC_ENV.lower() == 'data':
        if pager_duty_incident:
            pager_duty_incident_dag_failure(context)
        task_fail_slack_notification_bi_channel_callback(context)


DEFAULT_ARGS = {
    'owner': '   bi',
    'depends_on_past': False,
    'provide_context': True,
    'on_failure_callback': task_fail_callbacks,
    'retries': 1 if TC_ENV.lower() == 'data' else 0,
}

MAX_ACTIVE_DAG_RUNS = {'dev': 1,
                       'staging': 3,
                       'data': 45}

CATCHUP = {'dev': False,
           'staging': False,
           'data': True}

REFUND_PIVOT_DBS = {'dev': 'dev_bi_pre_dwh',
                    'staging': 'stg_bi_pre_dwh',
                    'data': 'bi_pre_dwh'}

REFUND_MERGE_DBS = {'dev': 'dev_bi_dwh',
                    'staging': 'stg_bi_dwh',
                    'data': 'bi_dwh'}

REF_DATA_DBS = {'dev': 'bi_dwh_ref_data',
                'staging': 'bi_dwh_ref_data',
                'data': 'bi_dwh_ref_data'}

ORACLE_REF_MIRROR_DBS = {'dev': 'dev_bi_oracle_ref_mirror',
                         'staging': 'stg_bi_oracle_ref_mirror',
                         'data': 'bi_oracle_ref_mirror'}


class Source:
    SGP = 'sgp'
    TRACS = 'tracs'
    BEBOC = 'beboc'
    _25KV = '25kv'
    FX = 'fx'
    FRT = 'frt'
    SEASONS = 'seasons'
    FLIXBUS = 'flixbus'
    TTL = 'ttl'
    ALL = 'all'
    CUSTOMER_DETAILS = 'customer_details'
    EXTERNAL_DATAFEEDS = 'external_data_feeds'


class Stage:
    HISTORY = 'history'
    PIVOT = 'pivot'
    INT = 'int'
    MERGE = 'merge'
    ORDERLINES = 'orderlines'
    REFERENCE = 'reference'


class Project:
    ORDERLINES = 'orderlines'
    BACKFILL_ORDERLINES = 'backfill-orderlines'
    REFUNDS = 'refunds'
    TOC2SGP = 'toc2sgp'
    EXTERNAL_DATAFEEDS = 'd365_fo'


DATA_LAKE = 'data_lake_private_prod'
# DATA_SOURCE_25KV = '25kv'
# DATA_SOURCE_BEBOC = 'beboc'
# DATA_SOURCE_FLIXBUS = 'flixbus'

ORDERLINES_INPUT_DB_25KV = 'data_lake_private_prod'
ORDERLINES_INPUT_DB_FLIXBUS = 'data_lake_private_prod'
ORDERLINES_INPUT_DB_TTL = 'data_lake_private_prod'

ORDERLINES_HISTORY_DBS = {'dev': 'dev_bi_internal',
                          'staging': 'stg_bi_internal',
                          'data': 'bi_internal'}

ORDERLINES_PIVOT_DBS = {'dev': 'dev_bi_pre_dwh',
                        'staging': 'stg_bi_pre_dwh',
                        'data': 'bi_pre_dwh'}

ORDERLINES_MERGE_DBS = {'dev': 'dev_bi_dwh',
                        'staging': 'stg_bi_dwh',
                        'data': 'bi_dwh'}

ORACLE_MIRROR_DBS = {'dev': 'dev_bi_oracle_ref_mirror',
                     'staging': 'stg_bi_oracle_ref_mirror',
                     'data': 'bi_oracle_ref_mirror'}

ORDERLINES_S3_LOCATION = {'dev': 'tld-dev-priv-bi-data',
                          'staging': 'tld-stg-priv-bi-data',
                          'data': 'tld-prod-priv-bi-data'}
TOC2SGP_DBS = {'dev': 'dev_toc2sgp',
               'staging': 'stg_toc2sgp',
               'data': 'toc2sgp'}

ORDERLINES_TABLE = 'order_line_fare_legs'
ORDERLINES_CURRENT_TABLE = 'order_line_fare_legs_current'
ORDERLINES_FEES_TABLE = 'order_line_fee_details'
ORDERLINES_RESERVATIONS_TABLE = 'order_line_fare_leg_reservations'

EXTERNAL_DATA_FEEDS_HISTORY_DBS = {'dev': 'dev_bi_internal',
                                   'staging': 'stg_bi_internal',
                                   'data': 'bi_internal'}

EXTERNAL_DATA_FEEDS_PIVOT_DBS = {'dev': 'dev_bi_pre_dwh',
                                 'staging': 'stg_bi_pre_dwh',
                                 'data': 'bi_pre_dwh'}

STANDARD_FLEET_CORE_CONF = [
    {
        "InstanceType": "m4.2xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "0.595"
    },
    {
        "InstanceType": "m5.2xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "0.156"
    },
    {
        "InstanceType": "m5a.2xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "0.384"
    },
    {
        "InstanceType": "m5d.2xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "0.504"
    }
]

STANDARD_BACKFILL_FLEET_CORE_CONF = [
    {
        "InstanceType": "r5d.24xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "2.174"
    },
    {
        "InstanceType": "r5.24xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "2.174"
    }
]


def repair_table_callable_no_indexing(**ctx):
    table = Table(metastore=GlueMetaStore(ctx['aws_region']),
                  db=Database(ctx['db_name']),
                  name=ctx['table_name'])

    refresh_glue_partitions(table=table,
                            index_field=None,
                            drop_invalid_partitions=True,
                            drop_all_stale_partitions=True)


def repair_table_callable(**ctx):
    table = Table(metastore=GlueMetaStore(ctx['aws_region']),
                  db=Database(ctx['db_name']),
                  name=ctx['table_name'])

    refresh_glue_partitions(table=table,
                            index_field=Field('execution_date', Field.STRING),
                            drop_invalid_partitions=True,
                            drop_all_stale_partitions=True)


def repair_table_callable_filtered_by_source(**ctx):
    table = Table(metastore=GlueMetaStore(ctx['aws_region']),
                  db=Database(ctx['db_name']),
                  name=ctx['table_name'])

    index_field = ctx.get('index_field', 'execution_date')
    drop_invalid_partitions = ctx.get('drop_invalid_partitions', False)

    refresh_glue_partitions(
        table=table,
        index_field=Field(index_field, Field.STRING),
        filter_fields=[
            Field('source_system', Field.STRING, ctx['source_system'])
        ],
        drop_invalid_partitions=drop_invalid_partitions,
        drop_all_stale_partitions=False
    )


def next_day(ds):
    exec_datetime = datetime.strptime(ds, '%Y-%m-%d')
    return (exec_datetime + timedelta(days=1)).strftime('%Y-%m-%d')


def ref_generic_spark_instance_config(normal_instance_count=3, emr_step_concurrency=0):
    instance_count = normal_instance_count
    instance_type_core = [
        {
            "InstanceType": "m4.2xlarge",
            "BidPrice": "0.595"
        },
        {
            "InstanceType": "m5.2xlarge",
            "BidPrice": "0.156"
        },
        {
            "InstanceType": "m5a.2xlarge",
            "BidPrice": "0.384"
        },
        {
            "InstanceType": "m5d.2xlarge",
            "BidPrice": "0.504"
        }
    ]

    instance_num_cores = 16  # Set to number of cores for instance type. TODO- this value needs further examination
    instance_memory_mb = 32768  # Set instance memory in Mb

    # Config for the MASTER nodes
    # For a master instance fleet, only one of
    # TargetSpotCapacity/TargetOnDemandCapacity can be specified,
    # and its value must be 1.
    instance_type_master = [{
        "InstanceType": "m4.2xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "0.595"
    }, {
        "InstanceType": "m5.2xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "0.454"
    },
        {
            "InstanceType": "m5a.2xlarge",
            "WeightedCapacity": 1,
            "BidPrice": "0.384"
        }
    ]

    instance_config = {
        "instance_type_master": instance_type_master,
        "instance_type_core": instance_type_core,
        "instance_count": instance_count
    }

    if emr_step_concurrency > 1:
        # Boost number of instances to use for initial run
        # Set instance_ values for CORE instance type
        # emr_step_concurrency = 5
        executor_num_cores = 5  # Considered as a good compromise for HDFS throughput and parallelism
        executors_per_instance = (instance_num_cores - 1) // executor_num_cores
        total_executor_count = instance_count * executors_per_instance
        initial_executor_count = floor(total_executor_count // emr_step_concurrency)  # Executors per parallel step
        executor_memory_mb = floor((instance_memory_mb - 1000) * 0.9 / executors_per_instance)
        shuffle_partitions = 3 * instance_count * (instance_num_cores - 1)

        spark_config = {
            "spark.executor.memory": str(executor_memory_mb) + "M",
            "spark.executor.cores": str(executor_num_cores),
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.initialExecutors": str(initial_executor_count),
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.dynamicAllocation.maxExecutors": str(total_executor_count),
            "spark.driver.memory": "3g",
            "spark.driver.cores": "2",
            "spark.driver.maxResultSize": "3g"
        }
    else:
        executors_per_instance = instance_num_cores // min(8, instance_num_cores)  # Allow 8 cores per executor
        executor_count = instance_count * executors_per_instance + 1  # +1 because driver executor is included in count.
        executor_memory_mb = floor(instance_memory_mb / executors_per_instance * 0.58)
        executor_num_cores = instance_num_cores // executors_per_instance
        shuffle_partitions = 3 * instance_count * instance_num_cores

        spark_config = {
            "spark.executor.memory": str(executor_memory_mb) + "M",
            "spark.executor.instances": str(executor_count),
            "spark.executor.cores": str(executor_num_cores),
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
            "spark.dynamicAllocation.enabled": "false",
            "spark.driver.maxResultSize": "3g"
        }
    return instance_config, spark_config


def get_job_emr_config(ctx):
    emr_step_concurrency = ctx['emr_concurrency']
    emr_instance_count = ctx['emr_instance_count']

    instance_config, spark_config = ref_generic_spark_instance_config(normal_instance_count=emr_instance_count,
                                                                      emr_step_concurrency=emr_step_concurrency)
    spark_update({"spark.jars.packages": "ch.cern.sparkmeasure:spark-measure_2.12:0.17"})
    emr_conf = Defaults.get_emr_config(spark_job_name=ctx['SPARK_JOB_NAME'],
                                       spark_job_version=ctx['SPARK_JOB_VERSION'],
                                       dag_id=ctx['dag'].dag_id,
                                       env=ctx['TC_ENV'],
                                       spark_config=spark_config,
                                       instance_config=instance_config,
                                       version=ctx['emr_config_version'])

    emr_conf.update({"StepConcurrencyLevel": emr_step_concurrency})
    emr_conf['Instances']['KeepJobFlowAliveWhenNoSteps'] = False
    # V3.sh has the additional functionality to push spark and emr metrics to S3 location
    path = {'DEV': 's3://tld-dev-priv-data-lab/bi_team/infra-bi-assets/bootstraps/bi_bootstrap_oracle_v1.sh',
            'STAGING': 's3://tld-staging-priv-data-lab/bi_team/infra-bi-assets/bootstraps/bi_bootstrap_oracle_v1.sh',
            'DATA': 's3://tld-prod-priv-data-lab/bi_team/infra-bi-assets/bootstraps/bi_bootstrap_oracle_v1.sh'}

    emr_conf['BootstrapActions'][0]['ScriptBootstrapAction']['Path'] = path[ctx['TC_ENV']]
    emr_conf['BootstrapActions'][0]['ScriptBootstrapAction']['Args'].append(ctx['TC_ENV'].lower())

    return emr_conf


EMR_SERVICE_ROLE = {
    'dev': 'roleTuringBIEMRServiceDev',
    'staging': 'roleTuringBIEMRServicePreProd',
    'data': 'roleTuringBIEMRService'
}

EMR_JOBFLOW_ROLE = {
    'dev': 'roleBusinessIntelligenceDevEMR',
    'staging': 'roleBusinessIntelligencePreProdEMR',
    'data': 'roleBusinessIntelligenceEMR'
}
