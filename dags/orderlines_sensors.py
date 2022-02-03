from airflow.models.dag import DAG
from utils.defaults import Defaults
from airflow.contrib.sensors.aws_glue_catalog_partition_sensor import \
    AwsGlueCatalogPartitionSensor
from airflow.operators.dummy import DummyOperator
import time
from airflow.operators.python import PythonOperator
from orderlines_common import (
    EIGHTEEN_HOURS_IN_SECONDS,
    THREE_HOUR_IN_SECONDS,
    FOUR_HOUR_IN_SECONDS,
    TWO_HOUR_IN_SECONDS,
    ONE_HOUR_IN_SECONDS,
    SIX_HOURS_IN_SECONDS)
from functools import partial
from types import SimpleNamespace
import orderlines_sensor_tables as sensor_tables

TASK_ID = '{}.{}_input_sensor'

RETRIES = 4

SENSOR_CFG = {
    'orderlines_25kv_history':
        {
            'sensor_timeout': SIX_HOURS_IN_SECONDS,
            'skip_sensor_on_timeout': False,
            'whirlwind_processing_delay': TWO_HOUR_IN_SECONDS
        },
    'orderlines_tracs_history':
        {
            'sensor_timeout': SIX_HOURS_IN_SECONDS,
            'skip_sensor_on_timeout': False,
        },
    'orderlines_sgp_history':
        {
            'sensor_timeout': TWO_HOUR_IN_SECONDS,
            'skip_sensor_on_timeout': False,
            'additional_sensors': ['orderlines_sgp_history_join']
        },
    'refunds_tracs_pivot':
        {
            'sensor_timeout': SIX_HOURS_IN_SECONDS,
            'skip_sensor_on_timeout': False,
        },
    'refunds_ttl_pivot':
        {
            'sensor_timeout': FOUR_HOUR_IN_SECONDS,
            'skip_sensor_on_timeout': True,
        },
    'refunds_tracs_pivot':
        {
            'sensor_timeout': SIX_HOURS_IN_SECONDS,
            'skip_sensor_on_timeout': False,
        },
    'refunds_ttl_pivot':
        {
            'sensor_timeout': FOUR_HOUR_IN_SECONDS,
            'skip_sensor_on_timeout': True,
        },
    # Additional sensor set for sgp history where we need to join some events to
    # get a common create date. These sensors point to data_lake_private_prod instead of
    # dl_private_batch_prod because for the join we're looking for the complete data set and
    # not the delta since last load.
    'orderlines_sgp_history_join':
        {
            'sensor_timeout': SIX_HOURS_IN_SECONDS,
            'skip_sensor_on_timeout': False,
        },
    'orderlines_fx_reference':
        {
            'sensor_timeout': SIX_HOURS_IN_SECONDS,
            'skip_sensor_on_timeout': False,
        },
    'orderlines_frt_history':
        {
            'sensor_timeout': FOUR_HOUR_IN_SECONDS,
            'skip_sensor_on_timeout': True,
        },
    'orderlines_beboc_history':
        {
            'sensor_timeout': FOUR_HOUR_IN_SECONDS,
            'skip_sensor_on_timeout': True,
        },
    'orderlines_flixbus_history':
        {
            'sensor_timeout': TWO_HOUR_IN_SECONDS,
            'skip_sensor_on_timeout': True,
        },
    'orderlines_ttl_pivot':
        {
            'sensor_timeout': TWO_HOUR_IN_SECONDS,
            'skip_sensor_on_timeout': True,
        },
}

SENSOR_DB = {
    'dl_private_batch_prod': ['orderlines_sgp_history',
                              'orderlines_beboc_history'],
    'data_lake_private_prod': ['orderlines_fx_reference',
                               'orderlines_frt_history',
                               'orderlines_flixbus_history',
                               'orderlines_sgp_history_join',
                               'orderlines_ttl_pivot',
                               'refunds_tracs_pivot',
                               'refunds_ttl_pivot',
                               'orderlines_tracs_history',
                               'orderlines_25kv_history']
}

SENSOR_EXPRESSION = {
    "year_month_day='{{ next_ds }}'": ['orderlines_sgp_history',
                                       'orderlines_frt_history',
                                       'orderlines_sgp_history_join',
                                       'refunds_tracs_pivot',
                                       'refunds_ttl_pivot',
                                       'orderlines_tracs_history'],
    "year_month_day='{{ ds }}'": ['orderlines_25kv_history',
                                  'orderlines_beboc_history',
                                  'orderlines_ttl_pivot',
                                  'orderlines_flixbus_history',
                                  'orderlines_fx_reference']
}

SENSOR_TABLES = {
    'orderlines_25kv_history': sensor_tables.dl_25kv,
    'orderlines_tracs_history': sensor_tables.dl_tracs,
    'orderlines_sgp_history': sensor_tables.dl_sgp,
    'orderlines_sgp_history_join': sensor_tables.dl_sgp_join,
    'orderlines_fx_reference': sensor_tables.dl_fx,
    'orderlines_frt_history': sensor_tables.dl_frt,
    'orderlines_beboc_history': sensor_tables.dl_beboc,
    'orderlines_flixbus_history': sensor_tables.dl_flixbus,
    'orderlines_ttl_pivot': sensor_tables.dl_ttl,
    'refunds_tracs_pivot': sensor_tables.dl_tracs_refunds_service,
    'refunds_ttl_pivot': sensor_tables.dl_ttl_refunds
}


def create_partial_sensors(
        db: str,
        tables: [],
        expression: str,
        retries: int,
        timeout: int,
        soft_fail: bool,
        poke_interval: int = Defaults.SENSOR_DEFAULTS['poke_interval'],
        mode: str = Defaults.SENSOR_DEFAULTS['mode'],
        aws_conn_id: str = Defaults.SENSOR_DEFAULTS['aws_conn_id'],
        region_name: str = Defaults.SENSOR_DEFAULTS['region_name']
):
    additional_args = {}
    if soft_fail:  # Delete condition once we migrate to an airflow version that fixes the skip bug.
        additional_args.update({'on_failure_callback': None})
        soft_fail = False
    return [partial(AwsGlueCatalogPartitionSensor,
                    task_id=TASK_ID.format(db, t),
                    database_name=db,
                    table_name=t,
                    expression=expression,
                    timeout=timeout,
                    retries=retries,
                    soft_fail=soft_fail,
                    poke_interval=poke_interval,
                    mode=mode,
                    aws_conn_id=aws_conn_id,
                    region_name=region_name,
                    **additional_args) for t in tables]


def flip_dict_keys_and_list_of_values(cfg: {}):
    return dict((i, k) for k, v in cfg.items() for i in v)


def build_sensors_config(partition_set: str):
    return SimpleNamespace(db=flip_dict_keys_and_list_of_values(SENSOR_DB)[partition_set],
                           tables=SENSOR_TABLES[partition_set],
                           expression=flip_dict_keys_and_list_of_values(SENSOR_EXPRESSION)[partition_set],
                           timeout=SENSOR_CFG[partition_set]['sensor_timeout'] / (RETRIES + 1),
                           retries=RETRIES,
                           soft_fail=SENSOR_CFG[partition_set]['skip_sensor_on_timeout']
                           )


def build_sensors(partition_set: str):
    cfg = build_sensors_config(partition_set)
    sensors = create_partial_sensors(db=cfg.db,
                                     tables=cfg.tables,
                                     expression=cfg.expression,
                                     timeout=cfg.timeout,
                                     retries=cfg.retries,
                                     soft_fail=cfg.soft_fail)

    return sensors


def get_glue_partition_sensors(partition_set: str,
                               dag: DAG):
    sensor_tasks = []
    sensor_list = []

    if SENSOR_CFG.get(partition_set):
        partition_set_list = [partition_set]
        partition_set_list += SENSOR_CFG.get(partition_set) \
            .get('additional_sensors', [])
        for sensor_set in partition_set_list:
            sensor_list += build_sensors(sensor_set)

    if sensor_list:
        soft_fail = SENSOR_CFG[partition_set].get('skip_sensor_on_timeout', False)

        sensors_start = DummyOperator(dag=dag,
                                      task_id=f"{partition_set}_sensors_start")
        sensors_end = DummyOperator(dag=dag,
                                    trigger_rule="all_done" if soft_fail else "none_failed",
                                    # Revert to `none_failed` after skip bug is fixed
                                    task_id=f"{partition_set}_sensors_end")
        sensors_none_failed_required_task = DummyOperator(
            task_id=f"{partition_set}_required_to_enable_none_failed_trigger",
            dag=dag)
        sensors = [sensor(dag=dag) for sensor in sensor_list]
        sensors_none_failed_required_task >> sensors_end
        sensor_tasks = [sensors_start, sensors]
        time_delay = SENSOR_CFG.get(partition_set) \
            .get('whirlwind_processing_delay', None)
        if time_delay is not None:
            time_delay_sensor = PythonOperator(task_id=f"{partition_set}_whirlwind_delay",
                                               dag=dag,
                                               provide_context=False,
                                               python_callable=lambda: time.sleep(time_delay))
            sensor_tasks.append(time_delay_sensor)
        sensor_tasks.append(sensors_end)
    return sensor_tasks
