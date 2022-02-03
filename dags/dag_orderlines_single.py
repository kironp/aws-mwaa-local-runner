from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from orderlines_common import (
    DEFAULT_ARGS,
    MAX_ACTIVE_DAG_RUNS,
    TC_ENV,
    DAG_ID_SINGLE,
    Stage,
    Source,
    Project
)
from stage_classes import EtlPartialDag
from pipeline_metadata import PipelineMetaData

# IMPORT OPERATOR PARTIALS - TODO: deprecate
from orderlines_seasons_merge import full_repair_table_part as full_repair_seasons_m
from orderlines_seasons_merge import source_repair_table_part as source_repair_seasons_m
from orderlines_25kv_merge import repair_table_part_int as repair_25kv_int
from orderlines_25kv_merge import full_repair_table_part_merge as full_repair_25kv_m
from orderlines_25kv_merge import source_repair_table_part_merge as source_repair_25kv_m
from orderlines_beboc_merge import repair_table_part_int as repair_beboc_int
from orderlines_beboc_merge import full_repair_table_part_merge as full_repair_beboc_m
from orderlines_beboc_merge import source_repair_table_part_merge as source_repair_beboc_m
from orderlines_flixbus_merge import full_repair_table_part as full_repair_flixbus_m
from orderlines_flixbus_merge import source_repair_table_part as source_repair_flixbus_m
from orderlines_sgp_merge import full_repair_table_part as full_repair_sgp_m
from orderlines_sgp_merge import source_repair_table_part as source_repair_sgp_m
from orderlines_tracs_merge import full_repair_table_part as full_repair_tracs_m
from orderlines_tracs_merge import source_repair_table_part as source_repair_tracs_m
from orderlines_ttl_manual_merge import full_repair_table_part as full_repair_ttl_m
from orderlines_ttl_manual_merge import source_repair_table_part as source_repair_ttl_m

from pipelines import Daily, Backfill

DAILY_DAG_ID = f"{DAG_ID_SINGLE}_daily"
BACKFILL_DAG_ID = f"{DAG_ID_SINGLE}_backfill"


def create_dag_stages(dag, p):
    ### REFERENCE ###
    currency_fx = EtlPartialDag(pipeline_metadata=p.currency_meta,
                                sensor_metadata=p.currency_sensor_meta)

    ### SGP ###
    sgp_history = EtlPartialDag(pipeline_metadata=p.sgp_history_meta,
                                sensor_metadata=p.sgp_history_sensor_meta)

    sgp_pivot = EtlPartialDag(pipeline_metadata=p.sgp_pivot_meta,
                              dependencies=[sgp_history])

    sgp_merge = EtlPartialDag(pipeline_metadata=p.sgp_merge_meta,
                              table_repairs=full_repair_sgp_m,
                              secondary_table_repair=source_repair_sgp_m,
                              dependencies=[sgp_pivot, currency_fx])

    sgp_pivot_refunds = EtlPartialDag(pipeline_metadata=p.sgp_pivint_refunds_meta,
                                      dependencies=[sgp_pivot])

    ### 25KV ###
    _25kv_history = EtlPartialDag(pipeline_metadata=p._25kv_history_meta,
                                  sensor_metadata=p._25kv_history_sensor_meta)

    _25kv_pivot = EtlPartialDag(pipeline_metadata=p._25kv_pivot_meta,
                                dependencies=[_25kv_history])

    _25kv_int = EtlPartialDag(pipeline_metadata=p._25kv_int_meta,
                              table_repairs=repair_25kv_int,
                              dependencies=[_25kv_pivot, currency_fx])

    _25kv_merge = EtlPartialDag(pipeline_metadata=p._25kv_merge_meta,
                                table_repairs=full_repair_25kv_m,
                                secondary_table_repair=source_repair_25kv_m,
                                dependencies=[_25kv_int])

    _25kv_int_refunds = EtlPartialDag(pipeline_metadata=p._25kv_int_refunds_meta,
                                      dependencies=[_25kv_pivot])

    ### BEBOC ###
    beboc_history = EtlPartialDag(pipeline_metadata=p.beboc_history_meta,
                                  sensor_metadata=p.beboc_history_sensor_meta)

    beboc_pivot = EtlPartialDag(pipeline_metadata=p.beboc_pivot_meta,
                                dependencies=[beboc_history])

    beboc_int = EtlPartialDag(pipeline_metadata=p.beboc_int_meta,
                              table_repairs=repair_beboc_int,
                              dependencies=[beboc_pivot, sgp_pivot, currency_fx])

    beboc_merge = EtlPartialDag(pipeline_metadata=p.beboc_merge_meta,
                                table_repairs=full_repair_beboc_m,
                                secondary_table_repair=source_repair_beboc_m,
                                dependencies=[beboc_int])

    beboc_int_refunds = EtlPartialDag(pipeline_metadata=p.beboc_int_refunds_meta,
                                      dependencies=[beboc_pivot, sgp_pivot])

    ### FLIXBUS ###
    flixbus_history = EtlPartialDag(pipeline_metadata=p.flixbus_history_meta,
                                    sensor_metadata=p.flixbus_history_sensor_meta)

    flixbus_pivot = EtlPartialDag(pipeline_metadata=p.flixbus_pivot_meta,
                                  dependencies=[flixbus_history])

    flixbus_merge = EtlPartialDag(pipeline_metadata=p.flixbus_merge_meta,
                                  table_repairs=[full_repair_flixbus_m],
                                  secondary_table_repair=source_repair_flixbus_m,
                                  dependencies=[flixbus_pivot, currency_fx])

    ### FRT ###
    frt_history = EtlPartialDag(pipeline_metadata=p.frt_history_meta,
                                sensor_metadata=p.frt_history_sensor_meta)

    frt_pivot = EtlPartialDag(pipeline_metadata=p.frt_pivot_meta,
                              dependencies=[frt_history])

    frt_int_refunds = EtlPartialDag(pipeline_metadata=p.frt_int_refunds_meta,
                                    dependencies=[sgp_pivot, frt_pivot])

    ### TRACS ###
    tracs_history = EtlPartialDag(pipeline_metadata=p.tracs_history_meta,
                                  sensor_metadata=p.tracs_history_sensor_meta)

    tracs_pivot = EtlPartialDag(pipeline_metadata=p.tracs_pivot_meta,
                                dependencies=[tracs_history])

    tracs_merge = EtlPartialDag(pipeline_metadata=p.tracs_merge_meta,
                                table_repairs=full_repair_tracs_m,
                                secondary_table_repair=source_repair_tracs_m,
                                dependencies=[currency_fx, tracs_pivot])

    tracs_pivot_refunds = EtlPartialDag(
        pipeline_metadata=p.tracs_pivot_refunds_meta,
        sensor_metadata=p.tracs_pivot_refunds_sensor_meta)

    tracs_int_refunds = EtlPartialDag(pipeline_metadata=p.tracs_int_refunds_meta,
                                      dependencies=[
                                          sgp_pivot,
                                          tracs_pivot,
                                          tracs_pivot_refunds
                                      ])

    ### SEASONS ###
    seasons_merge = EtlPartialDag(pipeline_metadata=p.seasons_merge_meta,
                                  table_repairs=full_repair_seasons_m,
                                  secondary_table_repair=source_repair_seasons_m,
                                  dependencies=[
                                      tracs_pivot,
                                      currency_fx,
                                      frt_pivot
                                  ])

    seasons_int_refunds = EtlPartialDag(pipeline_metadata=p.seasons_int_refunds_meta,
                                        dependencies=[frt_pivot, tracs_pivot])

    ### TTL ###
    ttl_pivot = EtlPartialDag(pipeline_metadata=p.ttl_pivot_meta,
                              sensor_metadata=p.ttl_pivot_sensor_meta)

    ttl_merge = EtlPartialDag(pipeline_metadata=p.ttl_merge_meta,
                              table_repairs=[full_repair_ttl_m],
                              secondary_table_repair=source_repair_ttl_m,
                              dependencies=[
                                  ttl_pivot,
                                  currency_fx
                              ])

    ttl_pivot_refunds = EtlPartialDag(
        pipeline_metadata=p.ttl_pivot_refunds_meta,
        sensor_metadata=p.ttl_pivot_refunds_sensor_meta)

    ttl_int_refunds = EtlPartialDag(pipeline_metadata=p.ttl_int_refunds_meta,
                                    dependencies=[
                                        sgp_pivot,
                                        tracs_pivot,
                                        ttl_pivot_refunds
                                    ])

    ### REFUNDS MERGE ###
    refunds_merge = EtlPartialDag(dependencies=[
        sgp_pivot_refunds,
        seasons_int_refunds,
        tracs_int_refunds,
        beboc_int_refunds,
        _25kv_int_refunds,
        ttl_int_refunds,
        frt_int_refunds
    ],
        pipeline_metadata=p.refunds_merge_meta)

    start = DummyOperator(dag=dag, task_id='start')
    end = DummyOperator(dag=dag, task_id='end')

    bi_dwh = EtlPartialDag(dag=dag,
                           pipeline_metadata=PipelineMetaData(
                               project='bi',
                               stage='dwh',
                               source=Source.ALL),
                           dependencies=[
                               refunds_merge,
                               seasons_merge,
                               ttl_merge,
                               _25kv_merge,
                               sgp_merge,
                               flixbus_merge,
                               tracs_merge,
                               beboc_merge
                           ])
    bi_dwh.propagate_dependencies(start_root=start)
    bi_dwh >> end
    return bi_dwh


dag = DAG(dag_id=DAILY_DAG_ID,
         start_date=datetime.strptime('2021-06-28', "%Y-%m-%d"),
         catchup=False,
         max_active_runs=MAX_ACTIVE_DAG_RUNS[TC_ENV.lower()],
         concurrency=40,
         default_args=DEFAULT_ARGS,
         default_view='graph',
         schedule_interval='@daily',
         user_defined_macros={})

daily = create_dag_stages(dag, Daily)
globals()[daily.dag.dag_id] = daily.dag

dag = DAG(dag_id=BACKFILL_DAG_ID,
         start_date=datetime.strptime('2021-06-21', "%Y-%m-%d"),
         catchup=False,
         max_active_runs=MAX_ACTIVE_DAG_RUNS[TC_ENV.lower()],
         concurrency=40,
         default_args=DEFAULT_ARGS,
         default_view='graph',
         schedule_interval=None,
         user_defined_macros={})

backfill = create_dag_stages(dag, Backfill)
globals()[backfill.dag.dag_id] = backfill.dag
