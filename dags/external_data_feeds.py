from pipeline_metadata import PipelineMetaData
from stage_classes import EtlPartialDag
import logging
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from pipelines import Daily, Backfill
from orderlines_common import (
    TC_ENV,
    DAG_ID_EXTERNALDATA_SINGLE,
    DEFAULT_ARGS,
    MAX_ACTIVE_DAG_RUNS
)

DAILY_DAG_ID = f"{DAG_ID_EXTERNALDATA_SINGLE}_daily"
BACKFILL_DAG_ID = f"{DAG_ID_EXTERNALDATA_SINGLE}_backfill"


def create_dag_stages(dag, p):
    ### D365 General Ledger ###
    externaldata_history = EtlPartialDag(pipeline_metadata=p.external_data_feeds_history_meta)

    externaldata_pivot = EtlPartialDag(dag=dag, pipeline_metadata=p.external_data_feeds_pivot_meta,
                                       dependencies=[externaldata_history])
    start = DummyOperator(dag=dag, task_id='start')
    end = DummyOperator(dag=dag, task_id='end')
    externaldata_pivot.propagate_dependencies(start_root=start)
    externaldata_pivot >> end
    return externaldata_pivot


dag = DAG(dag_id=DAILY_DAG_ID,
          start_date=datetime.strptime('2021-10-29', "%Y-%m-%d"),
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
          start_date=datetime.strptime('2021-01-01', "%Y-%m-%d"),
          catchup=False,
          max_active_runs=MAX_ACTIVE_DAG_RUNS[TC_ENV.lower()],
          concurrency=40,
          default_args=DEFAULT_ARGS,
          default_view='graph',
          schedule_interval=None,
          user_defined_macros={})


backfill = create_dag_stages(dag, Backfill)
globals()[backfill.dag.dag_id] = backfill.dag
