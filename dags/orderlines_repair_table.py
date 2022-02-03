import logging
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from orderlines_common import DEFAULT_ARGS, ORDERLINES_MERGE_DBS, \
    repair_table_callable, ORDERLINES_TABLE, ORDERLINES_CURRENT_TABLE, SGP_BACKFILL_END_DATES, TRACS_BACKFILL_END_DATES, \
    FRT_BACKFILL_END_DATES, _25KV_BACKFILL_END_DATES, FLIXBUS_BACKFILL_END_DATES, TC_ENV
from orderlines_version import __orderlines_version__

LOG = logging.getLogger(__name__)

DAG_ID = '_'.join(['orderlines_repair_table', __orderlines_version__])

DEFAULT_START_DATE = max(SGP_BACKFILL_END_DATES.get(TC_ENV.lower()).get('merge','2000-01-01'),
                         TRACS_BACKFILL_END_DATES.get(TC_ENV.lower()).get('merge','2000-01-01'),
                         FRT_BACKFILL_END_DATES.get(TC_ENV.lower()).get('merge','2000-01-01'),
                         _25KV_BACKFILL_END_DATES.get(TC_ENV.lower()).get('merge','2000-01-01'),
                         FLIXBUS_BACKFILL_END_DATES.get(TC_ENV.lower()).get('merge','2000-01-01'))

with DAG(dag_id=DAG_ID,
         catchup=False,
         max_active_runs=1,
         start_date=datetime.strptime(DEFAULT_START_DATE, "%Y-%m-%d"),
         default_args=DEFAULT_ARGS,
         default_view='graph',
         schedule_interval='0 22 * * *') as dag:
    PythonOperator(dag=dag,
                   task_id='repair_table.{}.{}'.format(ORDERLINES_MERGE_DBS[TC_ENV.lower()], ORDERLINES_TABLE),
                   python_callable=repair_table_callable,
                   op_kwargs={'aws_region': 'eu-west-1',
                              'db_name': ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                              'table_name': ORDERLINES_TABLE})
