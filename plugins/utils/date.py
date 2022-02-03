import logging

from airflow.models.taskinstance import TaskInstance
from datetime import timedelta,datetime,time


LOG = logging.getLogger(__name__)


def get_all_possible_execution_dates(ti: TaskInstance):
    # Todo: test for frequency > once a day
    # Todo: raise exception/warning if this ti instance is manually triggered
    exec_date = ti.execution_date
    next_exec_date = ti.task.dag.following_schedule(exec_date)
    LOG.info("Current Execution date {}".format(exec_date))
    delta = next_exec_date - exec_date
    LOG.info("Timedelta {}".format(delta))

    start_date = ti.task.dag.start_date or ti.task.dag.default_args.get('start_date')
    LOG.info("Start Date {}".format(start_date))

    execution_dates = []
    current_date = exec_date
    while True:
        if current_date >= start_date:
            execution_dates.append(current_date)
        else:
            break
        current_date -= delta

    return execution_dates


def _get_next_execution_date(ti: TaskInstance):
    exec_date = ti.execution_date
    next_exec_date = ti.task.dag.following_schedule(exec_date)
    return next_exec_date


def add_next_exec_date(func):
    def wrap_func(**ctx):
        ctx['next_execution_date'] = _get_next_execution_date(ctx['ti'])
        return func(**ctx)
    return wrap_func


def get_run_date_range(ctx):
        next_exec_date = _get_next_execution_date(ctx['ti'])
        next_exec_minus = next_exec_date - timedelta(days=1)
        run_start_date_str = ctx['ds']
        ti = ctx['ti']
        run_end_date = min(dt for dt in [next_exec_minus, ti.task.dag.end_date] if dt is not None)
        run_end_date_str = run_end_date.strftime('%Y-%m-%d')
        return run_start_date_str, run_end_date_str

def get_today():
    now = datetime.datetime.now()
    return now.strftime("%Y_%m_%d")


def safely(f, max_tries, back_off, max_back_off, max_variance_pct):
    tries = 0
    current_back_off = back_off
    last_exception = None
    while tries <= max_tries:
        try:
            return f()
        except Exception as e:
            LOG.info('Safely failure: %s' % (e))
            time.sleep(current_back_off)
            current_back_off = min(max_back_off,
                                   current_back_off * (1 +
                                                       random.random() *
                                                       max_variance_pct))
            last_exception = e
            tries += 1

    # exceeded max_tries without the function returning correctly
    raise last_exception
