from datetime import datetime

from orderlines_common import (
    FLIXBUS_BACKFILL_END_DATES,
    BACKFILL_MIN_DATE,
    TC_ENV,
    ORDERLINES_INPUT_DB_FLIXBUS,
    Stage,
    Source)

from orderlines_pipeline_config import (
    historySource,
    pivotSource,
    mergeSource
)


class flixbusHistory(historySource):
    data_source = Source.FLIXBUS
    etl_stage = Stage.HISTORY
    input_db = ORDERLINES_INPUT_DB_FLIXBUS
    backfill_end_date = FLIXBUS_BACKFILL_END_DATES[TC_ENV.lower()]['history']
    pipeline_data = {'actions': 'flixbusactions'}

    @staticmethod
    def pipelines():
        return list(flixbusHistory.pipeline_data.keys())

    @staticmethod
    def glue_sensor_table_name(pipeline):
        return f'hist_{flixbusHistory.data_source}_{pipeline}'

    @staticmethod
    def input_counter_sql(pipeline):
        return """
        select count(*)
          from "%(input_table)s"
         where (( year_month_day BETWEEN '%(backfill_min_date)s' 
                                     AND '{{ ds }}' 
                  AND '%(backfill_end_date)s' >= '{{ ds }}' ) 
            or ( year_month_day = '{{ ds }}' 
                 and '%(backfill_end_date)s' < '{{ ds }}' ))""" \
             % {"input_table": flixbusHistory.pipeline_data[pipeline],
                "backfill_min_date": BACKFILL_MIN_DATE, 
                "backfill_end_date": flixbusHistory.backfill_end_date} 

    @staticmethod
    def output_counter_sql(pipeline):
        return """
        select count(*)
          from %(output_table)s
         where execution_date='{{ ds }}'""" \
             % {"output_table": 'hist_{}_{}'.format(flixbusHistory.data_source, pipeline)}


class flixbusPivot(pivotSource):
    data_source = Source.FLIXBUS
    etl_stage = Stage.PIVOT
    backfill_end_date = FLIXBUS_BACKFILL_END_DATES[TC_ENV.lower()]['pivot']

    input_expression = "(( execution_date BETWEEN '%s' AND '{{ run_end_date_str }}' AND '%s' >= '{{ run_end_date_str }}' ) or ( execution_date = '{{ run_end_date_str }}' and '%s' < '{{ run_end_date_str }}' ))" % (
    BACKFILL_MIN_DATE, backfill_end_date, backfill_end_date)

    pipeline_data = {'actions': {'count_expression': 'distinct id || state',
                                 'where_expression': "eventdate_truncated >= (select min(eventdate_truncated) from " + pivotSource.input_db + ".hist_flixbus_actions where " + input_expression + ")",
                                 'required_dl_table': 'hist_flixbus_actions'}
    }

    @staticmethod
    def pipelines():
        return list(flixbusPivot.pipeline_data.keys())

    @staticmethod
    def glue_sensor_table_name(pipeline):
        return f'pivoted_{flixbusPivot.data_source}_{pipeline}'

    @staticmethod
    def input_counter_sql(pipeline):

        input_table = f'hist_{flixbusPivot.data_source}_{pipeline}'

        return """
        select count(%(count_expression)s)
          from %(input_table)s
         where %(where_clause)s """ \
             % {"count_expression": flixbusPivot.pipeline_data[pipeline]['count_expression'],
                "input_table": input_table,
                "where_clause": flixbusPivot.pipeline_data[pipeline]['where_expression']}

    @staticmethod
    def output_counter_sql(pipeline):
        return """
        select count(*)
          from %(output_table)s
         where execution_date='{{ ds }}'""" \
             % {"output_table": f'pivoted_{flixbusPivot.data_source}_{pipeline}'}