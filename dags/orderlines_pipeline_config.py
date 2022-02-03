from orderlines_common import (
    SGP_BACKFILL_END_DATES,
    FRT_BACKFILL_END_DATES,
    _25KV_BACKFILL_END_DATES,
    BEBOC_BACKFILL_END_DATES,
    FLIXBUS_BACKFILL_END_DATES,
    TRACS_BACKFILL_END_DATES,
    ORDERLINES_HISTORY_DBS,
    ORDERLINES_PIVOT_DBS,
    ORDERLINES_MERGE_DBS,
    BACKFILL_MIN_DATE,
    TC_ENV,
    ORDERLINES_INPUT_DB_25KV,
    ORDERLINES_INPUT_DB_FLIXBUS)


class templateSource:
    data_source = ''

    @staticmethod
    def pipelines():
        return []

    @staticmethod
    def glue_sensor_table_name():
        return str

    @staticmethod
    def glue_sensor_soft_fail(pipeline):
        return False

    @staticmethod
    def glue_sensor_expression(pipeline):
        return "execution_date='{{ ds }}'"

    @staticmethod
    def input_counter_sql():
        return str

    @staticmethod
    def output_counter_sql():
        return str

    @staticmethod
    def equality_tolerance(pipeline):
        return 0.0


class historySource(templateSource):
    input_db = ''
    output_db = ORDERLINES_HISTORY_DBS[TC_ENV.lower()]


class pivotSource(templateSource):
    input_db = ORDERLINES_HISTORY_DBS[TC_ENV.lower()]
    output_db = ORDERLINES_PIVOT_DBS[TC_ENV.lower()]


class intSource(templateSource):
    input_db = ORDERLINES_PIVOT_DBS[TC_ENV.lower()]
    output_db = ORDERLINES_PIVOT_DBS[TC_ENV.lower()]


class mergeSource(templateSource):
    input_db = ORDERLINES_PIVOT_DBS[TC_ENV.lower()]
    output_db = ORDERLINES_MERGE_DBS[TC_ENV.lower()]
