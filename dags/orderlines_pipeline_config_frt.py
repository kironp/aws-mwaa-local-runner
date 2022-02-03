from orderlines_common import (
    FRT_BACKFILL_END_DATES,
    BACKFILL_MIN_DATE,
    TC_ENV,
    Stage,
    Source)

from orderlines_pipeline_config import (
    historySource,
    pivotSource
)


class frtHistory(historySource):

    data_source = Source.FRT
    etl_stage = Stage.HISTORY
    input_db = 'data_lake_private_prod'
    backfill_end_date = FRT_BACKFILL_END_DATES[TC_ENV.lower()]['history']

    @staticmethod
    def pipelines():
        return ['direct', 'tracs']

    @staticmethod
    def glue_sensor_table_name(pipeline):
        return f'hist_{frtHistory.data_source}_{pipeline}_season_transactions'

    @staticmethod
    def glue_sensor_soft_fail(pipeline):
        return True

    @staticmethod
    def input_counter_sql(pipeline):
        if pipeline == 'direct':
            tracs_direct = """
           and (NOT regexp_like(referencefield1, '^[2-3]{1}[0-9]{9}$')
                OR COALESCE (sitecode, 'TB1') IN ('T2L', 'T2S', 'THS', 'TTL'))"""
        else:
            tracs_direct = """
           and regexp_like(referencefield1, '^[2-3]{1}[0-9]{9}$')
           and COALESCE (sitecode, 'TB1') NOT IN ('T2L', 'T2S', 'THS', 'TTL')"""

        return """
select count(*)
    from seasonsfrt
    where year_month_day BETWEEN CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                                                THEN '%(backfill_min_date)s'
                                                ELSE '{{ ds }}'
                                            END
                                        AND '{{ dag.next_day(ds) }}'
                            AND (referencefield5 != 'SGP' OR referencefield5 IS NULL)""" \
                % {"backfill_end_date": frtHistory.backfill_end_date,
                   "backfill_min_date": BACKFILL_MIN_DATE} + tracs_direct

    @staticmethod
    def output_counter_sql(pipeline):
        return """
        select count(*)
          from %(output_table)s
         where execution_date='{{ ds }}'""" \
             % {"output_table": 'hist_{}_{}_season_transactions'
                                .format(frtHistory.data_source, pipeline)}


class frtPivot(pivotSource):

    data_source = Source.FRT
    etl_stage = Stage.PIVOT
    backfill_end_date = FRT_BACKFILL_END_DATES[TC_ENV.lower()]['pivot']

    input_expression = "(( execution_date BETWEEN '%s' AND '{{ run_end_date_str }}' AND '%s' >= '{{ run_end_date_str }}' ) or ( execution_date = '{{ run_end_date_str }}' and '%s' < '{{ run_end_date_str }}' ))" % (
        BACKFILL_MIN_DATE, backfill_end_date, backfill_end_date)

    @staticmethod
    def pipelines():
        return ['direct', 'tracs']

    @staticmethod
    def glue_sensor_table_name(pipeline):
        return f'pivoted_{frtPivot.data_source}_{pipeline}_season_transactions'

    @staticmethod
    def glue_sensor_soft_fail(pipeline):
        return True

    @staticmethod
    def input_counter_sql(pipeline):

        input_table = f'hist_{frtPivot.data_source}_{pipeline}_season_transactions'

        if pipeline == 'tracs':
            partition_field = 'tr_id_partition'
        else:
            partition_field = 'order_id_partition'

        where_clause = f'{partition_field} >= (select min({partition_field}) from {input_table}'
        where_clause += " where " + frtPivot.input_expression + ")"

        return """
        select count(distinct orderid)
          from %(input_table)s
         where %(where_clause)s """ \
             % {"input_table": input_table,
                "where_clause": where_clause}

    @staticmethod
    def output_counter_sql(pipeline):
        return """
        select count(*)
          from %(output_table)s
         where execution_date='{{ ds }}'""" \
             % {"output_table": f'pivoted_{frtPivot.data_source}_{pipeline}_season_transactions'}