from datetime import datetime

from orderlines_common import (
    TRACS_BACKFILL_END_DATES,
    BACKFILL_MIN_DATE,
    TC_ENV,
    Stage,
    Source,
    Project)

from orderlines_pipeline_config import (
    historySource,
    pivotSource,
    mergeSource
)


class tracsHistory(historySource):
    data_source = Source.TRACS
    etl_stage = Stage.HISTORY
    input_db = 'data_lake_private_prod'
    backfill_end_date = TRACS_BACKFILL_END_DATES[TC_ENV.lower()]['history']

    pipelines_with_join = ['journey_legs',
                           'linked_bookings',
                           'supplements']

    pipeline_list = ['transactions',
                     'bookings',
                     'payments',
                     'contact_details',
                     'deliveries',
                     'delivery_statuses',
                     'season_bookings',
                     'transaction_booking_actions',
                     'adjustments',
                     'reservations',
                     'reservation_attributes'] + pipelines_with_join

    pipeline_soft_fail = ['transaction_booking_actions']

    @staticmethod
    def pipelines():
        return tracsHistory.pipeline_list

    @staticmethod
    def glue_sensor_table_name(pipeline):
        return f'hist_{tracsHistory.data_source}_{pipeline}'

    @staticmethod
    def glue_sensor_soft_fail(pipeline):

        if pipeline in tracsHistory.pipeline_soft_fail:
            return True
        else:
            return False

    @staticmethod
    def input_counter_sql(pipeline):

        input_table = f'tracs_{pipeline} l'

        if pipeline in tracsHistory.pipelines_with_join:
            select_clause = """
            select count(distinct l.id)
              from """ + input_table + """
        {% if ds == '""" + tracsHistory.backfill_end_date + """' %}
              join tracs_bookings b
                on b.id = l.bo_id
        {% endif %}"""
        else:
            select_clause = """
            select count(*)
              from %(input_table)s""" \
                            % {"input_table": input_table}

        where_clause = """
             where l.year_month_day BETWEEN CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                                                 THEN '%(backfill_min_date)s'
                                                 ELSE '{{ next_ds }}'
                                             END
                                        AND '{{ next_ds }}'""" \
                       % {"backfill_end_date": tracsHistory.backfill_end_date,
                          "backfill_min_date": BACKFILL_MIN_DATE}

        return select_clause + where_clause

    @staticmethod
    def output_counter_sql(pipeline):
        if pipeline in tracsHistory.pipelines_with_join:
            select_clause = 'select count(distinct id)'
        else:
            select_clause = 'select count(*)'

        return select_clause + """
          from %(output_table)s
         where execution_date='{{ ds }}'""" \
               % {"output_table": 'hist_{}_{}'.format(tracsHistory.data_source, pipeline)}


class tracsPivot(pivotSource):
    data_source = Source.TRACS
    etl_stage = Stage.PIVOT
    backfill_end_date = TRACS_BACKFILL_END_DATES[TC_ENV.lower()]['pivot']

    input_expression = "(( execution_date BETWEEN '%s' AND '{{ run_end_date_str }}' AND '%s' >= '{{ run_end_date_str }}' ) or ( execution_date = '{{ run_end_date_str }}' and '%s' < '{{ run_end_date_str }}' ))" % (
        BACKFILL_MIN_DATE, backfill_end_date, backfill_end_date)

    pipeline_data = {'transactions': {'count_expression': 'distinct id',
                                      'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_transactions where " + input_expression + ")"},
                     'bookings': {'count_expression': 'distinct id',
                                  'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_bookings where " + input_expression + ")"},
                     'payments': {'count_expression': 'distinct id',
                                  'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_payments where " + input_expression + ")"},
                     'linked_bookings': {'count_expression': 'distinct bo_id',
                                         'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_linked_bookings where " + input_expression + ")"},
                     'journey_legs': {'count_expression': 'distinct id',
                                      'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_journey_legs where " + input_expression + ")"},
                     'contact_details': {'count_expression': 'distinct co_id',
                                         'where_expression': "co_id_partition >= (select min(co_id_partition) from " + pivotSource.input_db + ".hist_tracs_contact_details where " + input_expression + ")"},
                     'deliveries': {'count_expression': 'distinct id',
                                    'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_deliveries where " + input_expression + ")"},
                     'delivery_statuses': {'count_expression': 'distinct del_id',
                                           'where_expression': "del_id_partition >= (select min(del_id_partition) from " + pivotSource.input_db + ".hist_tracs_delivery_statuses where " + input_expression + ")"},
                     'supplements': {'count_expression': 'distinct id',
                                     'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_supplements where " + input_expression + ")"},
                     'customer_managed_groups': {'is_reference_table': True,
                                                 'input_db': 'data_lake_private_prod',
                                                 'count_expression': 'distinct cu_id',
                                                 'where_expression': "1=1"},
                     'season_bookings': {'count_expression': 'distinct id',
                                         'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_season_bookings where " + input_expression + ")"},
                     'stations': {'is_reference_table': True,
                                  'input_db': 'data_lake_private_prod',
                                  'count_expression': 'distinct code',
                                  'where_expression': "year_month_day <= '{{ run_end_date_str }}'"},
                     'corporates': {'is_reference_table': True,
                                    'input_db': 'data_lake_private_prod',
                                    'count_expression': 'distinct id',
                                    'where_expression': "year_month_day <= '{{ run_end_date_str }}'"},
                     'ticket_types': {'is_reference_table': True,
                                      'input_db': 'data_lake_private_prod',
                                      'count_expression': 'distinct code',
                                      'where_expression': "year_month_day <= '{{ run_end_date_str  }}'"},
                     'season_refunds': {'is_reference_table': True,
                                        'input_db': 'data_lake_private_prod',
                                        'count_expression': 'distinct id',
                                        'where_expression': "year_month_day <= '{{ run_end_date_str }}'"},
                     'cg_ref_codes': {'is_reference_table': True,
                                      'input_db': 'data_lake_private_prod',
                                      'count_expression': """DISTINCT
                                                              COALESCE (rv_domain, 'A') ||
                                                              COALESCE (rv_meaning, 'A') ||
                                                              COALESCE (rv_high_value, 'A') ||
                                                              COALESCE (rv_low_value, 'A') ||
                                                              COALESCE (rv_abbreviation, 'A')""",
                                      'where_expression': "year_month_day <= '{{ run_end_date_str }}'"},
                     'interim_refunds': {'is_reference_table': True,
                                         'input_db': 'data_lake_private_prod',
                                         'count_expression': 'distinct id',
                                         'where_expression': "year_month_day <= '{{ run_end_date_str  }}'"},
                     'supplement_types': {'is_reference_table': True,
                                          'input_db': 'data_lake_private_prod',
                                          'count_expression': 'distinct code',
                                          'where_expression': "year_month_day <= '{{ run_end_date_str  }}'"},
                     'reservations': {
                         'count_expression': "distinct cast(jl_id as varchar)||'-'||cast(seq_num as varchar)",
                         'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_reservations where " + input_expression + ")"},
                     'reservation_attributes': {
                         'count_expression': 'distinct id',
                         'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_reservation_attributes where " + input_expression + ")"},
                     'adjustments': {
                         'count_expression': "distinct cast(bo_id as varchar)||'-'||reason",
                         'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_adjustments where " + input_expression + ")"},
                     'service_providers': {'is_reference_table': True,
                                           'input_db': 'data_lake_private_prod',
                                           'count_expression': 'distinct code||cast(eff_from_date as varchar)',
                                           'where_expression': "year_month_day <= '{{ run_end_date_str  }}'"},
                     'agents': {'is_reference_table': True,
                                           'input_db': 'data_lake_private_prod',
                                           'count_expression': 'distinct id',
                                           'where_expression': "year_month_day <= '{{ run_end_date_str  }}'"},
                     'agent_profiles': {'is_reference_table': True,
                                           'input_db': 'data_lake_private_prod',
                                           "count_expression": "distinct cast(ag_id as varchar)||'-'||cast(mg_id as varchar)",
                                           'where_expression': "year_month_day <= '{{ run_end_date_str  }}'"},
                     'transaction_booking_actions': {'count_expression': 'distinct id',
                                                     'where_expression': "tr_id_partition >= (select min(tr_id_partition) from " + pivotSource.input_db + ".hist_tracs_transaction_booking_actions where " + input_expression + ")",
                                                     'soft_fail': True}
                     }

    @staticmethod
    def pipelines():
        return list(tracsPivot.pipeline_data.keys())

    @staticmethod
    def glue_sensor_table_name(pipeline):

        if pipeline == 'cg_ref_codes':
            # cg_ref_codes doesn't write a new partition.
            # Use transactions table to detect the job is complete
            return f'pivoted_{tracsPivot.data_source}_transactions'
        else:
            return f'pivoted_{tracsPivot.data_source}_{pipeline}'

    @staticmethod
    def glue_sensor_soft_fail(pipeline):

        if tracsPivot.pipeline_data[pipeline].get('soft_fail'):
            return True
        else:
            return False

    @staticmethod
    def input_counter_sql(pipeline):

        if tracsPivot.pipeline_data[pipeline].get('is_reference_table'):
            input_table = '{}.{}_{}'.format(tracsPivot.pipeline_data[pipeline]['input_db'],
                                            tracsPivot.data_source,
                                            pipeline)
        else:
            input_table = f'hist_{tracsPivot.data_source}_{pipeline}'

        return """
        select count(%(count_expression)s)
          from %(input_table)s
         where %(where_clause)s """ \
               % {"count_expression": tracsPivot.pipeline_data[pipeline]['count_expression'],
                  "input_table": input_table,
                  "where_clause": tracsPivot.pipeline_data[pipeline]['where_expression']}

    @staticmethod
    def output_counter_sql(pipeline):

        return """
        select count(*)
          from %(output_table)s
         where execution_date='{{ ds }}'""" \
               % {"output_table": f'pivoted_{tracsPivot.data_source}_{pipeline}'}
