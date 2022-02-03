from datetime import datetime

from orderlines_common import (
    BEBOC_BACKFILL_END_DATES,
    BACKFILL_MIN_DATE,
    TC_ENV,
    Stage,
    Source)

from orderlines_pipeline_config import (
    historySource,
    pivotSource,
    intSource,
    mergeSource
)

class bebocHistory(historySource):
    data_source = Source.BEBOC
    etl_stage = Stage.HISTORY
    input_db = 'dl_private_batch_prod'
    backfill_end_date = BEBOC_BACKFILL_END_DATES[TC_ENV.lower()]['history']

    pipeline_data =  {'after_sales_intents': 'bebocaftersalesintents',
                      'channels': 'bebocchannels',
                      'discounts': 'bebocdiscounts',
                      'folders': 'bebocfolders',
                      'invoice_lines_by_invoice_id': 'bebocinvoicelines',
                      'invoice_lines_by_payable_id': 'bebocinvoicelines',
                      'invoices': 'bebocinvoices',
                      'orders': 'bebocorders',
                      'payments': 'bebocpayments',
                      'pnrs_by_header_id': 'bebocpnrs',
                      'pnrs_by_pnr_id': 'bebocpnrs',
                      'segments': 'bebocsegments',
                      'tickets': 'beboctickets',
                      'trips': 'beboctrips',
                      'pnr_headers': 'bebocpnrheaders',
                      'refunds': 'bebocrefunds',
                      'subscriptions': 'bebocsubscriptions'}

    @staticmethod
    def pipelines():
        return list(bebocHistory.pipeline_data.keys())

    @staticmethod
    def glue_sensor_table_name(pipeline):
        return f'hist_{bebocHistory.data_source}_{pipeline}'

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
             % {"input_table": bebocHistory.pipeline_data[pipeline],
                "backfill_min_date": BACKFILL_MIN_DATE, 
                "backfill_end_date": bebocHistory.backfill_end_date} 

    @staticmethod
    def output_counter_sql(pipeline):
        return """
        select count(*)
          from %(output_table)s
         where execution_date='{{ ds }}'""" \
             % {"output_table": 'hist_{}_{}'.format(bebocHistory.data_source, pipeline)}


class bebocPivot(pivotSource):
    data_source = Source.BEBOC
    etl_stage = Stage.PIVOT
    backfill_end_date = BEBOC_BACKFILL_END_DATES[TC_ENV.lower()]['pivot']

    input_expression = "(( execution_date BETWEEN '%s' AND '{{ run_end_date_str }}' AND '%s' >= '{{ run_end_date_str }}' ) or ( execution_date = '{{ run_end_date_str }}' and '%s' < '{{ run_end_date_str }}' ))" % (
    BACKFILL_MIN_DATE, backfill_end_date, backfill_end_date)

    pipeline_data = {'channels': {'count_expression': 'distinct id',
                                  'where_expression': "id_truncated >= (select min(id_truncated) from " + pivotSource.input_db + ".hist_beboc_channels where " + input_expression + ")"},
             'discounts': {'count_expression': 'distinct id',
                           'where_expression': "pnr_id_truncated >= (select min(pnr_id_truncated) from " + pivotSource.input_db + ".hist_beboc_discounts where "+ input_expression + ")"},
             'invoice_lines_by_invoice_id': {'count_expression': 'distinct id',
                                             'where_expression': "invoice_id_truncated >= (select min(invoice_id_truncated) from " + pivotSource.input_db + ".hist_beboc_invoice_lines_by_invoice_id where "+ input_expression + ")"},
             'invoice_lines_by_payable_id': {'count_expression': 'distinct id',
                                             'where_expression': "payable_id_truncated >= (select min(payable_id_truncated) from " + pivotSource.input_db + ".hist_beboc_invoice_lines_by_payable_id where "+ input_expression + ")"},
             'invoices': {'count_expression': 'distinct id',
                          'where_expression': "id_truncated >= (select min(id_truncated) from " + pivotSource.input_db + ".hist_beboc_invoices where " + input_expression + ")"},
             'orders': {'count_expression': 'distinct id',
                        'where_expression': "user_id_truncated >= (select min(user_id_truncated) from " + pivotSource.input_db + ".hist_beboc_orders where " + input_expression + ")"},
             'payments': {'count_expression': 'distinct id',
                          'where_expression': "invoice_id_truncated >= (select min(invoice_id_truncated) from " + pivotSource.input_db + ".hist_beboc_payments where " + input_expression + ")"},
             'pnrs_by_pnr_id': {'count_expression': 'distinct id',
                                'where_expression': "id_truncated >= (select min(id_truncated) from " + pivotSource.input_db + ".hist_beboc_pnrs_by_pnr_id where " + input_expression + ")"},
             'pnrs_by_header_id': {'count_expression': 'distinct id',
                                   'where_expression': "header_id_truncated >= (select min(header_id_truncated) from " + pivotSource.input_db + ".hist_beboc_pnrs_by_header_id where " + input_expression + ")"},
             'tickets': {'count_expression': 'distinct id',
                         'where_expression': "pnr_id_truncated >= (select min(pnr_id_truncated) from " + pivotSource.input_db + ".hist_beboc_tickets where " + input_expression + ")"},
             'stations': {'is_reference_table': True,
                          'input_db': 'data_lake_private_prod',
                          'input_table': '"bebocstations"',
                          'count_expression': 'distinct id',
                          'where_expression': "year_month_day <= '{{ run_end_date_str }}'"},
             'geo_locations': {'is_reference_table': True,
                               'input_db': 'data_lake_private_prod',
                               'input_table': '"bebocgeolocations"',
                               'count_expression': 'distinct id',
                               'where_expression': "year_month_day <= '{{ run_end_date_str }}'"},
             'folders': {'count_expression': 'distinct id',
                         'where_expression': "created_at_truncated >= (select min(created_at_truncated) from " + pivotSource.input_db + ".hist_beboc_folders where " + input_expression + ")"},
             'trips': {'count_expression': 'distinct id',
                       'where_expression': "created_at_truncated >= (select min(created_at_truncated) from " + pivotSource.input_db + ".hist_beboc_trips where " + input_expression + ")"},
             'segments': {'count_expression': 'distinct id',
                          'where_expression': "created_at_truncated >= (select min(created_at_truncated) from " + pivotSource.input_db + ".hist_beboc_segments where " + input_expression + ")"},
             'after_sales_intents': {'count_expression': 'distinct id',
                                     'where_expression': "id_truncated >= (select min(id_truncated) from " + pivotSource.input_db + ".hist_beboc_after_sales_intents where " + input_expression + ")"},
             'refunds': {'count_expression': 'distinct id',
                         'where_expression': "id_truncated >= (select min(id_truncated) from " + pivotSource.input_db + ".hist_beboc_refunds where " + input_expression + ")"},
             'pnr_headers': {'count_expression': 'distinct id',
                             'where_expression': "id_truncated >= (select min(id_truncated) from " + pivotSource.input_db + ".hist_beboc_pnr_headers where " + input_expression + ")"},
             'subscriptions': {'count_expression': 'distinct event_id',
                                             'where_expression': "event_id_truncated >= (select min(event_id_truncated) from " + pivotSource.input_db + ".hist_beboc_subscriptions where " + input_expression + ")"},
    }

    @staticmethod
    def pipelines():
        return list(bebocPivot.pipeline_data.keys())

    @staticmethod
    def glue_sensor_table_name(pipeline):
        return f'pivoted_{bebocPivot.data_source}_{pipeline}'

    @staticmethod
    def input_counter_sql(pipeline):

        if bebocPivot.pipeline_data[pipeline].get('is_reference_table'):
            input_table = '{}.{}'.format(bebocPivot.pipeline_data[pipeline]['input_db'],
                                         bebocPivot.pipeline_data[pipeline]['input_table'],
                                         pipeline)
        else:
            input_table = f'hist_{bebocPivot.data_source}_{pipeline}'

        return """
        select count(%(count_expression)s)
          from %(input_table)s
         where %(where_clause)s """ \
             % {"count_expression": bebocPivot.pipeline_data[pipeline]['count_expression'],
                "input_table": input_table,
                "where_clause": bebocPivot.pipeline_data[pipeline]['where_expression']}

    @staticmethod
    def output_counter_sql(pipeline):
        return """
        select count(*)
          from %(output_table)s
         where execution_date='{{ ds }}'""" \
             % {"output_table": f'pivoted_{bebocPivot.data_source}_{pipeline}'}


class bebocInt(intSource):
    data_source = Source.BEBOC
    etl_stage = Stage.INT
    backfill_end_date = BEBOC_BACKFILL_END_DATES[TC_ENV.lower()]['merge']

    invoice_source = """ (
with invoices_updated as (
select CASE WHEN '{{ ds }}' = '""" + backfill_end_date + """'
            THEN 0
            ELSE COALESCE(min(ilv.min_invoice_id_truncated),0)
        END as min_invoice_id_truncated
      ,COALESCE(max(ilv.max_invoice_id_truncated),0) as max_invoice_id_truncated
  from (select min(id_truncated) as min_invoice_id_truncated
              ,max(id_truncated) as max_invoice_id_truncated
          from """ + intSource.input_db + """.pivoted_beboc_invoices
         where execution_date = '{{ run_end_date_str }}'
        union all
        select min(invoice_id_truncated) as min_invoice_id_truncated
              ,max(invoice_id_truncated) as max_invoice_id_truncated
          from """ + intSource.input_db + """.pivoted_beboc_invoice_lines_by_invoice_id
         where execution_date = '{{ run_end_date_str }}'
        union all
        select min(invoice_id_truncated) as min_invoice_id_truncated
              ,max(invoice_id_truncated) as max_invoice_id_truncated
          from """ + intSource.input_db + """.pivoted_beboc_payments
         where execution_date = '{{ run_end_date_str }}'
       ) ilv )
select id
from """ + intSource.input_db + """.pivoted_beboc_invoice_lines_by_invoice_id
where invoice_id_truncated >= (select min_invoice_id_truncated from invoices_updated) 
  and invoice_id_truncated <= (select max_invoice_id_truncated from invoices_updated)
) """
    pnr_products_source = """ (
with folder_dates as (
    select CASE WHEN '{{ ds }}' = '""" + backfill_end_date + """'
                THEN '1900-01-01'
                ELSE COALESCE( min(min_created_at_truncated), '1900-01-01')
            END as min_created_at_truncated
          ,COALESCE( max(max_created_at_truncated), '1900-01-01') as max_created_at_truncated
    from (
        select min(created_at_truncated) as min_created_at_truncated
              ,max(created_at_truncated) as max_created_at_truncated
          from """ + intSource.input_db + """.pivoted_beboc_segments
         where execution_date = '{{ run_end_date_str }}'
        union all
        select min(created_at_truncated) as min_created_at_truncated
              ,max(created_at_truncated) as max_created_at_truncated
          from """ + intSource.input_db + """.pivoted_beboc_folders
         where execution_date = '{{ run_end_date_str }}'
        union all
        select min(created_at_truncated) as min_created_at_truncated
              ,max(created_at_truncated) as max_created_at_truncated
          from """ + intSource.input_db + """.pivoted_beboc_trips
         where execution_date = '{{ run_end_date_str }}' ) )
,pnrs_updated as (
    select CASE WHEN '{{ ds }}' = '""" + backfill_end_date + """'
                THEN 0
                ELSE min(ilv.min_pnr_id_truncated)
            END as min_pnr_id_truncated
      ,max(ilv.max_pnr_id_truncated) as max_pnr_id_truncated
    from (select min(id_truncated) as min_pnr_id_truncated
              ,max(id_truncated) as max_pnr_id_truncated
          from """ + intSource.input_db + """.pivoted_beboc_pnrs_by_pnr_id
         where execution_date = '{{ run_end_date_str }}'
        union all
        select min(pnr_id_truncated) as min_pnr_id_truncated
              ,max(pnr_id_truncated) as max_pnr_id_truncated
          from """ + intSource.input_db + """.pivoted_beboc_tickets
         where execution_date = '{{ run_end_date_str }}'
           and converted_currency IS NOT NULL
        union all
        select min(pnr_id_truncated) as min_pnr_id_truncated
              ,max(pnr_id_truncated) as max_pnr_id_truncated
          from """ + intSource.input_db + """.pivoted_beboc_discounts
         where execution_date = '{{ run_end_date_str }}'
        union all
        select min(pnr_id_truncated) as min_pnr_id_truncated
              ,max(pnr_id_truncated) as max_pnr_id_truncated
          from """ + intSource.input_db + """.pivoted_beboc_folders
         where created_at_truncated >= ( select min_created_at_truncated from folder_dates ) 
           and created_at_truncated <= ( select max_created_at_truncated from folder_dates )
       ) ilv )
,pnr_headers_updated as (
    select CASE WHEN '{{ ds }}' = '""" + backfill_end_date + """'
                THEN 1
                ELSE min(header_id_truncated) 
            END as min_pnr_header_id_truncated
          ,max(header_id_truncated) as max_pnr_header_id_truncated
    from   """ + intSource.input_db + """.pivoted_beboc_pnrs_by_pnr_id
    where  id_truncated >= ( select min_pnr_id_truncated from pnrs_updated )
       and id_truncated <= ( select max_pnr_id_truncated from pnrs_updated )
       and header_id_truncated <> 0
)
select id
from   """ + intSource.input_db + """.pivoted_beboc_pnrs_by_pnr_id
where  header_id_truncated >= ( select min_pnr_header_id_truncated from pnr_headers_updated )
   and header_id_truncated <= ( select max_pnr_header_id_truncated from pnr_headers_updated )
) """

    pipeline_data = {
        'order_invoice_lines': {
            'output_db': intSource.input_db,
            'output_table': 'int_25kv_order_invoice_lines',
            'source_tables': {
                'invoices': 'pivot',
                'invoice_lines_by_invoice_id': 'pivot',
                'payments': 'pivot',
                'channels': 'pivot',
                'orders': 'pivot'},
            'source_db': intSource.input_db,
            'source_from_subquery': invoice_source,
            'internal_table': True,
            'output_count_expression': 'invoice_line_id',
            'output_where_expression': "execution_date = '{{ run_end_date_str }}'"}, # noqa: 501
        'pnr_product_fare_legs': {
            'output_db': intSource.input_db,
            'output_table': 'int_25kv_pnr_product_fare_legs',
            'source_tables': {
                'pnrs_by_pnr_id': 'pivot',
                'pnrs_by_header_id': 'pivot',
                'folders': 'pivot',
                'trips': 'pivot',
                'segments': 'pivot',
                'stations': 'pivot',
                'segments': 'pivot',
                'tickets': 'pivot',
                'discounts': 'pivot',
                'sources': 'pivot',
                'channels': 'pivot'},
            'source_db': intSource.input_db,
            'source_from_subquery': pnr_products_source,
            'internal_table': True,
            'output_count_expression': 'distinct id',
            'output_where_expression': "execution_date = '{{ run_end_date_str }}'"} # noqa: 501
    }

    @staticmethod
    def pipelines():
        return list(bebocInt.pipeline_data.keys())

    @staticmethod
    def glue_sensor_table_name(pipeline):
        return f'int_{bebocInt.data_source}_{pipeline}'

    @staticmethod
    def input_counter_sql(pipeline):

        return "select count(*) \n" \
             + "  from " \
             + bebocInt.pipeline_data[pipeline]['source_from_subquery']

    @staticmethod
    def output_counter_sql(pipeline):

        return """
select count(%(count_expression)s)
    from %(output_table)s
    where execution_date='{{ ds }}'""" \
        % {"count_expression": bebocInt.pipeline_data[pipeline]['output_count_expression'], # noqa 501
           "output_table": f'int_{bebocInt.data_source}_{pipeline}'}
