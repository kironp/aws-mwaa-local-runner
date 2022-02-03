from datetime import datetime

from orderlines_common import (
    SGP_BACKFILL_END_DATES,
    BACKFILL_MIN_DATE,
    TC_ENV,
    Stage,
    Source)

from orderlines_pipeline_config import (
    historySource,
    pivotSource
)


class sgpHistory(historySource):

    data_source = Source.SGP
    etl_stage = Stage.HISTORY
    # DB for backfill is the standard data lake
    input_db_backfill = 'data_lake_private_prod'
    # Use batch version for daily delta
    input_db = 'dl_private_batch_prod'
    input_db_choose = '{{ dag.sgp_choose_input_db(ds) }}'
    backfill_end_date = SGP_BACKFILL_END_DATES[TC_ENV.lower()]['history']

    pipeline_data = {
        'atoc': 'atocinventoryproduct_travelproduct_fares_farelegs',
        'eu': 'euinventoryproduct_travelproduct_fares_farelegs',
        'insurance_created': 'insuranceproductcreated',
        'insurance_issued': 'insuranceproductissued',
        'national_express_product_leg': 'nationalexpressproduct_leg',
        'national_express_product_offers_actual': 'nationalexpressproduct_offers_actual',  # noqa: E501
        'national_express_product_pnrs_issued': 'nationalexpressproduct_pnrs_issued',  # noqa: E501
        'order_invoices_products': 'order_invoices_products',
        'customer_origin_classified': 'customeroriginclassified',
        'payment_created': 'paymentcreated',
        'payment_details_created': 'paymentdetailscreated',
        'order_invoices_payments': 'order_invoices_payments',
        'atoc_railcard_inventory_product': 'AtocRailcardInventoryProduct',
        'compensation_actions_product': 'compensationactionscomplete_product',
        'atoc_travel_product_fares': 'atocinventoryproduct_travelproduct_fares',  # noqa: E501
        'atoc_inventory_product_fares': 'atocinventoryproduct_inventoryproduct_fares',  # noqa: E501
        'order_invoices_delivery_fees': 'order_invoices_deliveryfees',
        'order_invoices_booking_fee_breakdown_references': 'order_invoices_bookingfee_breakdown_references',  # noqa: E501
        'atoc_change_operation': 'atocinventorychangeoperation',
        'eu_change_operation': 'euinventorychangeoperation',
        'rebook_refund_context': 'rebookrefundcontext',
        'atoc_inventory_product_legs': 'atocinventoryproduct_inventoryproduct_legs',  # noqa: E501
        'eu_reservation_space_allocations': 'euinventoryproduct_travelproduct_fares_farelegs_reservation_spaceallocations',  # noqa: E501
        'atoc_reservation_space_allocations': 'atocinventoryproduct_travelproduct_fares_farelegs_reservation_spaceallocations',  # noqa: E501
        'order_invoices_promos': 'order_invoices_promos',
        'eu_railcard_inventory_product': 'eurailcardinventoryproduct'
    }

    change_op_tables = \
        {'atocinventorychangeoperation': {
            'join_cols': [
                "split_part(c.change_operation_target_products, ',', 1)",
                "p.product_id"
            ],
            'join_table': 'atocinventoryproduct_genericproduct'},
         'euinventorychangeoperation': {
             'join_cols': [
                 "split_part(c.change_operation_target_products, ',', 1)",
                 "p.travel_product_id"],
             'join_table': 'euinventoryproduct_product'},
         'rebookrefundcontext': {
             'join_cols': [
                 "split_part(c.target_product_references_value, 'products/', 2)",  # noqa: E501
                 "p.product_id"],
             'join_table': 'atocinventoryproduct_genericproduct'}}

    tables_with_offset = {
        'atoc_change_operation': -1,
        'eu_change_operation': -1,
        'rebook_refund_context': -1,
        'compensation_actions_product': -1,
        'customer_origin_classified': -1,
    }

    from_expr_dict = {}
    input_expression = ''

    @staticmethod
    def choose_input_db(ds):
        exec_datetime = datetime.strptime(ds, '%Y-%m-%d')
        if (datetime.today() - exec_datetime).days > 20 \
           or ds == sgpHistory.backfill_end_date:
            return sgpHistory.input_db_backfill
        return sgpHistory.input_db

    @staticmethod
    def pipelines():
        return list(sgpHistory.pipeline_data.keys())

    @staticmethod
    def glue_sensor_table_name(pipeline):
        return f'hist_{sgpHistory.data_source}_{pipeline}'

    @staticmethod
    def input_counter_sql(pipeline):
        input_table = sgpHistory.pipeline_data[pipeline]
        min_date_offset = sgpHistory.tables_with_offset.get(pipeline,0)

        if input_table in list(sgpHistory.change_op_tables.keys()):
            select_stmt = """
    select count(distinct c.unique_id)
      from {{ dag.sgp_choose_input_db(ds) }}.%(src_table)s c
      join %(input_db)s.%(join_table)s p
        on %(join_col_p)s = %(join_col_c)s
       and p.year_month_day = date_format(c.event_time, '%%Y-%%m-%%d') """ \
           % {"input_db": sgpHistory.input_db_backfill,
              "src_table": input_table,
              "join_table":
                  sgpHistory.change_op_tables[input_table]['join_table'],
              "join_col_c":
                  sgpHistory.change_op_tables[input_table]['join_cols'][0],
              "join_col_p":
                  sgpHistory.change_op_tables[input_table]['join_cols'][1]}
        else:
            select_stmt = """
select count(*)
  from %(input_db)s.%(src_table)s c """ \
                  % {"src_table": input_table,
                     "input_db": sgpHistory.input_db_choose}

        where_clause = """
 where c.year_month_day BETWEEN CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                                 THEN '%(backfill_min_date)s'
                                 ELSE cast(date_add('day', %(min_date_offset)i ,date'{{ ds }}') as varchar)
                             END
                         AND '{{ ds }}'""" \
                 % {"backfill_end_date": sgpHistory.backfill_end_date,
                    "backfill_min_date": BACKFILL_MIN_DATE,
                    "min_date_offset": min_date_offset}

        return select_stmt + where_clause

    @staticmethod
    def output_counter_sql(pipeline):
        input_table = sgpHistory.pipeline_data[pipeline]
        if input_table in list(sgpHistory.change_op_tables.keys()):
            select_stmt = 'select count(distinct unique_id)'
        else:
            select_stmt = 'select count(*)'

        from_stmt = f' from hist_{sgpHistory.data_source}_{pipeline}' + '\n' \
                    + "where execution_date = '{{ ds }}'"

        return select_stmt + '\n' + from_stmt

    @staticmethod
    def equality_tolerance(pipeline):
        if pipeline in ('customer_origin_classified',
                        'atoc_change_operation',
                        'eu_change_operation'):
            return 0.25

        return 0.0


class sgpPivot(pivotSource):
    data_source = Source.SGP
    etl_stage = Stage.PIVOT
    backfill_end_date = SGP_BACKFILL_END_DATES[TC_ENV.lower()]['pivot']

    input_expression = """
(( execution_date BETWEEN '%s'
                      AND '{{ run_end_date_str }}'
   AND '%s' >= '{{ run_end_date_str }}' )
    or ( execution_date = '{{ run_end_date_str }}'
         and '%s' < '{{ run_end_date_str }}' ))""" % (
        BACKFILL_MIN_DATE, backfill_end_date, backfill_end_date)

    pipeline_data = {
        'atoc': {
            'source_tables': ['hist_sgp_atoc'],
            'source_partitions': [
                'product_travel_product_id',
                'travel_product_fares_fare_legs_id']},
            'eu': {
                'source_tables': ['hist_sgp_eu'],
                'source_partitions': [
                    'euinvprod_travel_product_id',
                    'travel_product_fares_fare_legs_id']},
            'insurance': {
                'source_tables': [
                    'hist_sgp_insurance_created',
                    'hist_sgp_insurance_issued'],
                'source_partitions': [
                    'insurance_product_created_id']},
            'national_express_product_leg': {
                'source_tables': [
                    'hist_sgp_national_express_product_leg'],
                'source_partitions': [
                    'product_id',
                    'leg_id']},
            'national_express_product_offers_actual': {
                'source_tables': [
                    'hist_sgp_national_express_product_offers_actual'],
                'source_partitions': [
                    'product_id',
                    'offer_actual_id',
                    'leg_id']},
            'national_express_product_pnrs_issued': {
                'source_tables': [
                    'hist_sgp_national_express_product_pnrs_issued'],
                'source_partitions': ['product_id']},
            'orderevent': {
                'source_tables': [
                    'hist_sgp_order_invoices_products'],
                'source_partitions': [
                    'orderevent_id',
                    'invoices_id',
                    'invoices_products_product_id']},
            'atoc_change_operation': {
                'source_tables': ['hist_sgp_atoc_change_operation'],
                'source_partitions': ['change_operation_id']},
            'eu_change_operation': {
                'source_tables': ['hist_sgp_eu_change_operation'],
                'source_partitions': [
                    'change_operation_id',
                    'change_operation_originating_product',
                    'target_product_id']},
            'rebook_refund_context': {
                'source_tables': ['hist_sgp_rebook_refund_context'],
                'source_partitions': ['id']},
            'customer_origin_classified': {
                'source_tables': [
                    'hist_sgp_customer_origin_classified'],
                'source_partitions': ['order_id']},
            'payment_created': {
                'source_tables': ['hist_sgp_payment_created'],
                'source_partitions': ['id']},
            'payment_details_created': {
                'source_tables': ['hist_sgp_payment_details_created'],
                'source_partitions': ['id']},
            'order_invoices_payments': {
                'source_tables': ['hist_sgp_order_invoices_payments'],
                'source_partitions': [
                    'orderevent_id',
                    'invoices_id']},
            'atoc_railcard_inventory_product': {
                'source_tables': [
                    'hist_sgp_atoc_railcard_inventory_product'],
                'source_partitions': ['discount_card_product_id']},
            'compensation_actions_product': {
                'source_tables': [
                    'hist_sgp_compensation_actions_product'],
                'source_partitions': [
                    'id',
                    'orderid',
                    'product_item_uri']},
            'atoc_travel_product_fares': {
                'source_tables': [
                    'hist_sgp_atoc_travel_product_fares'],
                'source_partitions': [
                    'travelproduct_id',
                    'travelproduct_fares_id']},
            'atoc_travel_product_fares_fare_passengers': {
                'source_tables': [
                    'hist_sgp_atoc_travel_product_fares_farepassengers_passengers'],
                'source_partitions': [
                    'travelproduct_id',
                    'travelproduct_fares_id']},
            'atoc_inventory_product_fares': {
                'source_tables': [
                    'hist_sgp_atoc_inventory_product_fares'],
                'source_partitions': [
                    'product_travel_product_id',
                    'inventory_product_fares_id']},
            'atoc_inventory_product_legs': {
                'source_tables': [
                    'hist_sgp_atoc_inventory_product_legs'],
                'source_partitions': [
                    'product_inventory_product_id',
                    'product_inventory_product_leg_id']},
            'order_invoices_delivery_fees': {
                'source_tables': ['hist_sgp_order_invoices_delivery_fees'],
                'source_partitions': [
                    'orderevent_id',
                    'invoices_id',
                    'invoices_delivery_fees_option',
                    'invoices_delivery_fees_product_id']},
            'order_invoices_booking_fee_breakdown_references': {
                'source_tables': [
                    'hist_sgp_order_invoices_booking_fee_breakdown_references'],  # noqa: E501
                'source_partitions': [
                    'orderevent_id',
                    'invoices_id',
                    'CAST(invoices_booking_fee_breakdown_position AS VARCHAR)',
                    'invoices_booking_fee_breakdown_references_id']},
            'order_invoices_promos': {
                'source_tables': ['hist_sgp_order_invoices_promos'],
                'source_partitions': [
                    'id',
                    'invoices_id',
                    'promocodeuris',
                    'invoices_promos_productid']},
            'promotions': {
                'source_tables': ['data_lake_private_prod.promocoderedeemed_discountitems'],  # noqa: E501
                'source_partitions': [
                    'promocodeid',
                    'discountitems_invoiceid',
                    'discountitems_productid']},
            'eu_reservation_space_allocations': {
                'source_tables': ['hist_sgp_eu_reservation_space_allocations'],
                'source_partitions': [
                    'travelproduct_id',
                    'travelproduct_fares_farelegs_reservation_spaceallocations_id']},  # noqa: E501
        'atoc_reservation_space_allocations': {
            'source_tables': ['hist_sgp_atoc_reservation_space_allocations'],
            'source_partitions': [
                'travelproduct_id',
                'travelproduct_fares_farelegs_reservation_spaceallocations_id']},
        'eu_railcard_inventory_product': {
            'source_tables': [
                'hist_sgp_eu_railcard_inventory_product'],
            'source_partitions': ['discountcardproduct_id']}
        }

    @staticmethod
    def pipelines():
        def filter_if_skip(pipeline):
            if sgpPivot.pipeline_data[pipeline].get('skip_count_checks'):
                return False
            else:
                return True

        return filter(filter_if_skip, list(sgpPivot.pipeline_data.keys()))

    @staticmethod
    def glue_sensor_table_name(pipeline):
        return f'pivoted_{sgpPivot.data_source}_{pipeline}'

    @staticmethod
    def input_counter_sql(pipeline):

        input_tables = sgpPivot.pipeline_data[pipeline]['source_tables']

        count_expression \
            = "COUNT(DISTINCT CONCAT({}, ''))" \
              .format(', '.join(sgpPivot.pipeline_data[pipeline]
                                .get('source_partitions')))

        if pipeline == 'national_express_product_offers_actual':
            input_table = \
                "(SELECT * " \
                "FROM hist_sgp_national_express_product_offers_actual " \
                "CROSS JOIN UNNEST(SPLIT(offer_actual_rule_applies_to_leg_ids,',')) AS t(leg_id))"  # noqa: E501
        elif pipeline == 'eu_change_operation':
            input_table = \
                "(SELECT * " \
                "FROM hist_sgp_eu_change_operation " \
                "CROSS JOIN UNNEST(SPLIT(change_operation_target_products,',')) AS t(target_product_id))"  # noqa: E501
        elif pipeline == 'order_invoices_delivery_fees':
            input_table = \
                "(SELECT * " \
                "FROM hist_sgp_order_invoices_delivery_fees " \
                "CROSS JOIN UNNEST(SPLIT(invoices_delivery_fees_product_ids,',')) AS t(invoices_delivery_fees_product_id))"
        elif pipeline in ('atoc_reservation_space_allocations','eu_reservation_space_allocations'):
            input_table = input_tables[0]
            input_table = \
f"""            (select travelproduct_id
                       ,travelproduct_fares_farelegs_reservation_spaceallocations_id
                       ,header_eventname
                       ,create_date_ymd
                       ,execution_date
                       ,FIRST_VALUE(header_eventname) over ( PARTITION BY travelproduct_id
                                          ORDER BY CASE header_eventname
                                                      WHEN 'ProductCreated'           THEN 1
                                                      WHEN 'ProductReserved'          THEN 2
                                                      WHEN 'ProductExpiryChanged'     THEN 3
                                                      WHEN 'ProductLocked'            THEN 4
                                                      WHEN 'ProductIssuing'           THEN 5
                                                      WHEN 'ProductIssued'            THEN 6
                                                      WHEN 'ProductSuperseded'        THEN 7
                                                      WHEN 'ProductVoiding'           THEN 8
                                                      WHEN 'ProductVoided'            THEN 9
                                                      ELSE 10
                                                   END DESC
                                                  ,event_time DESC) as product_current_event_name
                   from {input_table})"""
        else:
            input_table = input_tables[0]

        if pipeline in ['order_invoices_delivery_fees',
                        'order_invoices_booking_fee_breakdown_references',
                        'order_invoices_promos']:
            # uses generic_pipeline, uses range from min history partitions
            where_operator = ">="
            aggregate_ymd = "MIN(create_date_ymd)"
        else:
            # uses sgp_generic_pipeline which uses specifc affected history partitions.
            where_operator = "IN"
            aggregate_ymd = "DISTINCT create_date_ymd"

        where_clause = f"create_date_ymd {where_operator} ("
        where_clause += " UNION ".join([f"SELECT {aggregate_ymd} "
                                        "  FROM %(table)s "
                                        " WHERE " % {'table': table}
                                        + sgpPivot.input_expression
                                        for table in input_tables])
        where_clause += ") AND execution_date <= '{{ run_end_date_str }}'"

        if pipeline in ['promotions']:
            where_clause = "year_month_day <= '{{ run_end_date_str }}'"
        elif pipeline in ('atoc_reservation_space_allocations','eu_reservation_space_allocations'):
            where_clause += " and header_eventname = product_current_event_name"


        return """
        select %(count_expression)s
          from %(input_table)s
         where %(where_clause)s """ \
             % {"count_expression": count_expression,
                "input_table": input_table,
                "where_clause": where_clause}

    @staticmethod
    def output_counter_sql(pipeline):

        if pipeline == 'eu':
            count_expression \
                = "COUNT(DISTINCT CONCAT({}, ''))" \
                  .format(', '.join(sgpPivot.pipeline_data[pipeline]
                                    .get('source_partitions')))
        else:
            count_expression = 'COUNT(*)'

        return """
        select %(count_expression)s
          from %(output_table)s
         where execution_date='{{ ds }}'""" \
             % {"count_expression": count_expression,
                "output_table": f'pivoted_{sgpPivot.data_source}_{pipeline}'}
