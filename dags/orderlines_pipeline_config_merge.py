from ..orderlines_common import (
    ORDERLINES_PIVOT_DBS,
    ORDERLINES_MERGE_DBS,
    ORACLE_MIRROR_DBS,
    TC_ENV,
    BACKFILL_MIN_DATE,
    _25KV_BACKFILL_END_DATES,
    BEBOC_BACKFILL_END_DATES,
    FLIXBUS_BACKFILL_END_DATES,
    SEASONS_BACKFILL_END_DATES,
    SGP_BACKFILL_END_DATES,
    TRACS_BACKFILL_END_DATES,
    TTL_BACKFILL_END_DATES,
    Source
)

from .merge_source import MergeSource
from .pipeline import Pipeline

ORDERLINES_25KV_MERGE_OUTPUT_SQL = """
    select count(*) as query_generic_count 
    from %(output_db)s.%(output_table)s 
    where execution_date = '{{ run_end_date_str }}' 
        and source_system = '25KV' 
        and product_filter = 1

"""

ORDERLINES_FLIXBUS_MERGE_OUTPUT_SQL = """
    select count(product_id) as query_generic_count 
    from %(output_db)s.%(output_table)s 
    where product_filter = 1
        and source_system = 'FLIXBUS'
        and execution_date = '{{ ds }}'
"""

ORDERLINES_TRACS_MERGE_OUTPUT_SQL = """
select count(1) as query_generic_count from (
SELECT product_id
  FROM %(output_db)s.%(output_table)s
 WHERE execution_date = '{{ run_end_date_str }}'
   AND source_system = 'TRACS'
   AND product_filter =1
        )
"""

ORDERLINES_TRACS_RESERVATIONS_OUTPUT_SQL = """
select count(1) as query_generic_count from (
SELECT reservation_space_allocation_id
  FROM %(output_db)s.%(output_table)s
 WHERE execution_date = '{{ run_end_date_str }}'
   AND source_system = 'TRACS'
        )
"""

ORDERLINES_TRACS_FEES_OUTPUT_SQL = """
select count(*) as query_generic_count
from %(output_db)s.order_line_fee_details
where fee_filter = 1
  and source_system = 'TRACS'
  and ('{{ ds }}' = '%(backfill_end_date)s' 
       or execution_date = '{{ run_end_date_str }}'
       )
"""

ORDERLINES_BEBOC_MERGE_OUTPUT_SQL = """
select count(*) as query_generic_count
from %(output_db)s.order_line_fare_legs
where execution_date = '{{ run_end_date_str }}'
    and source_system = 'BEBOC'
    and product_filter = 1
"""

ORDERLINES_SEASONS_MERGE_OUTPUT_SQL = """
select count(1) as query_generic_count from (
SELECT product_id
  FROM %(output_db)s.order_line_fare_legs
 WHERE execution_date = '{{ run_end_date_str }}'
   AND source_system = 'SEASONS'
   AND product_filter =1
        )
"""

ORDERLINES_SEASONS_FEES_OUTPUT_SQL = """
select count(*) as query_generic_count
from %(output_db)s.order_line_fee_details
where fee_filter = 1
  and source_system = 'SEASONS'
  and ('{{ ds }}' = '%(backfill_end_date)s' 
       or execution_date = '{{ run_end_date_str }}'
       )
"""

ORDERLINES_SGP_MERGE_OUTPUT_SQL = """
select count(*) as query_generic_count
from %(output_db)s.order_line_fare_legs
where product_filter = 1
   and source_system = 'SGP'
   and ('{{ ds }}' = '%(backfill_end_date)s'
       or (execution_date = '{{ run_end_date_str }}'
           and product_created_date >= date'{{ run_start_date_str }}')
       )
"""

ORDERLINES_SGP_FEES_OUTPUT_SQL = """
select count(*) as query_generic_count
from %(output_db)s.order_line_fee_details
where fee_filter = 1
  and source_system = 'SGP'
  and ('{{ ds }}' = '%(backfill_end_date)s'
       or (execution_date = '{{ run_end_date_str }}'
           and fee_order_create_date >= date'{{ run_start_date_str }}')
       )
"""

ORDERLINES_SGP_RESERVATIONS_OUTPUT_SQL = """
select count(*) as query_generic_count
from %(output_db)s.order_line_fare_leg_reservations
where reservation_filter in (0, 1)
  and source_system = 'SGP'
   and ('{{ ds }}' = '%(backfill_end_date)s'
       or (execution_date = '{{ run_end_date_str }}'
           and product_created_date >= date'{{ run_start_date_str }}')
       )
"""

ORDERLINES_TTL_MERGE_OUTPUT_SQL = """
select count(order_id) as query_generic_count
from %(output_db)s.order_line_fare_legs
WHERE execution_date ='{{ ds }}' AND source_system = 'TTL'"""

ORDERLINES_25KV_MERGE_INPUT_SQL = """
select count(*) as query_generic_count from  (
WITH
    delta_ids AS (
    SELECT COALESCE(min(CASE WHEN invoice_payable_type = 'PnrHeader'
                                THEN invoice_payable_id END),0)                                 AS min_pnr_header_id_truncated
         , COALESCE(max(CASE WHEN invoice_payable_type = 'PnrHeader'
                                THEN invoice_payable_id_truncated END),0)                       AS max_pnr_header_id_truncated
         , COALESCE(min(CASE WHEN invoice_payable_type = 'Subscription'
                                THEN invoice_payable_id END),0)                                 AS min_subs_id_truncated
         , COALESCE(max(CASE WHEN invoice_payable_type = 'Subscription'
                                THEN invoice_payable_id_truncated END),0)                       AS max_subs_id_truncated
         , COALESCE(min(CASE WHEN invoice_payable_type = 'AfterSalesIntent'
                                THEN invoice_payable_id END),0)                                 AS min_afi_id_truncated
         , COALESCE(max(CASE WHEN invoice_payable_type = 'AfterSalesIntent'
                                THEN invoice_payable_id_truncated END),0)                       AS max_afi_id_truncated
         , COALESCE(min(CAST(CAST(order_date AS DATE) AS VARCHAR)),'{{ run_end_date_str }}')    AS min_date
    FROM %(input_db)s.int_25kv_order_invoice_lines
    WHERE
        execution_date = '{{ run_end_date_str }}'
    AND invoice_payable_type IN ('PnrHeader', 'Subscription', 'AfterSalesIntent'))

   ,min_dates AS (
    SELECT
        COALESCE(min(min_date),'{{ run_end_date_str }}') AS min_date
    FROM (
            SELECT min(CAST(CAST(creation_date AS DATE) AS VARCHAR)) AS min_date
            FROM %(input_db)s.int_25kv_pnr_product_fare_legs
            WHERE execution_date = '{{ run_end_date_str }}'

            UNION ALL

            SELECT MIN (CAST(CAST(creation_date AS DATE) AS VARCHAR)) AS min_date
            FROM %(input_db)s.int_25kv_pnr_product_fare_legs
            WHERE pnr_header_id_truncated BETWEEN
                (select min_pnr_header_id_truncated from delta_ids)
            AND (select min_pnr_header_id_truncated from delta_ids)

            UNION ALL

            SELECT MIN (CAST(CAST(creation_date AS DATE) AS VARCHAR)) AS min_date
            FROM %(input_db)s.pivoted_25kv_subscriptions
            WHERE execution_date = '{{ run_end_date_str }}'

            UNION ALL

            SELECT MIN (CAST(CAST(creation_date AS DATE) AS VARCHAR)) AS min_date
            FROM %(input_db)s.pivoted_25kv_subscriptions
            WHERE id_truncated BETWEEN
                (select min_subs_id_truncated from delta_ids)
            AND (select max_subs_id_truncated from delta_ids)

            UNION ALL

            SELECT MIN (CAST(CAST(created_at AS DATE) AS VARCHAR)) AS min_date
            FROM %(input_db)s.pivoted_25kv_after_sales_intents
            WHERE execution_date = '{{ run_end_date_str }}'
              and type = 'ExchangeIntent'
              and payable_type = 'PnrHeader'

            UNION ALL

            SELECT MIN (CAST(CAST(created_at AS DATE) AS VARCHAR)) AS min_date
            FROM %(input_db)s.pivoted_25kv_after_sales_intents
            WHERE id_truncated BETWEEN
                (select min_afi_id_truncated from delta_ids)
            AND (select max_afi_id_truncated from delta_ids)
            and type = 'ExchangeIntent'
            and payable_type = 'PnrHeader'

            UNION ALL

            SELECT min_date
            FROM delta_ids
         ) ilv)

   , id_ranges AS (
        SELECT COALESCE(MIN(ilv.min_pnr_header_id_truncated), 0)                            AS min_pnr_header_id_truncated
             , COALESCE(MAX(ilv.max_pnr_header_id_truncated), 0)                            AS max_pnr_header_id_truncated
             , COALESCE(MIN(ilv.min_subs_id_truncated), 0)                                  AS min_subs_id_truncated
             , COALESCE(MAX(ilv.max_subs_id_truncated), 0)                                  AS max_subs_id_truncated
             , COALESCE(MIN(ilv.min_invoice_id_truncated), 0)                               AS min_invoice_id_truncated
             , COALESCE(MAX(ilv.max_invoice_id_truncated), 0)                               AS max_invoice_id_truncated
             , COALESCE(MIN(ilv.min_exch_pnr_header_id_truncated), 0)                       AS min_exch_pnr_header_id_truncated
             , COALESCE(MAX(ilv.max_exch_pnr_header_id_truncated), 0)                       AS max_exch_pnr_header_id_truncated
             , COALESCE(MIN(ilv.min_asi_id_truncated), 0)                                   AS min_asi_id_truncated
             , COALESCE(MAX(ilv.max_asi_id_truncated), 0)                                   AS max_asi_id_truncated
        FROM
            (
                SELECT MIN(pnr_header_id_truncated)    AS min_pnr_header_id_truncated
                     , MAX(pnr_header_id_truncated)    AS max_pnr_header_id_truncated
                     , NULL                            AS min_subs_id_truncated
                     , NULL                            AS max_subs_id_truncated
                     , NULL                            AS min_invoice_id_truncated
                     , NULL                            AS max_invoice_id_truncated
                     , null                            as min_exch_pnr_header_id_truncated
                     , null                            as max_exch_pnr_header_id_truncated
                     , NULL                            as min_asi_id_truncated
                     , NULL                            as max_asi_id_truncated
                FROM %(input_db)s.int_25kv_pnr_product_fare_legs
                WHERE CAST(CAST(creation_date AS DATE) AS VARCHAR) BETWEEN (SELECT min_date FROM min_dates)
                AND '{{ run_end_date_str }}'
                AND pnr_header_id_truncated <> 0

                UNION ALL

                SELECT  NULL                         AS min_pnr_header_id_truncated
                      , NULL                         AS max_pnr_header_id_truncated
                      , MIN (id_truncated)           AS min_subs_id_truncated
                      , MAX (id_truncated)           AS max_subs_id_truncated
                      , NULL                         AS min_invoice_id_truncated
                      , NULL                         AS max_invoice_id_truncated
                      , null                         as min_exch_pnr_header_id_truncated
                      , null                         as max_exch_pnr_header_id_truncated
                      , NULL                         as min_asi_id_truncated
                      , NULL                         as max_asi_id_truncated
                FROM %(input_db)s.pivoted_25kv_subscriptions
                WHERE CAST(CAST(creation_date AS DATE) AS VARCHAR) BETWEEN (SELECT min_date FROM min_dates)
                AND '{{ run_end_date_str }}'

                UNION ALL

                SELECT  NULL                         AS min_pnr_header_id_truncated
                      , NULL                         AS max_pnr_header_id_truncated
                      , NULL                         AS min_subs_id_truncated
                      , NULL                         AS max_subs_id_truncated
                      , MIN (invoice_id_truncated)   AS min_invoice_id_truncated
                      , MAX (invoice_id_truncated)   AS max_invoice_id_truncated
                      , null                         as min_exch_pnr_header_id_truncated
                      , null                         as max_exch_pnr_header_id_truncated
                      , NULL                         as min_asi_id_truncated
                      , NULL                         as max_asi_id_truncated
                FROM %(input_db)s.int_25kv_order_invoice_lines
                WHERE CAST(CAST(order_created_date AS DATE) AS VARCHAR) BETWEEN (SELECT min_date FROM min_dates)
                AND '{{ run_end_date_str }}'

                UNION ALL

                SELECT  NULL                         AS min_pnr_header_id_truncated
                      , NULL                         AS max_pnr_header_id_truncated
                      , NULL                         AS min_subs_id_truncated
                      , NULL                         AS max_subs_id_truncated
                      , NULL                         AS min_invoice_id_truncated
                      , NULL                         AS max_invoice_id_truncated
                      , MIN (payable_id)             as min_exch_pnr_header_id_truncated
                      , MAX (payable_id_truncated)   as max_exch_pnr_header_id_truncated
                      , MIN (id)                     as min_asi_id_truncated
                      , MAX (id_truncated)           as max_asi_id_truncated
                FROM %(input_db)s.pivoted_25kv_after_sales_intents
                WHERE CAST(CAST(created_at AS DATE) AS VARCHAR) BETWEEN (SELECT min_date FROM min_dates)
                AND '{{ run_end_date_str }}'
                and type = 'ExchangeIntent'
                and payable_type = 'PnrHeader'
            ) ilv)

SELECT DISTINCT id
FROM %(input_db)s.int_25kv_pnr_product_fare_legs p
LEFT JOIN %(input_db)s.int_25kv_order_invoice_lines o
    ON o.invoice_payable_id = p.header_id AND o.invoice_payable_type = 'PnrHeader'
    AND o.invoice_id_truncated >= (SELECT min_invoice_id_truncated FROM id_ranges)
    AND o.invoice_id_truncated <= (SELECT max_invoice_id_truncated FROM id_ranges)
    AND o.invoice_payable_successful = True
WHERE
    p.pnr_header_id_truncated >=
        CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' THEN 1
        ELSE (SELECT min_pnr_header_id_truncated FROM id_ranges) END
  AND p.pnr_header_id_truncated <= (SELECT max_pnr_header_id_truncated FROM id_ranges)
  AND p.sale_pnr_header_order = 1
  AND CAST(CAST(COALESCE(o.order_created_date, p.creation_date) AS DATE) AS VARCHAR) >=
        CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' THEN '1900-01-01'
        ELSE (SELECT GREATEST(min_date, CAST( date_add('Day' , -365,DATE'{{ ds }}') AS VARCHAR)) min_date FROM min_dates) END

UNION ALL

SELECT DISTINCT id
FROM %(input_db)s.pivoted_25kv_subscriptions p
LEFT JOIN %(input_db)s.int_25kv_order_invoice_lines o
    ON o.invoice_payable_id = p.id AND o.invoice_payable_type = 'Subscription'
    AND o.invoice_id_truncated >= (SELECT min_invoice_id_truncated FROM id_ranges)
    AND o.invoice_id_truncated <= (SELECT max_invoice_id_truncated FROM id_ranges)
    AND o.invoice_payable_successful = True
WHERE
    p.id_truncated >=
        CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' THEN 0
        ELSE (SELECT min_subs_id_truncated FROM id_ranges) END
  AND p.id_truncated <= (SELECT max_subs_id_truncated FROM id_ranges)
  AND CAST(CAST(COALESCE(o.order_created_date, p.creation_date) AS DATE) AS VARCHAR) >=
        CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' THEN '1900-01-01'
        ELSE (SELECT GREATEST(min_date, CAST( date_add('Day' , -365,DATE'{{ ds }}') AS VARCHAR)) min_date FROM min_dates) END

UNION ALL

SELECT DISTINCT id
FROM %(input_db)s.pivoted_25kv_after_sales_intents p
LEFT JOIN %(input_db)s.int_25kv_order_invoice_lines o
    ON o.invoice_payable_id = p.id AND o.invoice_payable_type = 'AfterSalesIntent'
    AND o.invoice_id_truncated >= (SELECT min_invoice_id_truncated FROM id_ranges)
    AND o.invoice_id_truncated <= (SELECT max_invoice_id_truncated FROM id_ranges)
    AND o.invoice_payable_successful = True
WHERE
    p.id_truncated >=
        CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' THEN 0
        ELSE (SELECT min_asi_id_truncated FROM id_ranges) END
  AND p.id_truncated <= (SELECT max_asi_id_truncated FROM id_ranges)
  AND CAST(CAST(COALESCE(o.order_created_date, p.created_at) AS DATE) AS VARCHAR) >=
        CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' THEN '1900-01-01'
        ELSE (SELECT GREATEST(min_date, CAST( date_add('Day' , -365,DATE'{{ ds }}') AS VARCHAR))min_date FROM min_dates) END
  AND p.workflow_state = 'processed'
  AND p.type = 'ExchangeIntent'
)
"""

ORDERLINES_FLIXBUS_MERGE_INPUT_SQL = """
    select count(id) as query_generic_count from %(input_db)s.%(input_table)s 
    where state = 'APPROVED' 
        and  ( 
            execution_date = '{{ ds }}' 
                or 
            ( '%(backfill_end_date)s' = '{{ ds }}' and execution_date <= '{{ ds }}' )                                    
        )
"""

ORDERLINES_TRACS_MERGE_INPUT_SQL = """
select count(1) as query_generic_count from (
WITH tr_id_range AS
         (
             SELECT min(min_tr_id_partition) AS min_tr_id_partition
                  , max(max_tr_id_partition) AS max_tr_id_partition
             FROM (
                      SELECT min(tr_id_partition) AS min_tr_id_partition
                           , max(tr_id_partition) AS max_tr_id_partition
                      FROM %(input_db)s.pivoted_tracs_bookings
                      WHERE execution_date = '{{ run_end_date_str }}'
                      UNION ALL
                      SELECT min(tr_id_partition) AS min_tr_id_partition
                           , max(tr_id_partition) AS max_tr_id_partition
                      FROM %(input_db)s.pivoted_tracs_deliveries
                      WHERE execution_date = '{{ run_end_date_str }}'
                  )
         )
    ,tr_start_date as
           (SELECT CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' 
                      THEN DATE '1900-01-01'
                      ELSE DATE(MIN(start_date_time)) END AS min_start_date
                  ,CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' 
                      THEN 0
                      ELSE min(tr_id_partition) 
                     END AS min_tr_id_partition
              FROM %(input_db)s.pivoted_tracs_transactions t1
             WHERE t1.start_date_time >= (SELECT GREATEST(DATE(MIN(t2.start_date_time))
                                            , date_add('month',-14,date'{{ ds }}') )
                                            FROM %(input_db)s.pivoted_tracs_transactions t2
                                           WHERE t2.tr_id_partition = (SELECT min_tr_id_partition FROM tr_id_range))
            )
    ,tracs_bookings as
        (
         SELECT b.id, b.tr_id, b.del_id, b.pro_code, b.total_cost, b.cost_of_tickets
           FROM %(input_db)s.pivoted_tracs_bookings b
           JOIN %(input_db)s.pivoted_tracs_transactions t
             ON b.tr_id = t.id
            AND t.tr_id_partition >= (select min_tr_id_partition from tr_start_date) 
            AND t.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
            AND COALESCE(t.is_one_platform, 'N') = 'N'
            AND t.start_date_time >= (select min_start_date from tr_start_date)
          WHERE b.tr_id_partition >= (select min_tr_id_partition from tr_start_date) 
            AND b.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
            AND b.del_id is not null
        )
         SELECT product_id FROM
         (
            SELECT CAST(id AS VARCHAR) product_id FROM tracs_bookings

            UNION ALL

            SELECT DISTINCT
                    CASE
                        WHEN REPLACE(lower(st.description), ' ', '-') IN
                             ('paypal-fee', 'booking-fee', 'credit-card-leisure-fee')
                            THEN cast(s.tr_id AS varchar) || '_' || REPLACE(lower(st.description), ' ', '-')
                        ELSE cast(COALESCE(lb.bo_id, s.bo_id) AS varchar) || '_' ||
                             REPLACE(lower(st.description), ' ', '-')
                    END AS product_id
            FROM %(input_db)s.pivoted_tracs_supplements s
            JOIN %(input_db)s.pivoted_tracs_supplement_types st
                ON s.sut_code = st.code
                AND st.classification IN ('CV','INS','CP','OTHER','JAFEE','HOTEL')
            JOIN tracs_bookings b
                ON s.bo_id = b.id
               AND b.del_id is not null
            LEFT JOIN %(input_db)s.pivoted_tracs_linked_bookings lb
                ON s.bo_id = lb.linked_bo_id
                AND lb.tr_id_partition >= (select min_tr_id_partition from tr_start_date)
                AND lb.tr_id_partition <= (select max_tr_id_partition from tr_id_range)

            UNION ALL

            SELECT DISTINCT cast(COALESCE(lb.bo_id, b.id) AS VARCHAR)||'-promo-'||
                MAX(b.pro_code) OVER (PARTITION BY b.tr_id, COALESCE(lb.bo_id, b.id))
            FROM tracs_bookings b
            LEFT JOIN %(input_db)s.pivoted_tracs_linked_bookings lb
                ON b.id = lb.linked_bo_id
                AND lb.tr_id_partition >= (select min_tr_id_partition from tr_start_date)
                AND lb.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
            WHERE
                b.del_id is not null
            AND b.pro_code is not null and b.total_cost <> b.cost_of_tickets

            UNION ALL

            select DISTINCT CAST(d.id AS VARCHAR)||'deliveryFee'
            FROM %(input_db)s.pivoted_tracs_deliveries d
            JOIN tracs_bookings b
            ON d.id = b.del_id
            WHERE d.tr_id_partition >= (select min_tr_id_partition from tr_start_date)
            AND d.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
            AND d.cost > 0
         )      
         EXCEPT
         SELECT CAST(lb.linked_bo_id AS VARCHAR) product_id
           FROM %(input_db)s.pivoted_tracs_linked_bookings lb
          WHERE lb.tr_id_partition >= (select min_tr_id_partition from tr_start_date) 
            AND lb.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
         EXCEPT
         SELECT '689532449' -- BIDW-1904 This is due to the partial history data from Tracs
         -- (Tracs only has 3 years history data), it cannot be fixed.
        )
"""

ORDERLINES_TRACS_RESERVATIONS_INPUT_SQL = """
select count(1) as query_generic_count from (
WITH tr_id_range AS
         (
             SELECT min(min_tr_id_partition) AS min_tr_id_partition
                  , max(max_tr_id_partition) AS max_tr_id_partition
             FROM (
                      SELECT min(tr_id_partition) AS min_tr_id_partition
                           , max(tr_id_partition) AS max_tr_id_partition
                      FROM %(input_db)s.pivoted_tracs_bookings
                      WHERE execution_date = '{{ run_end_date_str }}'
                      UNION ALL
                      SELECT min(tr_id_partition) AS min_tr_id_partition
                           , max(tr_id_partition) AS max_tr_id_partition
                      FROM %(input_db)s.pivoted_tracs_deliveries
                      WHERE execution_date = '{{ run_end_date_str }}'
                  )
         )
    ,tr_start_date as
           (SELECT CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                      THEN DATE '1900-01-01'
                      ELSE DATE(MIN(start_date_time)) END AS min_start_date
                  ,CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                      THEN 0
                      ELSE min(tr_id_partition)
                     END AS min_tr_id_partition
              FROM %(input_db)s.pivoted_tracs_transactions t1
             WHERE t1.start_date_time >= (SELECT GREATEST(DATE(MIN(t2.start_date_time))
                                            , date_add('month',-14,date'{{ ds }}') )
                                            FROM %(input_db)s.pivoted_tracs_transactions t2
                                           WHERE t2.tr_id_partition = (SELECT min_tr_id_partition FROM tr_id_range))
            )
    ,tracs_journey_legs as
        (
         SELECT jl.id
           FROM %(input_db)s.pivoted_tracs_bookings b
           JOIN %(input_db)s.pivoted_tracs_transactions t
             ON b.tr_id = t.id
            AND t.tr_id_partition >= (select min_tr_id_partition from tr_start_date)
            AND t.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
            AND COALESCE(t.is_one_platform, 'N') = 'N'
            AND t.start_date_time >= (select min_start_date from tr_start_date)
          LEFT JOIN %(input_db)s.pivoted_tracs_journey_legs jl
                ON b.id = jl.bo_id
                AND jl.tr_id_partition >= (select min_tr_id_partition from tr_start_date)
                AND jl.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
          WHERE b.tr_id_partition >= (select min_tr_id_partition from tr_start_date)
            AND b.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
            AND b.del_id is not null
        )
            SELECT CAST(r.jl_id AS VARCHAR)||'-'||CAST(r.seq_num AS VARCHAR) as reservation_space_allocation_id
            FROM %(input_db)s.pivoted_tracs_reservations r
            JOIN tracs_journey_legs jl
                ON jl.id = r.jl_id
            WHERE r.tr_id_partition >= (select min_tr_id_partition from tr_start_date)
              AND r.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
        )
"""

ORDERLINES_TRACS_FEES_INPUT_SQL = """
select count(1) as query_generic_count from (
WITH tr_id_range AS
         (
             SELECT min(min_tr_id_partition) AS min_tr_id_partition
                  , max(max_tr_id_partition) AS max_tr_id_partition
             FROM (
                      SELECT min(tr_id_partition) AS min_tr_id_partition
                           , max(tr_id_partition) AS max_tr_id_partition
                      FROM %(input_db)s.pivoted_tracs_bookings
                      WHERE execution_date = '{{ run_end_date_str }}'
                      UNION ALL
                      SELECT min(tr_id_partition) AS min_tr_id_partition
                           , max(tr_id_partition) AS max_tr_id_partition
                      FROM %(input_db)s.pivoted_tracs_deliveries
                      WHERE execution_date = '{{ run_end_date_str }}'
                  )
         )
    ,tr_start_date as
           (SELECT CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' 
                      THEN DATE '1900-01-01'
                      ELSE DATE(MIN(start_date_time)) END AS min_start_date
                  ,CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' 
                      THEN 0
                      ELSE min(tr_id_partition) 
                     END AS min_tr_id_partition
              FROM %(input_db)s.pivoted_tracs_transactions t1
             WHERE t1.start_date_time >= (SELECT GREATEST(DATE(MIN(t2.start_date_time))
                                            , date_add('month',-14,date'{{ ds }}') )
                                            FROM %(input_db)s.pivoted_tracs_transactions t2
                                           WHERE t2.tr_id_partition = (SELECT min_tr_id_partition FROM tr_id_range))
            )
    ,tracs_bookings as
        (
         SELECT b.id, b.del_id, b.tr_id, b.pro_code, b.total_cost, b.cost_of_tickets
              , CAST(COALESCE(lb.bo_id, b.id) AS VARCHAR) product_id
           FROM %(input_db)s.pivoted_tracs_bookings b
           JOIN %(input_db)s.pivoted_tracs_transactions t
             ON b.tr_id = t.id
            AND t.tr_id_partition >= (select min_tr_id_partition from tr_start_date) 
            AND t.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
            AND COALESCE(t.is_one_platform, 'N') = 'N'
            AND t.start_date_time >= (select min_start_date from tr_start_date)
          LEFT JOIN %(input_db)s.pivoted_tracs_linked_bookings lb
                ON b.id = lb.linked_bo_id
                AND lb.tr_id_partition >= (select min_tr_id_partition from tr_start_date)
                AND lb.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
          WHERE b.tr_id_partition >= (select min_tr_id_partition from tr_start_date) 
            AND b.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
            AND b.del_id is not null
        )
            SELECT DISTINCT CAST(b.tr_id as VARCHAR)||s.sut_code product_id
            FROM %(input_db)s.pivoted_tracs_supplements s
            JOIN %(input_db)s.pivoted_tracs_supplement_types st
                ON s.sut_code = st.code
                AND st.classification IN ('CV','OTHER','JAFEE')
            JOIN tracs_bookings b
                ON s.bo_id = b.id
               AND b.del_id is not null

            UNION ALL

            SELECT DISTINCT cast(COALESCE(lb.bo_id, b.id) AS VARCHAR)||'-promo-'||
                MAX(b.pro_code) OVER (PARTITION BY b.tr_id, COALESCE(lb.bo_id, b.id))
            FROM tracs_bookings b
            LEFT JOIN %(input_db)s.pivoted_tracs_linked_bookings lb
                ON b.id = lb.linked_bo_id
                AND lb.tr_id_partition >= (select min_tr_id_partition from tr_start_date)
                AND lb.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
            WHERE
                b.del_id is not null
            AND b.pro_code is not null and b.total_cost <> b.cost_of_tickets

            UNION ALL

            select DISTINCT 'deliveryFee'||cast(d.id as varchar) product_id
            FROM %(input_db)s.pivoted_tracs_deliveries d
            JOIN tracs_bookings b
            ON d.id = b.del_id
            WHERE d.tr_id_partition >= (select min_tr_id_partition from tr_start_date)
            AND d.tr_id_partition <= (select max_tr_id_partition from tr_id_range)
        )
"""

ORDERLINES_BEBOC_MERGE_INPUT_SQL = """
select count(*) as query_generic_count from  (
WITH
    delta_ids AS (
    SELECT COALESCE(min(CASE WHEN invoice_payable_type = 'PnrHeader'
                                THEN invoice_payable_id END),0)                                 AS min_pnr_header_id_truncated
         , COALESCE(max(CASE WHEN invoice_payable_type = 'PnrHeader'
                                THEN invoice_payable_id_truncated END),0)                       AS max_pnr_header_id_truncated
         , COALESCE(min(CASE WHEN invoice_payable_type = 'Subscription'
                                THEN invoice_payable_id END),0)                                 AS min_subs_id_truncated
         , COALESCE(max(CASE WHEN invoice_payable_type = 'Subscription'
                                THEN invoice_payable_id_truncated END),0)                       AS max_subs_id_truncated
         , COALESCE(min(CASE WHEN invoice_payable_type = 'AfterSalesIntent'
                                THEN invoice_payable_id END),0)                                 AS min_afi_id_truncated
         , COALESCE(max(CASE WHEN invoice_payable_type = 'AfterSalesIntent'
                                THEN invoice_payable_id_truncated END),0)                       AS max_afi_id_truncated
         , COALESCE(min(CAST(CAST(order_date AS DATE) AS VARCHAR)),'{{ run_end_date_str }}')    AS min_date
    FROM %(input_db)s.int_beboc_order_invoice_lines
    WHERE
        execution_date = '{{ run_end_date_str }}'
    AND invoice_payable_type IN ('PnrHeader', 'Subscription', 'AfterSalesIntent'))

   ,min_dates AS (
    SELECT
        COALESCE(min(min_date),'{{ run_end_date_str }}') AS min_date
    FROM (
            SELECT min(CAST(CAST(creation_date AS DATE) AS VARCHAR)) AS min_date
            FROM %(input_db)s.int_beboc_pnr_product_fare_legs
            WHERE execution_date = '{{ run_end_date_str }}'

            UNION ALL

            SELECT MIN (CAST(CAST(creation_date AS DATE) AS VARCHAR)) AS min_date
            FROM %(input_db)s.int_beboc_pnr_product_fare_legs
            WHERE pnr_header_id_truncated BETWEEN
                (select min_pnr_header_id_truncated from delta_ids)
            AND (select min_pnr_header_id_truncated from delta_ids)

            UNION ALL

            SELECT MIN (CAST(CAST(created_at AS DATE) AS VARCHAR)) AS min_date
            FROM %(input_db)s.pivoted_beboc_after_sales_intents
            WHERE execution_date = '{{ run_end_date_str }}'
              and type = 'ExchangeIntent'
              and payable_type = 'PnrHeader'

            UNION ALL

            SELECT MIN (CAST(CAST(created_at AS DATE) AS VARCHAR)) AS min_date
            FROM %(input_db)s.pivoted_beboc_after_sales_intents
            WHERE id_truncated BETWEEN
                (select min_afi_id_truncated from delta_ids)
            AND (select max_afi_id_truncated from delta_ids)
            and type = 'ExchangeIntent'
            and payable_type = 'PnrHeader'

            UNION ALL

            SELECT min_date
            FROM delta_ids
         ) ilv)

   , id_ranges AS (
        SELECT COALESCE(MIN(ilv.min_pnr_header_id_truncated), 0)                            AS min_pnr_header_id_truncated
             , COALESCE(MAX(ilv.max_pnr_header_id_truncated), 0)                            AS max_pnr_header_id_truncated
             , COALESCE(MIN(ilv.min_subs_id_truncated), 0)                                  AS min_subs_id_truncated
             , COALESCE(MAX(ilv.max_subs_id_truncated), 0)                                  AS max_subs_id_truncated
             , COALESCE(MIN(ilv.min_invoice_id_truncated), 0)                               AS min_invoice_id_truncated
             , COALESCE(MAX(ilv.max_invoice_id_truncated), 0)                               AS max_invoice_id_truncated
             , COALESCE(MIN(ilv.min_exch_pnr_header_id_truncated), 0)                       AS min_exch_pnr_header_id_truncated
             , COALESCE(MAX(ilv.max_exch_pnr_header_id_truncated), 0)                       AS max_exch_pnr_header_id_truncated
             , COALESCE(MIN(ilv.min_asi_id_truncated), 0)                                   AS min_asi_id_truncated
             , COALESCE(MAX(ilv.max_asi_id_truncated), 0)                                   AS max_asi_id_truncated
        FROM
            (
                SELECT MIN(pnr_header_id_truncated)    AS min_pnr_header_id_truncated
                     , MAX(pnr_header_id_truncated)    AS max_pnr_header_id_truncated
                     , NULL                            AS min_subs_id_truncated
                     , NULL                            AS max_subs_id_truncated
                     , NULL                            AS min_invoice_id_truncated
                     , NULL                            AS max_invoice_id_truncated
                     , null                            as min_exch_pnr_header_id_truncated
                     , null                            as max_exch_pnr_header_id_truncated
                     , NULL                            as min_asi_id_truncated
                     , NULL                            as max_asi_id_truncated
                FROM %(input_db)s.int_beboc_pnr_product_fare_legs
                WHERE CAST(CAST(creation_date AS DATE) AS VARCHAR) BETWEEN (SELECT min_date FROM min_dates)
                AND '{{ run_end_date_str }}'
                AND pnr_header_id_truncated <> 0

                UNION ALL

                SELECT  NULL                         AS min_pnr_header_id_truncated
                      , NULL                         AS max_pnr_header_id_truncated
                      , NULL                         AS min_subs_id_truncated
                      , NULL                         AS max_subs_id_truncated
                      , MIN (invoice_id_truncated)   AS min_invoice_id_truncated
                      , MAX (invoice_id_truncated)   AS max_invoice_id_truncated
                      , null                         as min_exch_pnr_header_id_truncated
                      , null                         as max_exch_pnr_header_id_truncated
                      , NULL                         as min_asi_id_truncated
                      , NULL                         as max_asi_id_truncated
                FROM %(input_db)s.int_beboc_order_invoice_lines
                WHERE CAST(CAST(order_created_date AS DATE) AS VARCHAR) BETWEEN (SELECT min_date FROM min_dates)
                AND '{{ run_end_date_str }}'

                UNION ALL

                SELECT  NULL                         AS min_pnr_header_id_truncated
                      , NULL                         AS max_pnr_header_id_truncated
                      , NULL                         AS min_subs_id_truncated
                      , NULL                         AS max_subs_id_truncated
                      , NULL                         AS min_invoice_id_truncated
                      , NULL                         AS max_invoice_id_truncated
                      , MIN (payable_id)             as min_exch_pnr_header_id_truncated
                      , MAX (payable_id_truncated)   as max_exch_pnr_header_id_truncated
                      , MIN (id)                     as min_asi_id_truncated
                      , MAX (id_truncated)           as max_asi_id_truncated
                FROM %(input_db)s.pivoted_beboc_after_sales_intents
                WHERE CAST(CAST(created_at AS DATE) AS VARCHAR) BETWEEN (SELECT min_date FROM min_dates)
                AND '{{ run_end_date_str }}'
                and type = 'ExchangeIntent'
                and payable_type = 'PnrHeader'
            ) ilv)

SELECT DISTINCT id
FROM %(input_db)s.pivoted_beboc_after_sales_intents p
LEFT JOIN %(input_db)s.int_beboc_order_invoice_lines o
    ON o.invoice_payable_id = p.id AND o.invoice_payable_type = 'AfterSalesIntent'
    AND o.invoice_id_truncated >= (SELECT min_invoice_id_truncated FROM id_ranges)
    AND o.invoice_id_truncated <= (SELECT max_invoice_id_truncated FROM id_ranges)
    AND o.invoice_payable_successful = True
WHERE
    p.id_truncated >=
        CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' THEN 0
        ELSE (SELECT min_asi_id_truncated FROM id_ranges) END
  AND p.id_truncated <= (SELECT max_asi_id_truncated FROM id_ranges)
  AND CAST(CAST(COALESCE(o.order_created_date, p.created_at) AS DATE) AS VARCHAR) >=
        CASE WHEN '{{ ds }}' = '%(backfill_end_date)s' THEN '1900-01-01'
        ELSE (SELECT GREATEST(min_date, CAST( date_add('Day' , -365,DATE'{{ ds }}') AS VARCHAR))min_date FROM min_dates) END
  AND p.workflow_state = 'processed'
  AND p.type = 'ExchangeIntent'
) 
"""

ORDERLINES_SEASONS_FEES_INPUT_SQL = """
select sum(cnt) as query_generic_count from (
    WITH 
    tr_id_range AS
         (SELECT CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                      THEN 2250000000
                      ELSE min(min_tr_id_partition)
                  END AS min_tr_id_partition
                ,CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                      THEN 999999999999
                      ELSE MAX(max_tr_id_partition) 
                  END AS max_tr_id_partition
            FROM (SELECT MIN(b.tr_id_partition) AS min_tr_id_partition
                        ,MAX(b.tr_id_partition) AS max_tr_id_partition
                    FROM %(input_db)s.pivoted_tracs_season_bookings b
                   WHERE b.execution_date = '{{ run_end_date_str }}'
                  UNION ALL
                  SELECT MIN(d.tr_id_partition) AS min_tr_id_partition
                        ,MAX(d.tr_id_partition) AS max_tr_id_partition
                    FROM %(input_db)s.pivoted_tracs_deliveries d
                   WHERE d.execution_date = '{{ run_end_date_str }}'
                  UNION ALL
                  SELECT MIN(ft.tr_id_partition) as min_tr_id_partition
                        ,MAX(ft.tr_id_partition) as max_tr_id_partition
                    FROM %(input_db)s.pivoted_frt_tracs_season_transactions ft
                   WHERE ft.execution_date = '{{ run_end_date_str }}' 
                     AND COALESCE(ft.referencefield10,'x') <> 'HIDECLIENTBILLING'
                     AND ft.ordertype in ('NEW','REN','FRV','VDR','VDC','COJ','REP','DUP','EXC')
                  )
         )
   ,frt_drt_range AS 
         (SELECT CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                      THEN 22000
                      ELSE COALESCE(MIN(order_id_partition),0)
                  END AS min_order_id_partition
                ,CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                      THEN 999999999999
                      ELSE COALESCE(MAX(td.order_id_partition),0) 
                  END AS max_order_id_partition
            FROM %(input_db)s.pivoted_frt_direct_season_transactions td
           WHERE execution_date =  '{{ run_end_date_str }}')
   ,start_date AS 
         (SELECT CASE 
                    WHEN '{{ ds }}' = '%(backfill_end_date)s' THEN DATE'1900-01-01'
                    ELSE COALESCE(DATE(MIN(ilv.min_order_create_date)),DATE '{{ run_end_date_str }}')
                 END AS min_order_create_date
            FROM (SELECT DATE(MIN(t2.start_date_time)) min_order_create_date
                    FROM %(input_db)s.pivoted_tracs_transactions t2
                   WHERE t2.tr_id_partition = (SELECT min_tr_id_partition FROM tr_id_range)
                  UNION
                  SELECT MIN(frt.orderdate)
                    FROM %(input_db)s.pivoted_frt_direct_season_transactions frt
                   WHERE order_id_partition = (SELECT MIN(min_order_id_partition) FROM frt_drt_range)
                 ) ilv
         )
    ,frt_tracs AS
         (
            SELECT f.*,  row_number() OVER (PARTITION BY f.tr_id order by f.orderid) tr_id_filter
            FROM %(input_db)s.pivoted_frt_tracs_season_transactions f
            WHERE COALESCE(f.referencefield10,'x') <> 'HIDECLIENTBILLING'
	         AND f.ordertype in ('NEW', 'REN', 'FRV', 'VDR', 'VDC', 'COJ','REP','DUP','EXC')
             AND f.orderdate >= (SELECT min_order_create_date FROM start_date)
             AND f.orderdate <  DATE'{{ run_end_date_str }}' + INTERVAL '1' DAY
	         AND f.tr_id_partition <= (SELECT max_tr_id_partition FROM tr_id_range)
         )
    ,frt_bulk_delivery_day_tracs AS
         (
             SELECT f.orderid
                  , f.issuedate
                  , CAST(CASE
                             WHEN regexp_like(f.deliverymethod, '(?i)MONDAY') AND 1 < day_of_week (f.issuedate)
                                 THEN date_add('day',0 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)MONDAY') AND 1 = day_of_week (f.issuedate)
                                 THEN date_add('day',0 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)TUESDAY') AND 2 < day_of_week (f.issuedate)
                                 THEN date_add('day',1 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)TUESDAY') AND 2 >= day_of_week (f.issuedate)
                                 THEN date_add('day',1 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)WEDNESDAY') AND 3 < day_of_week (f.issuedate)
                                 THEN date_add('day',2 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)WEDNESDAY') AND 3 >= day_of_week (f.issuedate)
                                 THEN date_add('day',2 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)THURSDAY') AND 4 < day_of_week (f.issuedate)
                                 THEN date_add('day',3 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)THURSDAY') AND 4 >= day_of_week (f.issuedate)
                                 THEN date_add('day',3 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)FRIDAY') AND 5 < day_of_week (f.issuedate)
                                 THEN date_add('day',4 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)FRIDAY') AND 5 >= day_of_week (f.issuedate)
                                 THEN date_add('day',4 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)SATURDAY') AND 6 < day_of_week (f.issuedate)
                                 THEN date_add('day',5 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)SATURDAY') AND 6 >= day_of_week (f.issuedate)
                                 THEN date_add('day',5 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)SUNDAY') AND 7 <> day_of_week (f.issuedate)
                                 THEN date_add('day',6 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)SUNDAY') AND 7 = day_of_week (f.issuedate)
                                 THEN date_add('day',6 , date_trunc('week',date_add('week',0 , f.issuedate)))
                 END AS DATE) bulk_delivery_date
             FROM %(input_db)s.pivoted_frt_tracs_season_transactions f
             WHERE regexp_like(f.deliverymethod, '(?i)BULK')
               AND f.ordertype IN ('NEW', 'REN', 'FRV', 'VDR', 'VDC', 'COJ','REP','DUP','EXC')
         )
    ,frt_detail_tracs as
         (
            select
                f.*
                ,CASE
                    WHEN f.deliverymethod = 'Bulk Delivery' AND f.orderid = FIRST_VALUE(f.orderid) OVER (PARTITION BY f.referencefieldclientaccountcode, CAST(f.issuedate AS DATE) ORDER BY f.orderid ASC) THEN f.deliveryprice
                    WHEN f.deliverymethod = 'Bulk Delivery' AND f.orderid <> FIRST_VALUE(f.orderid) OVER (PARTITION BY f.referencefieldclientaccountcode, CAST(f.issuedate AS DATE) ORDER BY f.orderid ASC) THEN CAST(0 AS DECIMAL(20, 2))
                    WHEN regexp_like(f.deliverymethod, '(?i)Bulk Delivery ') AND f.orderid = FIRST_VALUE(f.orderid) OVER (PARTITION BY f.referencefieldclientaccountcode, f.deliverymethod, fbdd.bulk_delivery_date ORDER BY f.orderid ASC) THEN f.deliveryprice
                    WHEN regexp_like(f.deliverymethod, '(?i)Bulk Delivery ') AND f.orderid <> FIRST_VALUE(f.orderid) OVER (PARTITION BY f.referencefieldclientaccountcode, f.deliverymethod, fbdd.bulk_delivery_date ORDER BY f.orderid ASC) THEN CAST(0 AS DECIMAL(20, 2))
                    ELSE f.deliveryprice
                  END as frt_delivery_price
            from
                frt_tracs f
                LEFT JOIN frt_bulk_delivery_day_tracs fbdd
                    ON f.orderid = fbdd.orderid
         )
         ,frt_direct AS
         (
            SELECT fd.*
            FROM %(input_db)s.pivoted_frt_direct_season_transactions fd
            WHERE fd.orderdate >= (SELECT min_order_create_date FROM start_date)
              AND fd.orderdate < DATE'{{ run_end_date_str }}' + INTERVAL '1' DAY
              AND COALESCE(fd.referencefield10,'x') <> 'HIDECLIENTBILLING'
              AND fd.ordertype in ('NEW','REN','FRV','VDR','VDC','COJ','REP','DUP','EXC')
         )
        ,frt_bulk_delivery_day_direct AS
         (
             SELECT f.orderid
                  , f.issuedate
                  , CAST(CASE
                             WHEN regexp_like(f.deliverymethod, '(?i)MONDAY') AND 1 < day_of_week (f.issuedate)
                                 THEN date_add('day',0 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)MONDAY') AND 1 = day_of_week (f.issuedate)
                                 THEN date_add('day',0 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)TUESDAY') AND 2 < day_of_week (f.issuedate)
                                 THEN date_add('day',1 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)TUESDAY') AND 2 >= day_of_week (f.issuedate)
                                 THEN date_add('day',1 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)WEDNESDAY') AND 3 < day_of_week (f.issuedate)
                                 THEN date_add('day',2 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)WEDNESDAY') AND 3 >= day_of_week (f.issuedate)
                                 THEN date_add('day',2 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)THURSDAY') AND 4 < day_of_week (f.issuedate)
                                 THEN date_add('day',3 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)THURSDAY') AND 4 >= day_of_week (f.issuedate)
                                 THEN date_add('day',3 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)FRIDAY') AND 5 < day_of_week (f.issuedate)
                                 THEN date_add('day',4 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)FRIDAY') AND 5 >= day_of_week (f.issuedate)
                                 THEN date_add('day',4 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)SATURDAY') AND 6 < day_of_week (f.issuedate)
                                 THEN date_add('day',5 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)SATURDAY') AND 6 >= day_of_week (f.issuedate)
                                 THEN date_add('day',5 , date_trunc('week',date_add('week',0 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)SUNDAY') AND 7 <> day_of_week (f.issuedate)
                                 THEN date_add('day',6 ,date_trunc('week', date_add('week',1 , f.issuedate)))
                             WHEN regexp_like(f.deliverymethod, '(?i)SUNDAY') AND 7 = day_of_week (f.issuedate)
                                 THEN date_add('day',6 , date_trunc('week',date_add('week',0 , f.issuedate)))
                 END AS DATE) bulk_delivery_date
             FROM %(input_db)s.pivoted_frt_direct_season_transactions f
             WHERE regexp_like(f.deliverymethod, '(?i)BULK')
               AND f.ordertype IN ('NEW', 'REN', 'FRV', 'VDR', 'VDC', 'COJ','REP','DUP','EXC')
         )
    ,frt_detail_direct as
         (
            select
                f.*
                ,CASE
                    WHEN f.deliverymethod = 'Bulk Delivery' AND f.orderid = FIRST_VALUE(f.orderid) OVER (PARTITION BY f.referencefieldclientaccountcode, CAST(f.issuedate AS DATE) ORDER BY f.orderid ASC) THEN f.deliveryprice
                    WHEN f.deliverymethod = 'Bulk Delivery' AND f.orderid <> FIRST_VALUE(f.orderid) OVER (PARTITION BY f.referencefieldclientaccountcode, CAST(f.issuedate AS DATE) ORDER BY f.orderid ASC) THEN CAST(0 AS DECIMAL(20, 2))
                    WHEN regexp_like(f.deliverymethod, '(?i)Bulk Delivery ') AND f.orderid = FIRST_VALUE(f.orderid) OVER (PARTITION BY f.referencefieldclientaccountcode, f.deliverymethod, fbdd.bulk_delivery_date ORDER BY f.orderid ASC) THEN f.deliveryprice
                    WHEN regexp_like(f.deliverymethod, '(?i)Bulk Delivery ') AND f.orderid <> FIRST_VALUE(f.orderid) OVER (PARTITION BY f.referencefieldclientaccountcode, f.deliverymethod, fbdd.bulk_delivery_date ORDER BY f.orderid ASC) THEN CAST(0 AS DECIMAL(20, 2))
                    ELSE f.deliveryprice
                  END as frt_delivery_price
            from
                frt_direct f
                LEFT JOIN frt_bulk_delivery_day_direct fbdd
                    ON f.orderid = fbdd.orderid
         )
    SELECT  CASE
                WHEN f.orderid IS NOT NULL AND (b.customer_cor_ref IS NOT NULL OR f.tr_id_filter > 1)
                    THEN CASE WHEN f.frt_delivery_price <> 0 THEN 1 ELSE 0 END
                ELSE CASE WHEN d.cost <> 0 THEN 1 ELSE 0 END
              END cnt
      FROM %(input_db)s.pivoted_tracs_season_bookings b
      JOIN %(input_db)s.pivoted_tracs_transactions t
        ON b.tr_id = t.id
       AND b.tr_id_partition = t.tr_id_partition
       AND t.start_date_time >= (SELECT min_order_create_date FROM start_date)
       AND t.start_date_time <  DATE'{{ run_end_date_str }}' + INTERVAL '1' DAY
       AND t.tr_id_partition <= (SELECT max_tr_id_partition FROM tr_id_range)
       AND COALESCE(t.is_one_platform, 'N') = 'N'
      JOIN %(input_db)s.pivoted_tracs_deliveries d
        ON b.del_id = d.id
       AND b.tr_id_partition = d.tr_id_partition
      LEFT JOIN frt_detail_tracs f
       ON f.tr_id = CAST(b.tr_id as varchar)
      AND f.ordertype in ('NEW','REN','FRV','VDR')
     WHERE b.del_id is not null
       AND b.renewal NOT IN ('D','C')
       AND ( b.customer_cor_ref is null 
             or 
             ( b.customer_cor_ref is not null and f.orderid is not null ) 
           )
    UNION ALL
    SELECT  CASE WHEN d.cost <> 0 THEN 1 ELSE 0 END
        +   CASE WHEN d.cost = 0 AND p.amount <> 0 THEN 1 ELSE 0 END
      FROM %(input_db)s.pivoted_tracs_season_bookings b
      JOIN %(input_db)s.pivoted_tracs_transactions t
        ON b.tr_id = t.id
       AND b.tr_id_partition = t.tr_id_partition
       AND t.start_date_time >= (SELECT min_order_create_date FROM start_date)
       AND t.start_date_time <  DATE'{{ run_end_date_str }}' + INTERVAL '1' DAY
       AND t.tr_id_partition <= (SELECT max_tr_id_partition FROM tr_id_range)
       AND COALESCE(t.is_one_platform, 'N') = 'N'
       AND b.renewal IN ('D')
      JOIN %(input_db)s.pivoted_tracs_deliveries d
        ON b.del_id = d.id
       AND b.tr_id_partition = d.tr_id_partition
       AND d.DM_CODE IN ('TOD','SMART')
       AND b.tr_id <> 2293300034
      JOIN %(input_db)s.pivoted_tracs_payments p
        ON d.tr_id = p.tr_id
       AND b.tr_id_partition = p.tr_id_partition
    UNION ALL 
    SELECT  CASE WHEN frt_delivery_price <> 0 THEN 1 ELSE 0 END
        +   CASE WHEN adminfee <> 0 THEN 1 ELSE 0 END
      FROM frt_detail_direct fd
    UNION ALL 
    SELECT  CASE WHEN frt_delivery_price <> 0 THEN 1 ELSE 0 END
        +   CASE WHEN adminfee <> 0 THEN 1 ELSE 0 END
      FROM frt_detail_tracs f
     WHERE f.ordertype in ('VDC','COJ','REP','DUP','EXC')
    UNION ALL
    SELECT  CASE WHEN booking_fee <> 0 THEN 1 ELSE 0 END 
        +   CASE WHEN del_fee <> 0 THEN 1 ELSE 0 END
        +   CASE WHEN admin_fee <> 0 THEN 1 ELSE 0 END
        +   CASE WHEN card_fee <> 0 THEN 1 ELSE 0 END
      FROM %(oracle_mirror_db)s.m_season_transactions mst
     WHERE (
        mst.SOURCE_SYSTEM = 'M_SEASON_CHARGES'
        OR (mst.SOURCE_SYSTEM = 'TCS_SEASON_BOOKINGS' AND (mst.CHANGEOVER_AMOUNT <> 0 OR mst.TICKET_COST = 0 ))
      )
       AND mst.booking_date >= (SELECT min_order_create_date FROM start_date)
       AND mst.booking_date <= DATE'{{ run_end_date_str }}'
       AND mst.tis_date >= DATE'2017-01-01'
       AND NOT REGEXP_LIKE(mst.ticket_number, 'MEC')
       AND NOT (mst.BOOKING_FEE + mst.DEL_FEE + mst.CARD_FEE = mst.CUSTOMER_AMOUNT AND mst.CUSTOMER_AMOUNT <> 0 AND mst.TICKET_COST = 0 AND mst.VOUCHER_AMOUNT = 0)
       AND mst.transaction_type = 'SALE'
    )
"""

ORDERLINES_SEASONS_MERGE_INPUT_SQL = """
select sum(query_generic_count) as query_generic_count from (
select count(1) as query_generic_count from (
    WITH 
    tr_id_range AS
         (SELECT CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                      THEN 2250000000
                      ELSE min(min_tr_id_partition)
                  END AS min_tr_id_partition
                ,CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                      THEN 999999999999
                      ELSE MAX(max_tr_id_partition) 
                  END AS max_tr_id_partition
            FROM (SELECT MIN(b.tr_id_partition) AS min_tr_id_partition
                        ,MAX(b.tr_id_partition) AS max_tr_id_partition
                    FROM %(input_db)s.pivoted_tracs_season_bookings b
                   WHERE b.execution_date = '{{ run_end_date_str }}'
                  UNION ALL
                  SELECT MIN(d.tr_id_partition) AS min_tr_id_partition
                        ,MAX(d.tr_id_partition) AS max_tr_id_partition
                    FROM %(input_db)s.pivoted_tracs_deliveries d
                   WHERE d.execution_date = '{{ run_end_date_str }}'
                  UNION ALL
                  SELECT MIN(ft.tr_id_partition) as min_tr_id_partition
                        ,MAX(ft.tr_id_partition) as max_tr_id_partition
                    FROM %(input_db)s.pivoted_frt_tracs_season_transactions ft
                   WHERE ft.execution_date = '{{ run_end_date_str }}' 
                     AND COALESCE(ft.referencefield10,'x') <> 'HIDECLIENTBILLING'
                     AND ft.ordertype in ('NEW','REN','FRV','VDR','VDC','COJ','REP','DUP','EXC')
                  )
         )
   ,frt_drt_range AS 
         (SELECT CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                      THEN 22000
                      ELSE COALESCE(MIN(order_id_partition),0)
                  END AS min_order_id_partition
                ,CASE WHEN '{{ ds }}' = '%(backfill_end_date)s'
                      THEN 999999999999
                      ELSE COALESCE(MAX(td.order_id_partition),0) 
                  END AS max_order_id_partition
            FROM %(input_db)s.pivoted_frt_direct_season_transactions td
           WHERE execution_date =  '{{ run_end_date_str }}')
   ,start_date AS 
         (SELECT CASE 
                    WHEN '{{ ds }}' = '%(backfill_end_date)s' THEN DATE'1900-01-01'
                    ELSE COALESCE(DATE(MIN(ilv.min_order_create_date)),DATE '{{ run_end_date_str }}')
                 END AS min_order_create_date
            FROM (SELECT DATE(MIN(t2.start_date_time)) min_order_create_date
                    FROM %(input_db)s.pivoted_tracs_transactions t2
                   WHERE t2.tr_id_partition = (SELECT min_tr_id_partition FROM tr_id_range)
                  UNION
                  SELECT MIN(frt.orderdate)
                    FROM %(input_db)s.pivoted_frt_direct_season_transactions frt
                   WHERE order_id_partition = (SELECT MIN(min_order_id_partition) FROM frt_drt_range)
                 ) ilv
         )
    SELECT CASE WHEN f.orderid is null THEN CAST(b.id AS VARCHAR) ELSE f.orderid END id -- helps for debuging purpose
      FROM %(input_db)s.pivoted_tracs_season_bookings b
      JOIN %(input_db)s.pivoted_tracs_transactions t
        ON b.tr_id = t.id
       AND b.tr_id_partition = t.tr_id_partition
       AND t.start_date_time >= (SELECT min_order_create_date FROM start_date)
       AND t.start_date_time <  DATE'{{ run_end_date_str }}' + INTERVAL '1' DAY
       AND t.tr_id_partition <= (SELECT max_tr_id_partition FROM tr_id_range)
       AND COALESCE(t.is_one_platform, 'N') = 'N'
      LEFT JOIN %(input_db)s.pivoted_frt_tracs_season_transactions f
        ON COALESCE(f.referencefield10,'x') <> 'HIDECLIENTBILLING'
       AND f.tr_id = CAST(b.tr_id as varchar)
	   AND f.ordertype in ('NEW','REN','FRV','VDR')
     WHERE b.del_id is not null
       AND b.renewal NOT IN ('D','C')
       AND ( b.customer_cor_ref is null 
             or 
             ( b.customer_cor_ref is not null and f.orderid is not null ) 
           )
    UNION ALL
    SELECT CAST(b.id AS VARCHAR)
      FROM %(input_db)s.pivoted_tracs_season_bookings b
      JOIN %(input_db)s.pivoted_tracs_transactions t
        ON b.tr_id = t.id
       AND b.tr_id_partition = t.tr_id_partition
       AND t.start_date_time >= (SELECT min_order_create_date FROM start_date)
       AND t.start_date_time <  DATE'{{ run_end_date_str }}' + INTERVAL '1' DAY
       AND t.tr_id_partition <= (SELECT max_tr_id_partition FROM tr_id_range)
       AND COALESCE(t.is_one_platform, 'N') = 'N'
       AND b.renewal IN ('D')
      JOIN %(input_db)s.pivoted_tracs_deliveries d
        ON b.del_id = d.id
       AND b.tr_id_partition = d.tr_id_partition
       AND d.DM_CODE IN ('TOD','SMART')
       AND b.tr_id <> 2293300034
    UNION ALL 
    SELECT fd.orderid
      FROM %(input_db)s.pivoted_frt_direct_season_transactions fd
     WHERE fd.orderdate >= (SELECT min_order_create_date FROM start_date)
       AND fd.orderdate < DATE'{{ run_end_date_str }}' + INTERVAL '1' DAY
       AND COALESCE(fd.referencefield10,'x') <> 'HIDECLIENTBILLING'
       AND fd.ordertype in ('NEW','REN','FRV','VDR','VDC','COJ','REP','DUP','EXC')
    UNION ALL 
    SELECT f.orderid
      FROM %(input_db)s.pivoted_frt_tracs_season_transactions f
     WHERE COALESCE(f.referencefield10,'x') <> 'HIDECLIENTBILLING'
       AND f.ordertype in ('VDC','COJ','REP','DUP','EXC')
       AND f.orderdate >= (SELECT min_order_create_date FROM start_date)
       AND f.orderdate <  DATE'{{ run_end_date_str }}' + INTERVAL '1' DAY
       AND f.tr_id_partition <= (SELECT max_tr_id_partition FROM tr_id_range)
    UNION ALL
    SELECT 'ST'||CAST(floor(mst.st_id) AS VARCHAR)
      FROM %(oracle_mirror_db)s.m_season_transactions mst
     WHERE (
        mst.SOURCE_SYSTEM = 'M_SEASON_CHARGES'
        OR (mst.SOURCE_SYSTEM = 'TCS_SEASON_BOOKINGS' AND (mst.CHANGEOVER_AMOUNT <> 0 OR mst.TICKET_COST = 0 ))
      )
       AND mst.booking_date >= (SELECT min_order_create_date FROM start_date)
       AND mst.booking_date <= DATE'{{ run_end_date_str }}'
       AND mst.tis_date >= DATE'2017-01-01'
       AND mst.ticket_number not like 'MEC%%'
       AND NOT (mst.BOOKING_FEE + mst.DEL_FEE + mst.CARD_FEE = mst.CUSTOMER_AMOUNT AND mst.CUSTOMER_AMOUNT <> 0 AND mst.TICKET_COST = 0 AND mst.VOUCHER_AMOUNT = 0)
       AND mst.transaction_type = 'SALE'
    )
""" + " UNION ALL " + ORDERLINES_SEASONS_FEES_INPUT_SQL + ")"



ORDERLINES_SGP_MERGE_INPUT_SQL = """
select count(*) as query_generic_count from 
                         (select product_travel_product_id ct
                            from %(input_db)s.pivoted_sgp_atoc 
                           where '{{ ds }}' = '%(backfill_end_date)s'
                              or (execution_date = '{{ run_end_date_str }}' and
                                  create_date_ymd  >= '{{ run_start_date_str }}')
                           union
                          select euinvprod_travel_product_id
                            from %(input_db)s.pivoted_sgp_eu 
                           where '{{ ds }}' = '%(backfill_end_date)s'
                              or (execution_date = '{{ run_end_date_str }}' and
                                  create_date_ymd  >= '{{ run_start_date_str }}')
                           union
                          select product_id
                            from %(input_db)s.pivoted_sgp_insurance 
                           where '{{ ds }}' = '%(backfill_end_date)s'
                              or (execution_date = '{{ run_end_date_str }}' and
                                  create_date_ymd  >= '{{ run_start_date_str }}')
                           union
                          select product_id
                            from %(input_db)s.pivoted_sgp_national_express_product_offers_actual 
                           where '{{ ds }}' = '%(backfill_end_date)s'
                              or (execution_date = '{{ run_end_date_str }}' and
                                  create_date_ymd  >= '{{ run_start_date_str }}')
                           union
                          select discount_card_product_id
                            from %(input_db)s.pivoted_sgp_atoc_railcard_inventory_product 
                           where '{{ ds }}' = '%(backfill_end_date)s'
                              or (execution_date = '{{ run_end_date_str }}' and
                                  create_date_ymd  >= '{{ run_start_date_str }}')
                           union
                          select discount_card_product_id
                            from %(input_db)s.pivoted_sgp_eu_railcard_inventory_product 
                           where '{{ ds }}' = '%(backfill_end_date)s'
                              or (execution_date = '{{ run_end_date_str }}' and
                                  create_date_ymd  >= '{{ run_start_date_str }}')
                           union
                          select product_travel_product_id
                            from %(input_db)s.pivoted_sgp_atoc_travel_product_fares 
                           where ('{{ ds }}' = '%(backfill_end_date)s'
                              or (execution_date = '{{ run_end_date_str }}' and
                                  create_date_ymd  >= '{{ run_start_date_str }}'))
                            and regexp_like(travelproduct_fares_farecategory_name,'(?i)season')
                           UNION
                          SELECT df.invoices_id||'-'||df.invoices_delivery_fees_option AS fee_product_id
                            FROM %(input_db)s.pivoted_sgp_order_invoices_delivery_fees df
                           WHERE df.invoices_delivery_fees_price_amount > 0
                              AND ('{{ ds }}' = '%(backfill_end_date)s'
                                   OR (df.orderevent_order_date >= date'{{ ds }}'
                                       AND df.create_date_ymd > date_format(date_add('month',-14,date'{{ ds }}'),'%%Y-%%m-%%d')
                                      )
                                  )
                           UNION
                           -- Booking Fees
                           SELECT bf.invoices_id||'-bookingFee'
                             FROM %(input_db)s.pivoted_sgp_order_invoices_booking_fee_breakdown_references bf
                            WHERE bf.invoices_booking_fee_breakdown_price_amount > 0
                              AND ('{{ ds }}' = '%(backfill_end_date)s'
                                   OR (bf.orderevent_order_date >= date'{{ ds }}'
                                       AND bf.create_date_ymd > date_format(date_add('month',-14,date'{{ ds }}'),'%%Y-%%m-%%d')
                                      )
                                  )
                            UNION
                            -- Payment fees
                           SELECT pf.invoices_id||'-paymentFee'
                             FROM %(input_db)s.pivoted_sgp_order_invoices_payments pf
                            WHERE pf.invoices_payments_payment_fee_price_amount > 0
                              AND pf.orderevent_product_ids IS NOT NULL
                              AND ('{{ ds }}' = '%(backfill_end_date)s'
                                   OR (pf.orderevent_order_date >= date'{{ ds }}'
                                       AND pf.create_date_ymd > date_format(date_add('month',-14,date'{{ ds }}'),'%%Y-%%m-%%d')
                                      )
                                  )
                            UNION
                            -- Promotions
                            SELECT regexp_extract(ip.promocodeuris, '([^/]*)$') ||'_'||
                                    cast(row_number() OVER (PARTITION BY ip.promocodeuris
                                                                ORDER BY ip.invoices_promos_productid) 
                                        as varchar) AS fee_product_id
                              FROM %(input_db)s.pivoted_sgp_order_invoices_promos ip
                             WHERE ('{{ ds }}' = '%(backfill_end_date)s'
                                   OR (ip.orderdate >= date'{{ ds }}'
                                       AND ip.create_date_ymd > date_format(date_add('month',-14,date'{{ ds }}'),'%%Y-%%m-%%d')
                                      )
                                  )
                            UNION
                            -- COJ Admin Fees
                            SELECT af.invoices_id||'-cojAdminFee' AS fee_product_id
                              FROM %(input_db)s.pivoted_sgp_order_invoices_coj_admin_fees af
                             WHERE af.invoices_cojadminfees_price_amount > 0
                               AND ('{{ ds }}' = '%(backfill_end_date)s'
                                   OR (af.orderdate >= date'{{ ds }}'
                                       AND af.create_date_ymd > date_format(date_add('month',-14,date'{{ ds }}'),'%%Y-%%m-%%d')
                                      )
                                  )
                        )
"""

ORDERLINES_SGP_FEES_INPUT_SQL = """
SELECT count(*) 
  FROM (SELECT fees.*
              ,ROW_NUMBER() OVER (PARTITION BY fees.fee_order_id
                                              ,fees.fee_product_id
                                      ORDER BY 1) fee_filter
          FROM (-- Delivery fees
                SELECT df.invoices_id||'-'||df.invoices_delivery_fees_option AS fee_product_id
                      ,df.orderevent_id AS fee_order_id
                  FROM %(input_db)s.pivoted_sgp_order_invoices_delivery_fees df
                 WHERE 1=1
                    AND ('{{ ds }}' = '%(backfill_end_date)s'
                         OR (df.orderevent_order_date >= date'{{ ds }}'
                             AND df.create_date_ymd > date_format(date_add('month',-14,date'{{ ds }}'),'%%Y-%%m-%%d')
                            )
                        )
                 UNION ALL
                 -- Booking Fees
                 SELECT bf.invoices_id||'-booking-fee'
                       ,bf.orderevent_id
                   FROM %(input_db)s.pivoted_sgp_order_invoices_booking_fee_breakdown_references bf
                  WHERE bf.invoices_booking_fee_breakdown_price_amount > 0
                    AND ('{{ ds }}' = '%(backfill_end_date)s'
                         OR (bf.orderevent_order_date >= date'{{ ds }}'
                             AND bf.create_date_ymd > date_format(date_add('month',-14,date'{{ ds }}'),'%%Y-%%m-%%d')
                            )
                        )
                  UNION ALL
                  -- Payment fees
                 SELECT pf.invoices_id||'-payment-fee'
                       ,pf.orderevent_id
                   FROM %(input_db)s.pivoted_sgp_order_invoices_payments pf
                  WHERE pf.invoices_payments_payment_fee_price_amount > 0
                    AND pf.orderevent_product_ids IS NOT NULL
                    AND ('{{ ds }}' = '%(backfill_end_date)s'
                         OR (pf.orderevent_order_date >= date'{{ ds }}'
                             AND pf.create_date_ymd > date_format(date_add('month',-14,date'{{ ds }}'),'%%Y-%%m-%%d')
                            )
                        )
                  UNION
                  -- Promotions
                  SELECT regexp_extract(ip.promocodeuris, '([^/]*)$') ||'_'||
                         cast(row_number() OVER (PARTITION BY ip.promocodeuris
                                                     ORDER BY ip.invoices_promos_productid) 
                              as varchar) AS fee_product_id
                        ,ip.id
                    FROM %(input_db)s.pivoted_sgp_order_invoices_promos ip
                   WHERE ('{{ ds }}' = '%(backfill_end_date)s'
                         OR (ip.orderdate >= date'{{ ds }}'
                             AND ip.create_date_ymd > date_format(date_add('month',-14,date'{{ ds }}'),'%%Y-%%m-%%d')
                            )
                        )
                  UNION
                  -- COJ Admin Fees
                  SELECT af.invoices_id||'-cojAdminFee' AS fee_product_id
                        ,af.id
                    FROM %(input_db)s.pivoted_sgp_order_invoices_coj_admin_fees af
                   WHERE af.invoices_cojadminfees_price_amount > 0
                     AND ('{{ ds }}' = '%(backfill_end_date)s'
                         OR (af.orderdate >= date'{{ ds }}'
                             AND af.create_date_ymd > date_format(date_add('month',-14,date'{{ ds }}'),'%%Y-%%m-%%d')
                            )
                        )
               ) fees
       )
WHERE fee_filter = 1
"""

ORDERLINES_SGP_RESERVATIONS_INPUT_SQL = """
SELECT COUNT(*)
FROM (SELECT ar.fare_leg_reservation_id
            ,ar.space_allocations_id
            ,ar.travelproduct_id 
            ,ar.travelproduct_fares_id 
            ,ar.travelproduct_fares_farelegs_id 
        FROM pivoted_sgp_atoc_reservation_space_allocations ar
       WHERE '{{ ds }}' = '%(backfill_end_date)s'
          OR (ar.execution_date = '{{ run_end_date_str }}' AND
              ar.create_date_ymd  >= '{{ run_start_date_str }}')
       UNION
      SELECT er.fare_leg_reservation_id
            ,er.space_allocations_id
            ,er.travelproduct_id 
            ,er.travelproduct_fares_id 
            ,er.travelproduct_fares_farelegs_id 
        FROM pivoted_sgp_eu_reservation_space_allocations er
       WHERE '{{ ds }}' = '%(backfill_end_date)s'
          OR (er.execution_date = '{{ run_end_date_str }}' AND
              er.create_date_ymd  >= '{{ run_start_date_str }}')
     )
"""

ORDERLINES_TTL_MERGE_INPUT_SQL = """
select count(mb_id) as query_generic_count 
from %(input_db)s.pivoted_ttl_manual_bookings
where date_of_booking >= CAST(DATE'2017-01-01' AS TIMESTAMP) 
"""

def get_merge_source(source):
    if source == Source.FLIXBUS:
        end_date = FLIXBUS_BACKFILL_END_DATES[TC_ENV.lower()]['merge']
        pipelines = [
            Pipeline(data_source='merge',
                     input_sql=ORDERLINES_FLIXBUS_MERGE_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table="pivoted_flixbus_actions",
                     output_sql=ORDERLINES_FLIXBUS_MERGE_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fare_legs")
        ]
        flixbus = MergeSource(data_source=Source.FLIXBUS,
                              backfill_min_date=BACKFILL_MIN_DATE,
                              backfill_end_date=end_date,
                              input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                              # todo: move to pipeline
                              output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                              # todo: move to pipeline
                              pipelines=pipelines)
        return flixbus

    elif source == Source._25KV:
        end_date = _25KV_BACKFILL_END_DATES[TC_ENV.lower()]['merge']
        pipelines = [
            Pipeline(data_source='merge',
                     input_sql=ORDERLINES_25KV_MERGE_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table='None',
                     output_sql=ORDERLINES_25KV_MERGE_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fare_legs")
        ]
        t25kv = MergeSource(data_source=Source._25KV,
                            backfill_min_date=BACKFILL_MIN_DATE,
                            backfill_end_date=end_date,
                            input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                            output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                            pipelines=pipelines)
        return t25kv

    elif source == Source.TRACS:
        end_date = TRACS_BACKFILL_END_DATES[TC_ENV.lower()]['merge']
        pipelines = [
            Pipeline(data_source='merge',
                     input_sql=ORDERLINES_TRACS_MERGE_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table='None',
                     output_sql=ORDERLINES_TRACS_MERGE_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fare_legs"),
            Pipeline(data_source='fees',
                     input_sql=ORDERLINES_TRACS_FEES_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table='None',
                     output_sql=ORDERLINES_TRACS_FEES_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fee_details"),
            Pipeline(data_source='reservations',
                     input_sql=ORDERLINES_TRACS_RESERVATIONS_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table='None',
                     output_sql=ORDERLINES_TRACS_RESERVATIONS_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fare_leg_reservations")
        ]
        tracs = MergeSource(data_source=Source.TRACS,
                            backfill_min_date=BACKFILL_MIN_DATE,
                            backfill_end_date=end_date,
                            input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                            output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                            pipelines=pipelines)
        return tracs

    elif source == Source.BEBOC:
        end_date = BEBOC_BACKFILL_END_DATES[TC_ENV.lower()]['merge']
        pipelines = [
            Pipeline(data_source='merge',
                     input_sql=ORDERLINES_BEBOC_MERGE_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table='None',
                     output_sql=ORDERLINES_BEBOC_MERGE_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fare_legs")
        ]
        beboc = MergeSource(data_source=Source.BEBOC,
                            backfill_min_date=BACKFILL_MIN_DATE,
                            backfill_end_date=end_date,
                            input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                            output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                            pipelines=pipelines)
        return beboc

    elif source == Source.SEASONS:
        end_date = SEASONS_BACKFILL_END_DATES[TC_ENV.lower()]['merge']
        pipelines = [
            Pipeline(data_source='merge',
                     input_sql=ORDERLINES_SEASONS_MERGE_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table='None',
                     output_sql=ORDERLINES_SEASONS_MERGE_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fare_legs"),
            Pipeline(data_source='fees',
                     input_sql=ORDERLINES_SEASONS_FEES_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table='None',
                     output_sql=ORDERLINES_SEASONS_FEES_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fee_details")
        ]
        seasons = MergeSource(data_source=Source.SEASONS,
                              backfill_min_date=BACKFILL_MIN_DATE,
                              backfill_end_date=end_date,
                              input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                              output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                              pipelines=pipelines)
        return seasons

    elif source == Source.SGP:
        end_date = SGP_BACKFILL_END_DATES[TC_ENV.lower()]['merge']
        pipelines = [
            Pipeline(data_source='merge',
                     input_sql=ORDERLINES_SGP_MERGE_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table='None',
                     output_sql=ORDERLINES_SGP_MERGE_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fare_legs"),
            Pipeline(data_source='fees',
                     input_sql=ORDERLINES_SGP_FEES_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table='None',
                     output_sql=ORDERLINES_SGP_FEES_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fee_details"),
            Pipeline(data_source='reservations',
                     input_sql=ORDERLINES_SGP_RESERVATIONS_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table='None',
                     output_sql=ORDERLINES_SGP_RESERVATIONS_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fare_leg_reservations")
        ]
        sgp = MergeSource(data_source=Source.SGP,
                          backfill_min_date=BACKFILL_MIN_DATE,
                          backfill_end_date=end_date,
                          input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                          output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                          pipelines=pipelines)
        return sgp

    elif source == Source.TTL:
        end_date = TTL_BACKFILL_END_DATES[TC_ENV.lower()]['merge']
        pipelines = [
            Pipeline(data_source='merge',
                     input_sql=ORDERLINES_TTL_MERGE_INPUT_SQL,
                     input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                     oracle_mirror_db=ORACLE_MIRROR_DBS[TC_ENV.lower()],
                     input_table='None',
                     output_sql=ORDERLINES_TTL_MERGE_OUTPUT_SQL,
                     output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                     output_table="order_line_fare_legs")
        ]
        ttl = MergeSource(data_source=Source.TTL,
                          backfill_min_date=BACKFILL_MIN_DATE,
                          backfill_end_date=end_date,
                          input_db=ORDERLINES_PIVOT_DBS[TC_ENV.lower()],
                          output_db=ORDERLINES_MERGE_DBS[TC_ENV.lower()],
                          pipelines=pipelines)
        return ttl
