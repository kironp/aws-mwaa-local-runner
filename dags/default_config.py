from orderlines_sgp_history import (
    sgp_history_meta,
    sgp_history_backfill_meta,
    sgp_history_sensor_meta
)
from orderlines_sgp_pivot import sgp_pivot_meta, sgp_pivot_backfill_meta
from orderlines_sgp_merge import sgp_merge_meta, sgp_merge_backfill_meta
from orderlines_25kv_history import (
    _25kv_history_meta,
    _25kv_history_backfill_meta,
    _25kv_history_sensor_meta
)
from orderlines_25kv_pivot import _25kv_pivot_meta, _25kv_pivot_backfill_meta
from orderlines_25kv_merge import _25kv_int_meta, _25kv_merge_meta, \
    _25kv_int_backfill_meta, _25kv_merge_backfill_meta
from orderlines_beboc_history import (
    beboc_history_meta,
    beboc_history_backfill_meta,
    beboc_history_sensor_meta
)
from orderlines_beboc_pivot import beboc_pivot_meta , beboc_pivot_backfill_meta
from orderlines_beboc_merge import beboc_int_meta, beboc_merge_meta, beboc_int_backfill_meta, \
    beboc_merge_backfill_meta
from orderlines_flixbus_history import (
    flixbus_history_meta,
    flixbus_history_backfill_meta,
    flixbus_history_sensor_meta
)
from orderlines_flixbus_pivot import flixbus_pivot_meta, flixbus_pivot_backfill_meta
from orderlines_flixbus_merge import flixbus_merge_meta, flixbus_merge_backfill_meta
from orderlines_frt_history import (
    frt_history_meta,
    frt_history_backfill_meta,
    frt_history_sensor_meta
)
from orderlines_frt_pivot import frt_pivot_meta, frt_pivot_backfill_meta
from orderlines_seasons_merge import seasons_merge_meta, seasons_merge_backfill_meta
from orderlines_tracs_history import (
    tracs_history_meta,
    tracs_history_backfill_meta,
    tracs_history_sensor_meta
)
from orderlines_tracs_pivot import tracs_pivot_meta, tracs_pivot_backfill_meta
from orderlines_tracs_merge import tracs_merge_meta, tracs_merge_backfill_meta
from orderlines_ttl_manual_pivot import (
    ttl_pivot_meta,
    ttl_pivot_backfill_meta,
    ttl_pivot_sensor_meta
)
from orderlines_ttl_manual_merge import ttl_merge_meta, ttl_merge_backfill_meta
from orderlines_currency_fx import currency_meta, currency_sensor_meta
from refunds_dag_components import (

    seasons_int_refunds_meta,
    frt_int_refunds_meta,
    _25kv_int_refunds_meta,
    tracs_pivot_refunds_sensor_meta,
    tracs_pivot_refunds_meta,
    tracs_int_refunds_meta,
    beboc_int_refunds_meta,
    ttl_pivot_refunds_sensor_meta,
    ttl_pivot_refunds_meta,
    ttl_int_refunds_meta,
    refunds_merge_meta
)
from external_data_feeds_history import (
    external_data_feeds_history_meta,
    external_data_feeds_history_backfill_meta
)
from external_data_feeds_pivot import (
    external_data_feeds_pivot_meta,
    external_data_feeds_pivot_backfill_meta
)


class Daily:
    sgp_history_meta = sgp_history_meta
    sgp_pivot_meta = sgp_pivot_meta
    sgp_merge_meta = sgp_merge_meta
    _25kv_history_meta = _25kv_history_meta
    _25kv_pivot_meta = _25kv_pivot_meta
    _25kv_int_meta = _25kv_int_meta
    _25kv_merge_meta = _25kv_merge_meta
    beboc_history_meta = beboc_history_meta
    beboc_pivot_meta = beboc_pivot_meta
    beboc_int_meta = beboc_int_meta
    beboc_merge_meta = beboc_merge_meta
    flixbus_history_meta = flixbus_history_meta
    flixbus_pivot_meta = flixbus_pivot_meta
    flixbus_merge_meta = flixbus_merge_meta
    frt_history_meta = frt_history_meta
    frt_pivot_meta = frt_pivot_meta
    seasons_merge_meta = seasons_merge_meta
    tracs_history_meta = tracs_history_meta
    tracs_pivot_meta = tracs_pivot_meta
    tracs_merge_meta = tracs_merge_meta
    ttl_pivot_meta = ttl_pivot_meta
    ttl_merge_meta = ttl_merge_meta
    currency_meta = currency_meta

    seasons_int_refunds_meta = seasons_int_refunds_meta
    frt_int_refunds_meta = frt_int_refunds_meta
    _25kv_int_refunds_meta = _25kv_int_refunds_meta
    tracs_pivot_refunds_meta = tracs_pivot_refunds_meta
    tracs_int_refunds_meta = tracs_int_refunds_meta
    beboc_int_refunds_meta = beboc_int_refunds_meta
    ttl_pivot_refunds_meta = ttl_pivot_refunds_meta
    ttl_int_refunds_meta = ttl_int_refunds_meta
    refunds_merge_meta = refunds_merge_meta

    _25kv_history_sensor_meta = _25kv_history_sensor_meta
    beboc_history_sensor_meta = beboc_history_sensor_meta
    currency_sensor_meta = currency_sensor_meta
    flixbus_history_sensor_meta = flixbus_history_sensor_meta
    ttl_pivot_sensor_meta = ttl_pivot_sensor_meta
    tracs_history_sensor_meta = tracs_history_sensor_meta
    sgp_history_sensor_meta = sgp_history_sensor_meta
    frt_history_sensor_meta = frt_history_sensor_meta

    tracs_pivot_refunds_sensor_meta = tracs_pivot_refunds_sensor_meta
    ttl_pivot_refunds_sensor_meta = ttl_pivot_refunds_sensor_meta

    external_data_feeds_history_meta = external_data_feeds_history_meta
    external_data_feeds_pivot_meta = external_data_feeds_pivot_meta





class Backfill:
    sgp_history_meta = sgp_history_backfill_meta
    sgp_pivot_meta = sgp_pivot_backfill_meta
    sgp_merge_meta = sgp_merge_backfill_meta
    _25kv_history_meta = _25kv_history_backfill_meta
    _25kv_pivot_meta = _25kv_pivot_backfill_meta
    _25kv_int_meta = _25kv_int_backfill_meta
    _25kv_merge_meta = _25kv_merge_backfill_meta
    beboc_history_meta = beboc_history_backfill_meta
    beboc_pivot_meta = beboc_pivot_backfill_meta
    beboc_int_meta = beboc_int_backfill_meta
    beboc_merge_meta = beboc_merge_backfill_meta
    flixbus_history_meta = flixbus_history_backfill_meta
    flixbus_pivot_meta = flixbus_pivot_backfill_meta
    flixbus_merge_meta = flixbus_merge_backfill_meta
    frt_history_meta = frt_history_backfill_meta
    frt_pivot_meta = frt_pivot_backfill_meta
    seasons_merge_meta = seasons_merge_backfill_meta
    tracs_history_meta = tracs_history_backfill_meta
    tracs_pivot_meta = tracs_pivot_backfill_meta
    tracs_merge_meta = tracs_merge_backfill_meta
    ttl_pivot_meta = ttl_pivot_backfill_meta
    ttl_merge_meta = ttl_merge_backfill_meta
    currency_meta = currency_meta

    seasons_int_refunds_meta = seasons_int_refunds_meta
    frt_int_refunds_meta = frt_int_refunds_meta
    _25kv_int_refunds_meta = _25kv_int_refunds_meta
    tracs_pivot_refunds_meta = tracs_pivot_refunds_meta
    tracs_int_refunds_meta = tracs_int_refunds_meta
    beboc_int_refunds_meta = beboc_int_refunds_meta
    ttl_pivot_refunds_meta = ttl_pivot_refunds_meta
    ttl_int_refunds_meta = ttl_int_refunds_meta
    refunds_merge_meta = refunds_merge_meta

    external_data_feeds_history_meta = external_data_feeds_history_backfill_meta
    external_data_feeds_pivot_meta = external_data_feeds_pivot_backfill_meta



    _25kv_history_sensor_meta = None
    beboc_history_sensor_meta = None
    currency_sensor_meta = None
    flixbus_history_sensor_meta = None
    ttl_pivot_sensor_meta = None
    tracs_history_sensor_meta = None
    sgp_history_sensor_meta = None
    frt_history_sensor_meta = None

    tracs_pivot_refunds_sensor_meta = None
    ttl_pivot_refunds_sensor_meta = None
