from defaults.bi.dag import COUNTER_DEFAULTS, LOGGING_S3_BUCKET, TEAM, JOB_SUCCESS_MARKER_TYPE, \
    SENSOR_DEFAULTS
from defaults.bi.emr.config import get_emr_config

class Defaults:

    COUNTER_DEFAULTS = COUNTER_DEFAULTS
    LOGGING_S3_BUCKET = LOGGING_S3_BUCKET
    TEAM = TEAM
    JOB_SUCCESS_MARKER_TYPE = JOB_SUCCESS_MARKER_TYPE
    SENSOR_DEFAULTS = SENSOR_DEFAULTS
    get_emr_config = get_emr_config