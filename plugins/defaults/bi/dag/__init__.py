import os

TC_ENV = os.environ.get('TC_ENV')

# TC_ENV = os.environ.get('TC_ENV')
TC_ENV = "DEV"

SENSOR_TIMEOUT_DEFAULT = 6 * 60 * 60
POKE_INTERVAL_PROD = 60 * 10
POKE_INTERVAL_DEFAULT = 60 * 15

SENSOR_DEFAULTS = {'poke_interval': POKE_INTERVAL_PROD if TC_ENV.lower() == 'data' else POKE_INTERVAL_DEFAULT,
                   'mode': 'reschedule', 'timeout': SENSOR_TIMEOUT_DEFAULT,
                   'aws_conn_id': None, 'region_name': 'eu-west-1'}

LOGGING_S3_BUCKET = {'dev': 'tld-dev-priv-data-lab',
                     'staging': 'tld-staging-priv-data-lab',
                     'data': 'tld-prod-priv-data-lab'}

COUNTER_DEFAULTS = {'s3_location_for_query_results': "s3://" + LOGGING_S3_BUCKET[os.environ.get('TC_ENV',
                                                                                                'dev').lower()] + "/"
                                                                                                                  "bi_team/data_quality_checks/athena_results/",
                    'aws_conn_id': None, 'region_name': 'eu-west-1'}

TEAM = 'bi_team'
JOB_SUCCESS_MARKER_TYPE = 'job_success_marker'
