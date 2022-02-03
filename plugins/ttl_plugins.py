from airflow.plugins_manager import AirflowPlugin
from tl_operators.emr_launch_cluster_submit_job_operator import EMRLaunchClusterAndRunSparkJobOperator
from tl_operators.emr_ctx_conf_launch_cluster_submit_job_operator import EMRCtxConfLaunchClusterSubmitJobOperator
from tl_operators.pagerduty_incident_operator import PagerDutyIncidentOperator
from tl_sensors.marker_sensor import MarkerSensor
from tl_sensors.python_sensor import PythonSensor
from defaults.bi.emr.config import get_emr_config
# from defaults.bi.dag import COUNTER_DEFAULTS, LOGGING_S3_BUCKET, TEAM, JOB_SUCCESS_MARKER_TYPE, \
#     SENSOR_DEFAULTS
from utils.date import get_all_possible_execution_dates, add_next_exec_date
from utils.marker import S3Marker
from utils.marker_set import S3MarkerSet
from utils.defaults import Defaults
from callbacks.slack import task_fail_slack_notification_bi_channel_callback
from callbacks.pagerduty import pager_duty_incident_dag_failure
import os
# from tl_operators.athena_check_operator import AthenaCheckOperator
# from tl_operators.athena_query_generic_count_operator import AthenaQueryGenericCountOperator
# from tl_operators.athena_table_generic_count_operator import AthenaTableGenericCountOperator
# from tl_operators.athena_table_row_count_operator import AthenaTableRowCountOperator
# from tl_operators.athena_count_operator import AthenaCountOperator
# from tl_operators.aws_glue_catalog_partition_sensor import AwsGlueCatalogPartitionSensor
# from tl_operators.delete_glue_table_s3_data_operator import DeleteGlueTableS3DataOperator

# from tl_operators.marker_delete_operator import MarkerDeleteOperator
# from tl_operators.marker_set_delete_operator import MarkerSetDeleteOperator
# from tl_operators.marker_write_operator import MarkerWriteOperator
# from tl_operators.oracle_check_operator import OracleCheckOperator
# from tl_operators.oracle_query_generic_count_operator import OracleQueryGenericCountOperator
# from tl_operators.oracle_table_generic_count_operator import OracleTableGenericCountOperator
# from tl_operators.oracle_table_row_count_operator import OracleTableRowCountOperator
# from tl_operators.tableau_trigger_extract_operator import TableauTriggerExtractOperator
# from tl_operators.task_count_equality_operator import TaskCountEqualityCheckOperator
# from tl_operators.task_count_near_equality_operator import TaskCountNearEqualityCheckOperator
# from tl_operators.athena_query_operator import (AthenaQueryOperator, SQLReturnTypes)
# from tl_operators.tableau_trigger_subscription_operator import TableauTriggerSubscriptionOperator
#


class TtlPlugins(AirflowPlugin):
     name = 'ttl_plugins'

     tl_operators = [PagerDutyIncidentOperator,
                  EMRLaunchClusterAndRunSparkJobOperator,
                  EMRCtxConfLaunchClusterSubmitJobOperator]
     tl_sensors = [
                  PythonSensor,
                  MarkerSensor]
     utils = [
         Defaults,
         get_all_possible_execution_dates,
         add_next_exec_date,
         S3Marker,
         S3MarkerSet
     ]

     callbacks = [
          task_fail_slack_notification_bi_channel_callback, pager_duty_incident_dag_failure
     ]