from aws.boto.ssm import get_ssm_value
from callbacks.slack_utils import AirflowInstance, message_builder, post_to_slack, \
    BI_WEBHOOK_URL_SSM_NAME, BI_WEBHOOK_CHANNEL

def task_fail_slack_notification_bi_channel_callback(ctx,
                                                     channel=BI_WEBHOOK_CHANNEL,
                                                     url_ssm_name=BI_WEBHOOK_URL_SSM_NAME):
    task_instance = ctx['ti']
    airflow_instance = AirflowInstance()
    message = message_builder(airflow_instance, task_instance, 'failure', channel).to_dict()
    post_to_slack(message=message,
                  webhook_url=get_ssm_value(url_ssm_name))


def task_retry_slack_notification_bi_channel_callback(ctx,
                                                      channel=BI_WEBHOOK_CHANNEL,
                                                      url_ssm_name=BI_WEBHOOK_URL_SSM_NAME):
    task_instance = ctx['ti']
    airflow_instance = AirflowInstance()
    message = message_builder(airflow_instance, task_instance, 'retry', channel).to_dict()
    post_to_slack(message=message,
                  webhook_url=get_ssm_value(url_ssm_name))

def task_xcom_slack_notification_bi_channel_callback(ctx,
                                                     channel=BI_WEBHOOK_CHANNEL,
                                                     url_ssm_name=BI_WEBHOOK_URL_SSM_NAME):
    task_instance = ctx['ti']
    airflow_instance = AirflowInstance()
    message = message_builder(airflow_instance, task_instance, 'xcom', channel).to_dict()
    post_to_slack(message=message,
                  webhook_url=get_ssm_value(url_ssm_name))