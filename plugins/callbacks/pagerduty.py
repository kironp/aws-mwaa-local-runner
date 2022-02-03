from operators.pagerduty_incident_operator import PagerDutyIncidentOperator
from aws_utils.boto_conf.ssm_conf import get_ssm_value
from os import environ


def get_environ(val):
    r = environ.get(val, None)
    if r:
        r = r.lower()
    return r


BI_PAGERDUTY_EMAIL_ID = {
    'staging': 'bi_stg_team_email',
    'data': 'bi_team_email',
    'dev': 'bi_team_email'
}

BI_PAGERDUTY_SERVICE_ID = {
    'staging': 'bi_stg_pd_generic_oncall_service_id',
    'data': 'bi_pd_generic_oncall_service_id',
    'dev': 'bi_pd_generic_oncall_service_id'
}

API_KEY = {
    'staging': 'bi_stg_pd_api_key',
    'data': 'bi_pd_api_key',
    'dev': 'bi_pd_generic_oncall_service_id'
}

TC_ENV = get_environ('TC_ENV')


# For schema, refer - https://developer.pagerduty.com/api-reference/reference/REST/openapiv3.json/paths/~1incidents/post

def pager_duty_incident_dag_failure(context):
    details = """
                    TASK FAILED!
                    Task: {task}  |
                    Dag: {dag} |
                    Execution Time: {exec_date}               
                    """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        exec_date=context.get('execution_date')
    )
    operator = PagerDutyIncidentOperator(
        task_id=str(context.get('task_instance').task_id),
        title='Bi Airflow Alerts',
        email_address=get_ssm_value(BI_PAGERDUTY_EMAIL_ID[TC_ENV]),
        api_key=get_ssm_value(API_KEY[TC_ENV]),
        service_id=get_ssm_value(BI_PAGERDUTY_SERVICE_ID[TC_ENV]),
        details=details
    )
    return operator.execute(context=context)
