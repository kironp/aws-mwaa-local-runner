import json
import logging
from os import environ

import requests

from templates.slack import SlackWebHookMessage, BlockCollection, \
    PlainTextSection, FieldCollectionSection, StandardField, ButtonSection
from urls.airflow_urls import get_graph_view_dag_url, get_tree_view_dag_url, \
    get_task_log_url, get_task_xcom_url, AIRFLOW_NETLOC, AIRFLOW_SCHEME

BI_WEBHOOK_URL_SSM_NAME = 'slack_channel_bi_alerts_webbhook_url'
BI_WEBHOOK_CHANNEL = '#bi_alerts'

AIRFLOW_LABEL = {
    'dev': 'localhost',
    'data': 'fastnet'
}

LOG = logging.getLogger(__name__)


def get_environ(val):
    r = environ.get(val, None)
    if r:
        r = r.lower()
    return r


def post_to_slack(message, webhook_url):
    LOG.info(json.dumps(message, indent=3))
    if get_environ('TC_ENV') == 'dev' and not get_environ('POST_TO_SLACK'):
        LOG.info('TC_ENV == dev. Will skip posting messing to slack unless environment variable POST_TO_SLACK=TRUE')
        return None

    response = requests.post(webhook_url, json=message)
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is: %s' % (response.status_code, response.text))


class AirflowInstance:
    def __init__(self):
        self.env = get_environ('TC_ENV')
        self.label = AIRFLOW_LABEL[self.env]
        self.netloc = AIRFLOW_NETLOC[self.env]
        self.scheme = AIRFLOW_SCHEME[self.env]


class BaseBlocksMessage:

    def __init__(self, task_instance):
        blocks = BlockCollection()
        field_section = FieldCollectionSection([])
        field_section.append(StandardField('Dag_id', task_instance.dag_id))
        field_section.append(StandardField('Task_id', task_instance.task_id))
        field_section.append(StandardField('Execution_date', task_instance.execution_date))
        blocks.append(field_section)
        self.blocks = blocks

class BaseBlocksAlertMessage(BaseBlocksMessage):
    def __init__(self, airflow_instance, task_instance, *args, **kwargs):
        super().__init__(task_instance, *args, **kwargs)
        graph_button = ButtonSection('View DAG; graph view',
                                     ':giraffe_face:',
                                     get_graph_view_dag_url(task_instance.dag_id,
                                                            task_instance.execution_date,
                                                            airflow_instance.scheme,
                                                            airflow_instance.netloc
                                                            ))
        tree_button = ButtonSection('View DAG; tree view',
                                    ':evergreen_tree: :palm_tree: :evergreen_tree: ',
                                    get_tree_view_dag_url(task_instance.dag_id,
                                                          airflow_instance.scheme,
                                                          airflow_instance.netloc
                                                          ))
        task_log = ButtonSection('View Task Log',
                                 ':logs: :logs: :logs: ',
                                 get_task_log_url(task_instance.dag_id,
                                                  task_instance.task_id,
                                                  task_instance.execution_date,
                                                  scheme=airflow_instance.scheme,
                                                  netloc=airflow_instance.netloc
                                                  ))
        xcom_button = ButtonSection('View Xcom Data',
                                    ':crossed_swords:',
                                    get_task_xcom_url(task_instance.dag_id,
                                                      task_instance.task_id,
                                                      task_instance.execution_date,
                                                      scheme=airflow_instance.scheme,
                                                      netloc=airflow_instance.netloc
                                                      ))
        self.blocks.append(graph_button)
        self.blocks.append(tree_button)
        self.blocks.append(task_log)
        self.blocks.append(xcom_button)

class BaseBlocksFailureMessage(BaseBlocksAlertMessage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blocks.blocks.insert(0, PlainTextSection(':poop::poop::poop::poop::poop:\nFailure Notification'))


class BaseBlocksRetryMessage(BaseBlocksAlertMessage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blocks.blocks.insert(0, PlainTextSection(':eyes::eyes::eyes::eyes::eyes:\nRetry Notification'))


class BaseBlocksXComMessage(BaseBlocksMessage):
    def __init__(self, task_instance, *args, **kwargs):
        super().__init__(task_instance, *args, **kwargs)
        self.blocks.blocks.insert(0, PlainTextSection(':crossed_swords::crossed_swords::crossed_swords::crossed_swords::crossed_swords:\nXCom Notification'))
        xcom_data = task_instance.xcom_pull()

        field_headings = []
        for key in list(xcom_data[0].keys()):
            field_headings += [key]
        field_data = {}
        for heading in field_headings:
            field_data[heading] = '\n'.join(str(f[heading]) for f in xcom_data)

        field_section = FieldCollectionSection([])
        for heading in field_headings:
            field_section.append(StandardField(heading, field_data[heading]))

        self.blocks.append(field_section)


def message_builder(airflow_instance, task_instance, type, channel):
    if type == 'failure':
        return SlackWebHookMessage(channel=channel,
                                   blocks=BaseBlocksFailureMessage(airflow_instance, task_instance).blocks)
    if type == 'retry':
        return SlackWebHookMessage(channel=channel,
                                   blocks=BaseBlocksRetryMessage(airflow_instance, task_instance).blocks)

    if type == 'xcom':
        return SlackWebHookMessage(channel=channel,
                                   blocks=BaseBlocksXComMessage(task_instance).blocks)
