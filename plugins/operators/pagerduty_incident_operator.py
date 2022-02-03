from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import logging
import requests


class PagerDutyIncidentOperator(BaseOperator):
    """
    Creates PagerDuty Incidents.
    """

    @apply_defaults
    def __init__(self,
                 api_key,
                 title,
                 email_address,
                 service_id,
                 details='No details provided',
                 *args,
                 **kwargs):
        super(PagerDutyIncidentOperator, self).__init__(*args, **kwargs)
        self.api_key = api_key
        self.title = title
        self.email_address = email_address
        self.service_id = service_id
        self.details = details

    def execute(self, **kwargs):
        api_url = 'https://api.pagerduty.com/incidents'
        headers = {'Content-Type': 'application/json',
                   'Authorization': 'Token token={}'.format(self.api_key),
                   'Accept': 'application/vnd.pagerduty+json;version=2',
                   'From': self.email_address}
        data = {
            "incident": {
                "type": "incident",
                "title": self.title,
                "service": {
                    "id": self.service_id,
                    "type": "service_reference"
                },
                "urgency": "high",
                "body": {
                    "type": "incident_body",
                    "details": self.details
                }
            }
        }
        response = requests.post(api_url, headers=headers, json=data)

        if response.status_code != 201:
            msg = "PagerDuty API call failed ({})".format(response.reason)
            logging.error(msg)
            raise AirflowException(msg)
