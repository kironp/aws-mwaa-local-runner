from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class MarkerSensor(BaseSensorOperator):
    template_fields = ()

    @apply_defaults
    def __init__(self,
                 marker,
                 incl_execution_date=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.marker = marker
        self.incl_execution_date = incl_execution_date

    def poke(self, context):
        if self.incl_execution_date:
            self.marker.execution_date = context['ts']
        self.log.info("Poke {}".format(self.marker))
        return self.marker.exists
