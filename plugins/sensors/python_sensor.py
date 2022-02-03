from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class PythonSensor(BaseSensorOperator):
    template_fields = ()

    @apply_defaults
    def __init__(self,
                 expected_callable,
                 poke_callable,
                 op_kwargs=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.poke_callable = poke_callable
        self.expected_callable = expected_callable
        self.op_kwargs = op_kwargs or {}
        self.expected = None

    def poke(self, context):
        context.update(self.op_kwargs)
        if not self.expected:
            self.expected = set(self.expected_callable(**context))
        self.log.info("Expected values {}".format(sorted(self.expected)))

        found = set(self.poke_callable(**context))
        self.log.info("Found values {}".format(sorted(found)))

        missing = self.expected - found
        self.log.info("Values not found {}".format(sorted(missing)))
        return not missing
