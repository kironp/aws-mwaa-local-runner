from airflow.utils.decorators import apply_defaults

from tl_operators.emr_launch_cluster_submit_job_operator import EMRLaunchClusterAndRunSparkJobOperator


class EMRCtxConfLaunchClusterSubmitJobOperator(EMRLaunchClusterAndRunSparkJobOperator):
    @apply_defaults
    def __init__(self, emr_config_callable, bash_command_callable, bash_command_callable_args_dict, *args,
                 **kwargs):
        self.emr_config_callable = emr_config_callable
        super().__init__(emr_config='',
                         bash_command_callable=bash_command_callable,
                         bash_command_callable_args_dict=bash_command_callable_args_dict,
                         *args,
                         **kwargs)

    def execute(self, context):
        config_callable_kwargs = self._get_updated_context(context)
        self.emr_config = self.emr_config_callable(config_callable_kwargs)
        super().execute(context)
