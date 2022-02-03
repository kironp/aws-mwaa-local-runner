import boto3
from airflow.utils.decorators import apply_defaults
from utils.emr_utils import launch_emr_cluster, run_spark_bash_commands
from airflow.models.baseoperator import  BaseOperator
from airflow.exceptions import AirflowException


class EMRLaunchClusterAndRunSparkJobOperator(BaseOperator):
    @apply_defaults
    def __init__(self, emr_config, bash_command_callable, bash_command_callable_args_dict,
                 step_action_on_failure='TERMINATE_CLUSTER',
                 *args, **kwargs):
        super(EMRLaunchClusterAndRunSparkJobOperator, self).__init__(*args, **kwargs)
        if not callable(bash_command_callable):
            raise AirflowException('`set_spark_job_config` parameter must be callable')
        self.emr_config = emr_config
        self.bash_command_callable = bash_command_callable
        self.bash_command_callable_args_dict = bash_command_callable_args_dict or {}
        self.step_action_on_failure = step_action_on_failure

    def _get_updated_context(self, context: dict):
        r = self.bash_command_callable_args_dict
        r.update(context)
        return r

    def execute(self, context):
        """
        :param context: 'Context variables from Airflow DAG'
        """
        self.log.info('--------------------------------------------------------------------------------')
        self.log.info(context)

        bash_callable_kwargs = self._get_updated_context(context)
        bash_commands = self.bash_command_callable(**bash_callable_kwargs)
        if not isinstance(bash_commands, list):
            bash_commands = [bash_commands]
        self.log.info('SPARK BASH COMMANDS:')
        for bash_command in bash_commands:
            self.log.info(bash_command)
        self.log.info('--------------------------------------------------------------------------------')
        self.log.info('[Get Spark Cluster] Phase 1 Creating EMR Cluster: Starting')
        client = boto3.client(service_name='emr', region_name='eu-west-1')
        cluster_id = launch_emr_cluster(self.emr_config, client)
        run_spark_bash_commands(bash_commands=bash_commands,
                                client=client,
                                cluster_id=cluster_id,
                                step_names=['spark'] * len(bash_commands),
                                wait_for_completion=True,
                                action_on_failure=self.step_action_on_failure)
        self.log.info('[Submit Spark JOB] Phase 1 Running Spark Job: DONE')


