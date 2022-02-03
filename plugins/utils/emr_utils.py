from __future__ import absolute_import

import logging

import backoff
import boto3

from botocore.exceptions import ClientError, WaiterError
from utils.date import get_today, safely

LOG = logging.getLogger(__name__)

MAX_RETRIES_FOR_BACKOFF_EXCEPTIONS = 25


def get_emr_cluster_name(prefix, app, label, env):
    """
    :param prefix: prefix used when creating the cluster
    :param app: app name as specified when creating the cluster
    :param label: label as specified when creating the cluster
    :param env: env name as specified when creating the cluster
    :return:
    """
    launch_date = get_today()
    return '-'.join([prefix, app, label, env, launch_date])


def add_or_replace_emr_tag(tag_list, key_, value_):
    list_ = [d_ for d_ in tag_list if d_['Key'] != key_]
    list_ += [
        {
            "Key": key_,
            "Value": value_
        }
    ]
    return list_


def launch_emr_cluster(config, client=None):
    """
        This function has the purpose to create an
        EMR cluster and return the cluster ID to the caller.
    :param config: config dict which contains info about the cluster to create
    :param client: boto3.client (optional) if it is none, one will be created
    :return: The cluster ID just created
    """
    client = client or boto3.client(service_name='emr', region_name='eu-west-1')  # noqa: E501
    emr_config = get_emr_config(config)
    emr_update({'client': client})
    return run_job_flow(**emr_config)


def run_spark_bash_commands(bash_commands, client, cluster_id, step_names, wait_for_completion=True,
                            action_on_failure='TERMINATE_CLUSTER'):
    """
    :param bash_commands: bash commands to run on emr. normally spark-submit ...
    :type bash_commands: list
    :param client: boto3 client
    :param cluster_id: emr cluster_id
    :param step_names: name for this EMR step
    :param wait_for_completion: whether waiting or not for the job to complete before returning  # noqa: E501
    :return: None
    """
    steps = add_job_flow_steps(client,
                               cluster_id,
                               create_emr_steps_bash(bash_commands=bash_commands,
                                                     step_names=step_names,
                                                     action_on_failure=action_on_failure)
                               )

    if wait_for_completion:
        check_steps_execution(client, cluster_id, steps)


def run_spark_job(param, client, step_name, wait_for_completion=True):
    """
    :param param: dict with the following keys:
                - packs Any package to include when submitting the job
                - spark_args Any Spark argument to provide when submitting the job  # noqa: E501
                - job_path Path of the application to run
                - cluster_id Cluster ID to submit the job (must exist)
                - program_arg any additional argument to pass to the application  # noqa: E501
    :param client: boto3 client
    :param step_name: name for this EMR step
    :param wait_for_completion: whether waiting or not for the job to complete before returning  # noqa: E501
    :return: None
    """
    _PACKAGES = param['packs']
    _SPARK_ARGS = param['spark_args']
    job_path = param['job_path']
    cluster_id = param['cluster_id']
    program_arg = param['program_arg']

    bash_command = "/usr/bin/spark-submit {pkgs} {spark} {job} {args}".format(job=job_path,  # noqa: E501
                                                                              spark=_SPARK_ARGS,  # noqa: E501
                                                                              pkgs=_PACKAGES,  # noqa: E501
                                                                              args=program_arg)  # noqa: E501
    run_spark_bash_commands(bash_commands=[bash_command],
                            client=client,
                            cluster_id=cluster_id,
                            step_names=[step_name],
                            wait_for_completion=wait_for_completion)


def _wait_waiter_cluster(waiter, cluster_id):
    """
    :param waiter: A dictionary that provides parameters to control waiting behavior.
                    Delay - The amount of time in seconds to wait between attempts.
                    MaxAttempts - The maximum number of attempts to be made
    :param cluster_id: EMR cluster id
    :return: None if successful OR WaiterException
    """
    return waiter.wait(
        ClusterId=cluster_id,
        WaiterConfig={
            "Delay": 180,
            "MaxAttempts": 10
        }
    )


def get_cluster_status(client, cluster_id, waiter_param):
    waiter = client.get_waiter(waiter_param)
    status = _wait_waiter_cluster(waiter, cluster_id)
    to_return = {}
    if status is None:
        to_return['result'] = 'cluster_is_available'
    else:
        to_return['result'] = status
    return to_return


def await_for_cluster_creation(cluster_id, client):
    """
    :param cluster_id: EMR cluster id
    :param client: EMR client
    :return: status of the cluster. cluster_is_available OR Exception
    """
    return get_cluster_status(client, cluster_id, 'cluster_running')


def get_emr_config(config):
    emr_config_fields = ['Name',
                         'LogUri',
                         'AdditionalInfo',
                         'AmiVersion',
                         'ReleaseLabel',
                         'Instances',
                         'Steps',
                         'BootstrapActions',
                         'SupportedProducts',
                         'NewSupportedProducts',
                         'Applications',
                         'Configurations',
                         'VisibleToAllUsers',
                         'JobFlowRole',
                         'ServiceRole',
                         'Tags',
                         'SecurityConfiguration',
                         'AutoScalingRole',
                         'ScaleDownBehavior',
                         'CustomAmiId',
                         'EbsRootVolumeSize',
                         'RepoUpgradeOnBoot',
                         'KerberosAttributes',
                         'StepConcurrencyLevel']

    name_config = pop('Name')
    config['Name'] = get_emr_cluster_name(**name_config)

    config['Tags'] = add_or_replace_emr_tag(config['Tags'], 'Name', config['Name'])  # noqa: E501

    ret = {}
    for k in config:
        if k in emr_config_fields:
            ret[k] = config[k]
    return ret


def is_available_state(state):
    # Todo: Unit test
    if state in ['WAITING', 'RUNNING']:
        return True
    return False


def is_terminating_state(state):
    # Todo: Unit test
    if state in ['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS']:
        return True
    return False


def give_up(err):
    return not 'Rate exceeded' in str(err)


@backoff.on_exception(backoff.expo,
                      tuple([ClientError]),
                      max_tries=MAX_RETRIES_FOR_BACKOFF_EXCEPTIONS,
                      giveup=give_up)
def run_job_flow(client, **kwargs):
    # Todo: Unit test
    LOG.info('will create cluster')
    response = client.run_job_flow(**kwargs)
    if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
        raise Exception('run_job_flow() request failed: %s' % response)
    cluster_id = response['JobFlowId']
    LOG.info('created cluster %s', cluster_id)
    return cluster_id


def describe_cluster(client, cluster_id):
    return safely(f=lambda: client.describe_cluster(ClusterId=cluster_id),
                  max_tries=3, back_off=30, max_back_off=60, max_variance_pct=1.0)  # noqa: E501


def get_cluster_state(client, cluster_id):
    return describe_cluster(client, cluster_id)['Cluster']['Status']['State']


def terminate_cluster(client, cluster_id):
    state = get_cluster_state(client, cluster_id)
    if is_terminating_state(state):
        return False
    client.terminate_job_flows(JobFlowIds=[cluster_id])
    LOG.info('terminated cluster %s', cluster_id)
    return True

def _create_emr_step_bash(bash_command, step_name, jar, action_on_failure):
    step_args = []
    for _step_arg_ in bash_command.replace("\n", "").strip().split(" "):
        if _step_arg_ == "":
            continue
        step_args.append(_step_arg_)
        step = {
            "Name": step_name,
            'ActionOnFailure': action_on_failure,
            'HadoopJarStep': {
                'Jar': jar,
                'Args': step_args
            }
        }
    return step


def create_emr_steps_bash(bash_commands, step_names=[], jars=[], action_on_failure='TERMINATE_CLUSTER'):
    print("Running bash command: {}".format(bash_commands))
    DEFAULT_STEP_NAME = 'nameless_step'
    DEFAULT_JAR = 'command-runner.jar'
    if not isinstance(bash_commands, list):
        bash_commands = [bash_commands]
    if len(step_names) != len(bash_commands):
        step_names = [DEFAULT_STEP_NAME]*len(bash_commands)
    if len(jars) != len(bash_commands):
        jars = [DEFAULT_JAR]*len(bash_commands)

    steps = [_create_emr_step_bash(c, s, j, action_on_failure) for c, s, j in zip(bash_commands, step_names, jars)]
    return steps


def list_clusters(client, **kwargs):
    # Todo: Unit test
    # aws emr list-clusters
    #   --profile PROFILE_NAME
    #   --query 'Clusters[?Status.State!=`TERMINATED`
    #            && Status.State!=`TERMINATED_WITH_ERRORS`].[Status.State, Id, Name]'  # noqa: E501
    return client.list_clusters(**kwargs)


def get_job_flow_step(executable="spark-submit",
                      configuration=None,
                      job_file_path=None,
                      job_arguments=None,
                      step_name="nameless_step",
                      jar='command-runner.jar'):
    # Todo: Unit test
    step_args = [executable]
    if configuration:
        step_args += configuration

    if job_file_path:
        step_args += [job_file_path]

    if job_arguments:
        step_args += job_arguments

    step = {"Name": step_name,
            # could this come from the job / spark config? OR prettify the job_file_path?   # noqa: E501
            'ActionOnFailure': 'CONTINUE',  # could this come from the job / spark config ?  # noqa: E501
            'HadoopJarStep': {
                'Jar': jar,
                'Args': step_args
            }
            }
    return step


def get_spark_arguments(spark_configuration):
    return ["--spark-conf", spark_configuration]


def get_spark_job_arguments(emr_key,
                            emr_version,
                            emr_environment,
                            spark_key,
                            spark_version,
                            spark_environment,
                            sql_key,
                            sql_version,
                            sql_environment,
                            sql_parameters=None,
                            job_log_level=None):
    step_args = []
    step_args += ["--emr-key", emr_key]
    step_args += ["--emr-version", emr_version]
    step_args += ["--emr-environment", emr_environment]

    step_args += ["--spark-key", spark_key]
    step_args += ["--spark-version", spark_version]
    step_args += ["--spark-environment", spark_environment]

    step_args += ["--sql-key", sql_key]
    step_args += ["--sql-version", sql_version]
    step_args += ["--sql-environment", sql_environment]

    if sql_parameters:
        step_args += ["--sql-parameters", sql_parameters]

    if job_log_level:
        step_args += ["--log-level", job_log_level]

    return step_args


def add_job_flow_steps(client, cluster_id, steps):
    """
    :param client: boto3 client
    :param cluster_id: Cluster ID
    :param steps: EMR steps (as a list of just one dict) to add to the job flow
    :return: EMR steps ID which can be used to track the steps, please see 'check_steps_execution'  # noqa: E501
    """
    response = client.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
    if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
        raise Exception('add_job_flow_steps() request failed: %s' % response)
    step_ids = response['StepIds']
    LOG.info('Added steps: %s', step_ids)
    return step_ids


def check_steps_execution(client, cluster_id, steps):
    for step in steps:
        get_step_status(client, cluster_id, step)


def _wait_waiter_steps(waiter, cluster_id, step_id):
    """
    :param waiter: A dictionary that provides parameters to control waiting behavior.
                    Delay - The amount of time in seconds to wait between attempts.
                    MaxAttempts - The maximum number of attempts to be made
    :param cluster_id: EMR cluster id
    :param step_id: Step(s) identifier, which we are trying to execute on that cluster
    :return: None if successful OR WaiterException
    """
    return waiter.wait(
        ClusterId=cluster_id,
        StepId=step_id,
        WaiterConfig={
            "Delay": 120,
            "MaxAttempts": 360
        }
    )


@backoff.on_exception(backoff.expo,
                      tuple([ClientError, WaiterError]),  # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
                      max_tries=MAX_RETRIES_FOR_BACKOFF_EXCEPTIONS,
                      giveup=give_up)
def get_step_status(client, cluster_id, step_id):
    """
    :param client: boto3 client object
    :param cluster_id: EMR cluster id
    :param step_id: Steps we are trying to execute on that cluster
    :return: Waiter status
    """

    waiter = client.get_waiter("step_complete")
    return _wait_waiter_steps(waiter, cluster_id, step_id)