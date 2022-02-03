from math import floor
from copy import deepcopy
from emr import (
    emr_template,
    # instance_fleet_template,
    emr_master_instance_config,
    instance_fleet_template_master,
    instance_fleet_template_core
)
from orderlines_common import (
    JOB_BUCKET,
    EMR_SERVICE_ROLE,
    EMR_JOBFLOW_ROLE
)


class PipelineMetaData:
    def __init__(self,
                 project,
                 stage,
                 source,
                 spark_job_name=None,
                 spark_job_version=None,
                 pipelines=None,
                 output_tables=None,
                 output_database=None,
                 emr_step_concurrency=1,
                 emr_release_label='emr-6.3.0',
                 emr_core_instance_config={},
                 emr_core_instance_count=1,
                 emr_additional_config={},
                 spark_instance_num_cores=16,
                 spark_instance_memory_mb=32768,
                 spark_additional_properties={},
                 pipeline_additional_args=[''],
                 tc_env='dev'
                 ):
        self.project = project
        self.stage = stage
        self.source = source
        self.spark_job_name = spark_job_name
        self.spark_job_version = spark_job_version
        self.pipelines = pipelines
        self.output_tables = output_tables
        self.output_database = output_database
        self.emr_step_concurrency = emr_step_concurrency
        self.emr_release_label = emr_release_label
        self.emr_core_instance_config = emr_core_instance_config
        self.emr_core_instance_count = emr_core_instance_count
        self.emr_additional_config = emr_additional_config
        self.spark_instance_num_cores = spark_instance_num_cores
        self.spark_instance_memory_mb = spark_instance_memory_mb
        self.spark_additional_properties = spark_additional_properties
        self.pipeline_additional_args = pipeline_additional_args
        self.tc_env = tc_env
        self.job_bucket = JOB_BUCKET[tc_env]
        # self.emr_config = self.generate_emr_config()

    def generate_emr_config(self):
        emr_conf = deepcopy(emr_template)
        emr_conf['Instances']['InstanceFleets'] = [
            self._generate_emr_master_config(),
            self._generate_emr_core_config()
        ]
        emr_conf['Instances']['KeepJobFlowAliveWhenNoSteps'] = False
        emr_conf['Configurations'] = self._generate_emr_configuration()
        emr_conf['BootstrapActions'] = [
            {
                'ScriptBootstrapAction': {
                    'Path': f's3://{self.job_bucket}/bi_team/infra-bi-assets/'
                            f'bootstraps/bi_bootstrap_pex.sh',
                    'Args': [self.tc_env]
                },
                'Name': 'Main_Bootstrap'
            }
        ]
        emr_conf['LogUri'] = f"s3n://{self.job_bucket}/bi_team/logs/emr/{self.spark_job_name}" \
                             f"/{self.spark_job_version}/"
        emr_conf['StepConcurrencyLevel'] = self.emr_step_concurrency
        emr_conf['Name'] = f"da1-{self.tc_env}-{self.project}-{self.source}-{self.stage}" + "-{{ ds }}-bi"
        emr_conf['ReleaseLabel'] = self.emr_release_label
        emr_conf['ServiceRole'] = EMR_SERVICE_ROLE[self.tc_env]
        emr_conf['JobFlowRole'] = EMR_JOBFLOW_ROLE[self.tc_env]
        emr_conf['Steps'] = self._generate_step_config()
        return emr_conf

    def _generate_step_config(self):
        return [
            {
                'Name': pipeline,
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': f"s3://{self.job_bucket}/bi_team/infra-bi-assets/jars/"
                           f"script-runner.jar",
                    'Args': [
                                f"s3://{self.job_bucket}/bi_team/infra-bi-assets/scripts/"
                                f"pex-executer.sh",
                                f"s3://{self.job_bucket}/bi_team/deployment/{self.spark_job_name}/"
                                f"{self.spark_job_version}/{self.spark_job_name}.pex",
                                "HADOOP_HOME=/usr/lib/hadoop",
                                "SPARK_HOME=/usr/lib/spark",
                                f"TC_ENV={self.tc_env}",
                                f"./{self.spark_job_name}.pex", '-m', 'job',
                                '--pipeline-to-run', pipeline
                            ] + self.pipeline_additional_args}
            } for pipeline in self.pipelines
        ]

    def _generate_emr_master_config(self):
        master = instance_fleet_template_master.copy()
        master['InstanceTypeConfigs'] = emr_master_instance_config
        return master

    def _generate_emr_core_config(self):
        core = instance_fleet_template_core.copy()
        core['TargetSpotCapacity'] = self.emr_core_instance_count
        core['InstanceTypeConfigs'] = self.emr_core_instance_config
        return core

    def _generate_emr_configuration(self):
        conf = [
            {
                'Properties': {
                    'hive.metastore.client.factory.class':
                        'com.amazonaws.glue.catalog.metastore.'
                        'AWSGlueDataCatalogHiveClientFactory'},
                'Classification': 'spark-hive-site'
            },
            {
                'Classification': 'spark-env',
                'Configurations':
                    [
                        {'Classification': 'export',
                         'Properties': {'TC_ENV': self.tc_env}}
                    ]
            },
            {
                "Classification": "spark-defaults",
                "Properties": {**self._spark_settings()}
            }
        ]
        if len(self.emr_additional_config) > 0:
            conf.append(self.emr_additional_config)

        return conf

    def _spark_settings(self):

        executor_num_cores = 5  # Considered as a good compromise for HDFS throughput and parallelism
        executors_per_instance = (self.spark_instance_num_cores - 1) // executor_num_cores
        total_executor_count = self.emr_core_instance_count * executors_per_instance
        executor_memory_mb = floor((self.spark_instance_memory_mb - 1000) * 0.9 / executors_per_instance)
        shuffle_partitions = 3 * self.emr_core_instance_count * (self.spark_instance_num_cores - 1)

        spark_config = {
            "spark.executor.memory": str(executor_memory_mb) + "M",
            "spark.executor.cores": str(executor_num_cores),
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
            "spark.driver.memory": "5g",
            "spark.driver.maxResultSize": "3g",
            "spark.jars.packages": "ch.cern.sparkmeasure:spark-measure_2.12:0.17",
            "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "LEGACY",
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",
            "spark.sql.legacy.parquet.int96RebaseModeInRead": "LEGACY",
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY"
        }

        if self.emr_step_concurrency > 1:
            initial_executor_count = floor(total_executor_count // self.emr_step_concurrency)  # Executors per parallel step
            spark_update(
                {
                    "spark.dynamicAllocation.enabled": "true",
                    "spark.dynamicAllocation.initialExecutors": str(initial_executor_count),
                    "spark.dynamicAllocation.minExecutors": "1",
                    "spark.dynamicAllocation.maxExecutors": str(total_executor_count),
                    "spark.driver.cores": "2",
                }
            )
        else:
            executor_count = self.emr_core_instance_count * executors_per_instance + 1  # +1 because driver executor is included in count.
            spark_update(
                {
                    "spark.executor.instances": str(executor_count),
                    "spark.dynamicAllocation.enabled": "false"
                }
            )

        # Add and override with explicitly defined spark properties
        spark_update(self.spark_additional_properties)
        return spark_config
