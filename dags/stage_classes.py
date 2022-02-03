from __future__ import annotations
from abc import ABCMeta
from functools import partial, wraps, reduce
from typing import List
from airflow.exceptions import AirflowException
from airflow.models import DAG, BaseOperator
from airflow.operators.dummy import DummyOperator
import logging
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
# from airflow.providers.amazon.aws.operators.emr import  EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.utils.task_group import TaskGroup
from orderlines_sensors import (
     get_glue_partition_sensors
 )
from pipeline_metadata import PipelineMetaData
from sensor_metadata import SensorMetaData
from airflow.operators.python import PythonOperator
from orderlines_common import (
    repair_table_callable,
    repair_table_callable_no_indexing,
    Stage
)

LOG = logging.getLogger(__name__)

# This work around is required because:
# You cannot run an EMR with concurrency > 1 AND
# set step_action_on_failure = 'TERMINATE_CLUSTER'
# instead you must set step_action_on_failure = 'CONTINUE'
# So the cluster terminates normally when a step fails.
# This patching fixes this.


def additional_poke_check(f):
    @wraps(f)
    def func(self, context):

        response = self.get_emr_response()
        if not response['ResponseMetadata'][
                   'HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False

        state = self.state_from_response(response)
        msg = self.failure_message_from_response(response)

        if state == 'TERMINATED' and 'completed with errors' in msg:
            raise AirflowException(msg)
        return f(self, context)
    return func


EmrJobFlowSensor.poke = additional_poke_check(
    EmrJobFlowSensor.poke
)

class EtlStage(TaskGroup):

    def __init__(self,
                 pipeline_metadata,
                 table_repairs: List[partial] = None,
                 dependencies: List[EtlStage] = [],
                 *args,
                 **kwargs):
        super().__init__(
            group_id=f"{pipeline_metadata.source}-{pipeline_metadata.stage}",
            *args,
            **kwargs)
        self.table_repairs = table_repairs
        self.dependencies = dependencies
        self.pipeline_metadata = pipeline_metadata
        self.source = pipeline_metadata.source
        self.stage = pipeline_metadata.stage
        self.project = pipeline_metadata.project
        self.build_stage()
        self.dependency_memo = [self]

    def build_stage(self):
        with self:
            emr_operators = self._create_emr_operators()
            repair_tables = self._create_repair_operators()

            emr_operators >> repair_tables

    def _create_emr_operators(self):
        with TaskGroup('emr_operators') as emr_operators:
            launch_emr = DummyOperator(
                # dag=self.dag,
                task_id=f"submit_spark_job_{self.project}_{self.source}_{self.stage}"
            )
            # launch_emr = EmrCreateJobFlowOperator(
            #     dag=self.dag,
            #     task_id=f"submit_spark_job_{self.project}_{self.source}_{self.stage}",
            #     job_flow_overrides=self.pipeline_metadata.generate_emr_config(),
            #     region_name='eu-west-1'
            # )
            daily_sensor = DummyOperator(
                dag=self.dag,
                task_id=f"sense_{launch_emr.task_id}"
            )


            launch_emr >> daily_sensor
        return emr_operators

    # def _create_emr_operators(self):

    #
    #         emr_operators = []
    #
    #         if self.pipeline_metadata.pipelines is not None:
    #             launch_emr = EmrCreateJobFlowOperator(
    #                 dag=self.dag,
    #                 task_id=f"submit_spark_job_{self.project}_{self.source}_{self.stage}",
    #                 job_flow_overrides=self.pipeline_metadata.generate_emr_config(),
    #                 region_name='eu-west-1'
    #             )
    #             emr_operators.append(launch_emr)
    #
    #             daily_sensor = EmrJobFlowSensor(
    #                 dag=self.dag,
    #                 task_id=f"sense_{launch_emr.task_id}",
    #                 mode='reschedule',
    #                 job_flow_id="{{ task_instance.xcom_pull('" + launch_emr.task_id + "', key='return_value') }}"
    #             )
    #             emr_operators.append(daily_sensor)
    #
    #             end = DummyOperator(
    #                 dag=self.dag,
    #                 task_id=f'end_{launch_emr.task_id}'
    #             )
    #             emr_operators.append(end)
    #
    #         return emr_operators

    def _create_repair_operators(self):
        with TaskGroup('repair_tables') as repair_tables:
            for table in self.pipeline_metadata.output_tables:
                DummyOperator(
                    # dag=self.dag,
                    task_id=f"repair_{table}"
                )
        return repair_tables

    def recursive_dependencies(self, dag_parts, start_root=None):
        dep_list = []
        new_dep = []
        for dag_part in dag_parts:
            if len(dag_part.dependencies) == 0 and start_root:
                dep_list.append((start_root, dag_part))
            else:
                for dep in dag_part.dependencies:
                    if dep not in self.dependency_memo:
                        # dep.dag = self.dag
                        self.dependency_memo.append(dep)
                        new_dep.append(dep)
                    dep_list.append((dep, dag_part))
        if len(new_dep) == 0:
            return dep_list
        else:
            return dep_list + self.recursive_dependencies(
                dag_parts=new_dep,
                start_root=start_root)

    def propagate_dependencies(self, start_root=None):
        d = self.recursive_dependencies(dag_parts=[self],
                                        start_root=start_root)
        for i in d:
            upstream, downstream = i[0], i[1]
            upstream >> downstream



#
# class PartialDag(metaclass=ABCMeta):
#
#     def __init__(self, project: str, stage: str, source: str, dag: DAG = None):
#         self.stage_id = self._stage_id(project, source, stage)
#         self._dag = dag
#         self._start = None
#         self._end = None
#
#     @property
#     def start(self):
#         if not self._start and self.dag:
#             self._start = DummyOperator(dag=self.dag, task_id=f"{self.stage_id}-start")
#         return self._start
#
#     @property
#     def end(self):
#         if not self._end and self.dag:
#             self._end = DummyOperator(dag=self.dag, task_id=f"{self.stage_id}-end")
#         return self._end
#
#     @property
#     def dag(self):
#         return self._dag
#
#     def __iter__(self):
#         # Enables BaseOperators to identify start_task and set downstream using __rshift__
#         start_task = [self.start]
#         return iter(start_task)
#
#     def __rshift__(self, other):
#         """Implements Task >> Task"""
#         if isinstance(other, PartialDag):
#             BaseOperator.__rshift__(self.end, other.start)
#             return other
#         else:
#             return BaseOperator.__rshift__(self.end, other)
#
#     def __lshift__(self, other):
#         """Implements Task << Task"""
#         if isinstance(other, PartialDag):
#             BaseOperator.__lshift__(self.start, other.end)
#             return other
#         return BaseOperator.__lshift__(self.start, other)
#
#     @staticmethod
#     def _stage_id(project, source, stage):
#         return f"{project}-{source}-{stage}"
#
#
# class EtlPartialDag(PartialDag):
#     def __init__(self,
#                  pipeline_metadata: PipelineMetaData,
#                  sensor_metadata: SensorMetaData = [],
#                  dag: DAG = None,
#                  dependencies: List[EtlPartialDag] = [],
#                  table_repairs: List[partial] = None,
#                  secondary_table_repair: partial = None):
#         super().__init__(dag=dag,
#                          project=pipeline_metadata.project,
#                          stage=pipeline_metadata.stage,
#                          source=pipeline_metadata.source)
#         self.table_repairs = table_repairs
#         self.secondary_table_repair = secondary_table_repair
#         self.dependencies = dependencies
#         self.sensors = []
#         self.project = pipeline_metadata.project
#         self.stage = pipeline_metadata.stage
#         self.source = pipeline_metadata.source
#         self.pipeline_metadata = pipeline_metadata
#         if not isinstance(sensor_metadata, list):
#             self.sensor_metadata = [sensor_metadata]
#         else:
#             self.sensor_metadata = sensor_metadata
#
#         if dag:
#             self.configure_dag_tasks()
#         self.dependency_memo = [self]
#
#     @PartialDag.dag.setter
#     def dag(self, d):
#         if not isinstance(d, DAG):
#             raise TypeError(f"{d} is not of type DAG")
#         self._dag = d
#         self.configure_dag_tasks()
#
#     def configure_dag_tasks(self):
#         self.dag_tasks = []
#         if self.dag:
#             self.dag_tasks.append(self.start)
#             self.dag_tasks.extend(self._create_input_sensors())
#             self.dag_tasks.extend(self._create_emr_operators())
#             self.dag_tasks.append(self._create_repair_operators())
#             self.append_secondary_table_repair(self.secondary_table_repair)
#             self.dag_tasks.append(self.end)
#             reduce(lambda x, y: x >> y,
#                    [i for i in self.dag_tasks if i not in [None, []]])
#         else:
#             raise Exception('DAG not set, unable to configure PartialDag')
#
#     def _create_input_sensors(self):
#
#         sensors = []
#
#         if self.sensor_metadata:
#             for metadata in self.sensor_metadata:
#                 if metadata:
#                     l_ = get_glue_partition_sensors(
#                         partition_set=metadata.partition_set,
#                         dag=self._dag
#                     )
#                     sensors += l_
#
#         return sensors
#
#     def _create_emr_operators(self):
#
#         emr_operators = []
#
#         if self.pipeline_metadata.pipelines is not None:
#             launch_emr = EmrCreateJobFlowOperator(
#                 dag=self.dag,
#                 task_id=f"submit_spark_job_{self.project}_{self.source}_{self.stage}",
#                 job_flow_overrides=self.pipeline_metadata.generate_emr_config(),
#                 region_name='eu-west-1'
#             )
#             emr_operators.append(launch_emr)
#
#             daily_sensor = EmrJobFlowSensor(
#                 dag=self.dag,
#                 task_id=f"sense_{launch_emr.task_id}",
#                 mode='reschedule',
#                 job_flow_id="{{ task_instance.xcom_pull('" + launch_emr.task_id + "', key='return_value') }}"
#             )
#             emr_operators.append(daily_sensor)
#
#             end = DummyOperator(
#                 dag=self.dag,
#                 task_id=f'end_{launch_emr.task_id}'
#             )
#             emr_operators.append(end)
#
#         return emr_operators
#
#     def _create_repair_operators(self):
#         repairs = []
#         if self.table_repairs:
#             # Old method, supplying list of partials - Deprecate ASAP
#             for table in self.table_repairs:
#                 repairs.append(table(dag=self.dag))
#         elif self.pipeline_metadata.output_tables is not None:
#             # New method
#             for table in self.pipeline_metadata.output_tables:
#                 if self.pipeline_metadata.stage == Stage.HISTORY:
#                     repairs.append(
#                         PythonOperator(
#                             dag=self.dag,
#                             task_id=f"repair_{table}",
#                             python_callable=repair_table_callable_no_indexing,
#                             op_kwargs={
#                                 'aws_region': 'eu-west-1',
#                                 'db_name':
#                                     self.pipeline_metadata.output_database,
#                                 'table_name': table
#                             }
#                         )
#                     )
#                 else:
#                     repairs.append(
#                         PythonOperator(
#                             dag=self.dag,
#                             task_id=f"repair_{table}",
#                             python_callable=repair_table_callable,
#                             op_kwargs={
#                                 'aws_region': 'eu-west-1',
#                                 'db_name':
#                                     self.pipeline_metadata.output_database,
#                                 'table_name': table
#                             }
#                         )
#                     )
#         return repairs
#
#
#     def append_secondary_table_repair(self, secondary_table_repair):
#         """Use appropriate inputs to generate fully provisioned full-table-repair operator"""
#         if self.secondary_table_repair:
#             self.dag_tasks.append(secondary_table_repair(dag=self.dag))
#
#     def recursive_dependencies(self, dag_parts, start_root=None):
#         dep_list = []
#         new_dep = []
#         for dag_part in dag_parts:
#             if len(dag_part.dependencies) == 0 and start_root:
#                 dep_list.append((start_root, dag_part))
#             else:
#                 for dep in dag_part.dependencies:
#                     if dep not in self.dependency_memo:
#                         dep.dag = self.dag
#                         self.dependency_memo.append(dep)
#                         new_dep.append(dep)
#                     dep_list.append((dep, dag_part))
#         if len(new_dep) == 0:
#             return dep_list
#         else:
#             return dep_list + self.recursive_dependencies(
#                 dag_parts=new_dep,
#                 start_root=start_root)
#
#     def propagate_dependencies(self, start_root=None):
#         d = self.recursive_dependencies(dag_parts=[self],
#                                         start_root=start_root)
#         for i in d:
#             upstream, downstream = i[0], i[1]
#             upstream >> downstream
