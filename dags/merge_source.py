from typing import List

from .pipeline import Pipeline
from orderlines_common import Stage


class MergeSource:
    etl_stage = Stage.MERGE

    def __init__(self,
                 data_source: str,
                 backfill_min_date: str,
                 backfill_end_date: str,
                 input_db: str,
                 output_db: str,
                 pipelines: List[Pipeline],
                 ):
        self.data_source = data_source
        self.backfill_min_date = backfill_min_date
        self.backfill_end_date = backfill_end_date
        self.input_db = input_db
        self.output_db = output_db
        assert isinstance(pipelines, list)
        self.pipeline_data = pipelines

    def pipelines(self):
        return self.pipeline_data

    def glue_sensor_table_name(self,
                               pipeline):
        return pipeline.output_table

    def glue_sensor_expression(self, pipeline):
        return "execution_date='{{ ds }}' " \
               + "AND source_system = '{}'".format(self.data_source.upper())

    def glue_sensor_soft_fail(self, pipeline):
        return False

    def input_counter_sql(self,
                          pipeline):
        return pipeline.input_sql \
               % {"input_db": pipeline.input_db,
                  "oracle_mirror_db": pipeline.oracle_mirror_db,
                  "input_table": pipeline.input_table,
                  "backfill_min_date": self.backfill_min_date,
                  "backfill_end_date": self.backfill_end_date}

    def output_counter_sql(self, pipeline):
        return pipeline.output_sql \
               % {"output_db": pipeline.output_db,
                  "output_table": pipeline.output_table,
                  "backfill_min_date": self.backfill_min_date,
                  "backfill_end_date": self.backfill_end_date}

    def equality_tolerance(self, pipeline):
        return 0.0
