from abc import ABC, abstractmethod

MARKER = 'JOB_COMPLETE'


class AbstractPipelineMarker(ABC):

    @abstractmethod
    def __init__(self,
                 team,
                 type,
                 pipeline_stage,
                 pipeline_data_source,
                 pipeline_name):
        self.team = team
        self.type = type
        self.pipeline_stage = pipeline_stage
        self.data_source = pipeline_data_source
        self.pipeline_name = pipeline_name


class AbstractMarkerSet(AbstractPipelineMarker):
    @abstractmethod
    def delete_all(self):
        pass


class AbstractMarker(AbstractPipelineMarker):

    @abstractmethod
    def __init__(self,
                 team,
                 type,
                 pipeline_stage,
                 pipeline_data_source,
                 pipeline_name,
                 execution_date=None,
                 marker=MARKER):
        super().__init__(team,
                         type,
                         pipeline_stage,
                         pipeline_data_source,
                         pipeline_name)
        self.execution_date = execution_date
        self.marker = marker

    @abstractmethod
    def write(self):
        pass

    @abstractmethod
    def delete(self):
        pass
