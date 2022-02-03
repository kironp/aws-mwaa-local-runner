
class SensorMetaData:

    def __init__(self,
                 project: str,
                 source: str,
                 stage: str
                 ):
        self.project = project
        self.stage = stage
        self.source = source

    @property
    def partition_set(self):
        p = '_'.join([self.project,
                      self.source,
                      self.stage])
        return p
