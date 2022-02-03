from os.path import join

from hooks.s3_hook import S3Hook
from utils.base_markers import AbstractPipelineMarker, AbstractMarkerSet


class S3PipelineMarkerMixin(AbstractPipelineMarker):

    def __init__(self,
                 team,
                 type,
                 pipeline_stage,
                 pipeline_data_source,
                 pipeline_name,
                 bucket,
                 aws_conn_id=None):
        super().__init__(team,
                         type,
                         pipeline_stage,
                         pipeline_data_source,
                         pipeline_name)
        self.bucket = bucket
        self.aws_conn_id = aws_conn_id

    def _get_hook(self):
        return S3Hook(aws_conn_id=self.aws_conn_id)

    @property
    def prefix(self):
        return join(self.team,
                    self.type,
                    self.pipeline_stage,
                    self.data_source,
                    self.pipeline_name)


class S3MarkerSet(S3PipelineMarkerMixin, AbstractMarkerSet):
    def delete_all(self):
        hook = self._get_hook()
        hook.delete_under_prefix(self.bucket, self.prefix)

    def __repr__(self):
        return str(join('s3://', self.bucket, self.prefix))
