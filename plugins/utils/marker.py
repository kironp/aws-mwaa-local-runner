from os.path import join

from utils.base_markers import AbstractMarker
from utils.marker_set import S3PipelineMarkerMixin


class S3Marker(S3PipelineMarkerMixin, AbstractMarker):

    @property
    def key(self):
        return join(self.prefix,
                    self.marker)

    @property
    def prefix(self):
        if self.execution_date:
            r = join(super().prefix, 'execution_date=' + str(self.execution_date))
            return r
        return super().prefix

    def write(self):
        hook = self._get_hook()
        hook.load_string(string_data='',
                         bucket_name=self.bucket,
                         key=self.key)

    def delete(self):
        hook = self._get_hook()
        response = hook.delete_objects(bucket=self.bucket,
                                       keys=self.key)
        return response

    @property
    def exists(self):
        hook = self._get_hook()
        return hook.check_for_key(self.key, self.bucket)

    def __repr__(self):
        return str(join('s3://', self.bucket, self.key))
