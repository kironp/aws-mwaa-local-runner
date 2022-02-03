from airflow.providers.amazon.aws.hooks.s3 import S3Hook as OrigHook


class S3Hook(OrigHook):

    def delete_under_prefix(self,
                            bucket,
                            prefix):
        bucket = self.get_bucket(bucket)
        response = bucket.objects.filter(Prefix=prefix).delete()
        return response
