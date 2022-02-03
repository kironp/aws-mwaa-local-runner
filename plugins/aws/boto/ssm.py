import boto3


def get_ssm_value(name):
    client = boto3.client('ssm', 'eu-west-1')
    response = client.get_parameter(Name=name)
    return response['Parameter']['Value']
