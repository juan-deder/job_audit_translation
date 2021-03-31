import os
import boto3


class Config:
    ps = None

    def __init__(self):
        if not os.environ.get('ENV'):
            self.ps = boto3.client('ssm')

    def __getattr__(self, key):
        return self.ps.get_parameter(Name=key)['Parameter']['Value'] if self.ps\
            else os.getenv(key)
