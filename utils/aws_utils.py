import boto3
import json
from botocore.exceptions import ClientError


def get_ssm_parameter(name):
    """
    Get SSM parameter, return None if not found
    :param name: (string) parameter name
    :return: [string, None] value of parameter
    """
    ssm = boto3.client('ssm')
    try:
        parameter = ssm.get_parameter(Name=name, WithDecryption=True).get('Parameter').get('Value')
    except ssm.exceptions.ParameterNotFound:
        return None
    return parameter


def set_ssm_parameter(name, value, parameter_type='String', overwrite=False):
    ssm = boto3.client('ssm')
    ssm.put_parameter(
        Name=name,
        Value=value,
        Type=parameter_type,
        Overwrite=overwrite
    )


def list_files_s3(bucket, prefix=''):
    """
    streaming the string written in the file without downloading the file
    :param bucket: bucket name
    :param prefix: sub folder in bucket
    :return: list of files in the folder
    """
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    return [object_summary.key for object_summary in my_bucket.objects.filter(Prefix=prefix)]


def download_file_s3(bucket, key, local_path):
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket, key, local_path)
    except ClientError:
        return None
    return local_path


def put_dict_to_s3(bucket, key, dict_obj):
    client = boto3.client('s3')
    client.put_object(Body=json.dumps(dict_obj).encode('utf-8'), Bucket=bucket, Key=key)


def read_file_s3(bucket, key):
    """
    Stream file from s3 into memory
    :param bucket: s3 bucket
    :param key: s3 key
    :return: stream object
    """
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        return None
    return obj['Body'].read().decode('utf-8')
