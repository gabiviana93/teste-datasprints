#!/usr/bin/env python
# coding: utf-8

import boto3
import boto
import os
import re
from sagemaker import get_execution_role
from pathlib import Path

role = get_execution_role()
region = boto3.Session().region_name
s3 = boto3.resource('s3')

bucket='projeto-datasprints'
b = s3.Bucket(bucket)

def saveFileToS3(filename):
    key = "{}".format(filename)
    url = 's3://{}/{}'.format(bucket, key)
    boto3.Session().resource('s3').Bucket(bucket).Object(key).upload_file(filename)
    print('Arquivo adicionado ao bucket {}'.format(url))

def saveDirectoryToS3(path):
    data_dir = Path(path)
    for csv_file in data_dir.glob('*.csv'):
        saveFileToS3(str(csv_file))
        
def getFilesFromS3():
    b = s3.Bucket(bucket)
    objects =  b.objects.all()
    return objects

def getFileUrls():
    urls = []
    s3 = boto3.resource('s3')
    for file in b.objects.all():
        urls.append(file.key)
    return urls