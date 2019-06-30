#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   pull_mongo.py
@Time    :   2019/07/10 20:26:20
@Author  :   Solinari
@Contact :   deeper1@163.com
@License :   (C)Copyright 2017-2018, GPLv3
'''

# here put the import lib
import sys
import os
import tarfile
import datetime
import base64
from boto3.session import Session
import boto3
import yaml


def parseConfig(curprocess):
    curdir = os.path.dirname(curprocess)
    confile = os.path.join(curdir, 'config', 's3.yaml')
    args = dict()

    with open(confile) as f:
        config = yaml.safe_load(f)
        args['REGION'] = config['S3']['REGION']
        args['BUCKET'] = config['S3']['BUCKET']
        args['ACCESS_KEY'] = config['S3']['ACCESS_KEY']
        args['SECRET_KEY'] = config['S3']['SECRET_KEY']
        args['MONGO_DUMP'] = config['DUMP']['DST_PATH']
        args['TAR_FILE'] = config['DUMP']['TMP_FILE']
        args['RETRY_TIME'] = config['CONNECTION']['RETRY_TIME']

    return args


def s3_download(args):
    REGION = args['REGION']
    BUCKET = args['BUCKET']
    ACCESS_KEY = base64.b64decode(args['ACCESS_KEY'])
    SECRET_KEY = base64.b64decode(args['SECRET_KEY'])
    MONGO_DUMP = args['MONGO_DUMP']
    TAR_FILE = args['TAR_FILE']
    RETRY_TIME = args['RETRY_TIME']

    session = Session(aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY,
                      region_name=REGION)
    s3 = session.resource('s3')
    client = session.client('s3')
    bucket_info = client.list_objects(Bucket=BUCKET)

    contents = bucket_info['Contents']
    day = contents[0]['LastModified']
    filename = contents[0]['Key']
    for file in contents:
        if file['LastModified'] > day:
            day = file['LastModified']
            filename = file['Key']
        print filename

    print ('start to download file %s form AWS S3.', filename)
    for LOOP in range(0, RETRY_TIME):
        try:
            client.download_file(BUCKET, filename, '/tmp/mongo_backup.tar.gz')
            print ('download finished.')
        except:
            continue

    if LOOP+1 == RETRY_TIME:
        print('download success.')
    else:
        print('download error.')


def main(argv):
    """
        main program of pull mongo
    """
    curprocess = argv[0]
    args = parseConfig(curprocess)
    s3_download(args)


if __name__ == '__main__':
    main(sys.argv)
