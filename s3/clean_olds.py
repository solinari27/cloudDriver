#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   clean_olds.py
@Time    :   2019/07/15 22:06:31
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
    """
    parse config yaml to get the configuration arguments
    @param curprocess: curprocess abs path.
    this parms for location filesystem path.
    """
    curdir = os.path.dirname(curprocess)
    confile = os.path.join(curdir, 'config', 's3.yaml')
    args = dict()

    with open(confile) as f:
        config = yaml.safe_load(f)
        args['REGION'] = config['S3']['REGION']
        args['BUCKET'] = config['S3']['BUCKET']
        args['ACCESS_KEY'] = config['S3']['ACCESS_KEY']
        args['SECRET_KEY'] = config['S3']['SECRET_KEY']
        args['KEEP_S3_NUM'] = config['S3']['KEEP_S3_NUM']
        args['MONGO_DUMP'] = config['DUMP']['DST_PATH']
        args['TAR_FILE'] = config['DUMP']['TMP_FILE']
        args['RETRY_TIME'] = config['CONNECTION']['RETRY_TIME']

    return args


def clean_s3(args):
    REGION = args['REGION']
    BUCKET = args['BUCKET']
    ACCESS_KEY = base64.b64decode(args['ACCESS_KEY'])
    SECRET_KEY = base64.b64decode(args['SECRET_KEY'])
    MONGO_DUMP = args['MONGO_DUMP']
    TAR_FILE = args['TAR_FILE']
    RETRY_TIME = args['RETRY_TIME']
    KEEP_S3_NUM = args['KEEP_S3_NUM']

    session = Session(aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY,
                      region_name=REGION)
    s3 = session.resource('s3')
    client = session.client('s3')
    bucket_info = client.list_objects(Bucket=BUCKET)

    contents = bucket_info['Contents']
    date_list = list()
    for file in contents:
        date_list.append(file['LastModified'])
    date_list.sort(reverse=True)    # keep reverse order
    date_index = date_list[KEEP_S3_NUM-1]

    for file in contents:
        if file['LastModified'] < date_index:
            filename = file['Key']
            for LOOP in xrange(RETRY_TIME):
                try:
                    bucket.delete_objects(
                        Delete={
                            'Objects': [
                                {
                                    'Key': filename,
                                    'VersionId': 'string'
                                },
                            ]
                        }
                    )
                    print ('successfully delete file %s.', filename)
                except:
                    continue


def main(argv):
    """
        main program of pull mongo
    """
    curprocess = argv[0]
    args = parseConfig(curprocess)
    clean_s3(args=args)


if __name__ == '__main__':
    main(sys.argv)
