#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   backup_mongo.py
@Time    :   2019/07/14 20:15:06
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
        args['MONGO_DUMP'] = config['DUMP']['DST_PATH']
        args['TAR_FILE'] = config['DUMP']['TMP_FILE']
        args['RETRY_TIME'] = config['CONNECTION']['RETRY_TIME']

    return args


def make_tar(args):
    """
    make mongo db data into tar file
    """
    TAR_FILE = args['TAR_FILE']
    MONGO_DUMP = args['MONGO_DUMP']
    tar = tarfile.open(TAR_FILE, "w:gz")

    for root, dir, files in os.walk(MONGO_DUMP):
        for file in files:
            fullpath = os.path.join(root, file)
            tar.add(fullpath)
    tar.close()


def s3_upload(upload_file="test"， args):
    """
    upload tar file onto s3 service
    """
    REGION = args['REGION']
    BUCKET = args['BUCKET']
    ACCESS_KEY = base64.b64decode(args['ACCESS_KEY'])
    SECRET_KEY = base64.b64decode(args['SECRET_KEY'])
    TAR_FILE = args['TAR_FILE']

    session = Session(aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY,
                      region_name=REGION)
    s3 = session.resource('s3')
    client = session.client('s3')

    upload_data = open(TAR_FILE, 'rb')
    file_obj = s3.Bucket(BUCKET).put_object(Key=upload_file, Body=upload_data)


def main(argv):
    """
        main program of pull mongo
    """
    curprocess = argv[0]
    args = parseConfig(curprocess)
    RETRY_TIME = args['RETRY_TIME']

    make_tar(args)
    today = datetime.datetime.today().strftime('%Y_%m_%d')
    filename = "mongodump_" + today
    for i in xrange(RETRY_TIME):
        try:
            s3_upload(upload_file=filename, args=args)
            break
        except:
            continue


if __name__ == '__main__':
    main(sys.argv)
