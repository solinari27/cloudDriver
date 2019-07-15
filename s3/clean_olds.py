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
        args['MONGO_DUMP'] = config['DUMP']['DST_PATH']
        args['TAR_FILE'] = config['DUMP']['TMP_FILE']
        args['RETRY_TIME'] = config['CONNECTION']['RETRY_TIME']

    return args


def main(argv):
    """
        main program of pull mongo
    """
    curprocess = argv[0]
    args = parseConfig(curprocess)
    print args


if __name__ == '__main__':
    main(sys.argv)
