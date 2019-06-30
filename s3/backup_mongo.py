#!usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: solinari
@file: backup_mongo.py
@time: 2019/03/02
"""
import os
import tarfile
import datetime
from boto3.session import Session
import boto3


MONGO_DUMP = "/home/solinari/mongodump"
TAR_FILE = "/home/solinari/tartest.tar.gz"
ACCESS_KEY = 'AKIAJUL5J2M422GJ26QQ'
SECRET_KEY = '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'
BUCKET = 'stockcrawler'
REGION = 'ap-northeast-2'
RETRY_TIME = 50


def make_tar():
    tar = tarfile.open(TAR_FILE, "w:gz")

    for root, dir, files in os.walk(MONGO_DUMP):
        for file in files:
            fullpath = os.path.join(root, file)
            tar.add(fullpath)
    tar.close()


def s3_upload(upload_file="test"):
    session = Session(aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY,
                      region_name=REGION)
    s3 = session.resource('s3')
    client = session.client('s3')

    upload_data = open(TAR_FILE, 'rb')
    file_obj = s3.Bucket(BUCKET).put_object(Key=upload_file, Body=upload_data)


# main
make_tar()
today = datetime.datetime.today().strftime('%Y_%m_%d')
filename = "mongodump_" + today
for i in range(0, RETRY_TIME):
    try:
        s3_upload(upload_file=filename)
        break
    except:
        continue
