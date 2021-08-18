import boto3

BUCKET = 'igti-bootcamp-ed-2021-468906988086'
FOLDER = 'raw//'

s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=BUCKET, Prefix=FOLDER)

for page in pages:
    #for obj in page['Contents']:
        print (page)