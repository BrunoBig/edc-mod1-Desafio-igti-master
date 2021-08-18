import boto3
import os
import config

aws_access_key_id=config.aws_access_key_id
aws_secret_access_key=config.aws_secret_access_key
 
def upload_files(path):
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='sa-east-1'
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket('igti-bootcamp-ed-2021-468906988086')

 
    for subdir, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                print(full_path[len(path)+1:])
                bucket.put_object(Key='raw/'+full_path[len(path)+1:], Body=data)
                    
 
if __name__ == "__main__":
    upload_files('./Data/microdados_educacao_basica_2020/DADOS')