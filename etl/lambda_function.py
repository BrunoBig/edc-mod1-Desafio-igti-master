import boto3

def handler(event, context):
    """
    Lambda function that starts a job flow in EMR.
    """
    client = boto3.client('emr', region_name='sa-east-1')

    cluster_id = client.run_job_flow(
                Name='EMR-Ney-IGTI-delta',
                ServiceRole='EMR_DefaultRole',
                JobFlowRole='EMR_EC2_DefaultRole',
                VisibleToAllUsers=True,
                LogUri='s3://datalake-bruno-468906988086-igti-edc-tf/emr-logs',
                ReleaseLabel='emr-6.3.0',
                Instances={
                    'InstanceGroups': [
                        {
                            'Name': 'Master nodes',
                            'Market': 'SPOT',
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1,
                        },
                        {
                            'Name': 'Worker nodes',
                            'Market': 'SPOT',
                            'InstanceRole': 'CORE',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1,
                        }
                    ],
                    'Ec2KeyName': 'BRUNO-KEY-IGTI',
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                    'Ec2SubnetId': 'subnet-a04538e9'
                },

                Applications=[
                    {'Name': 'Spark'},
                    {'Name': 'Hive'},
                    {'Name': 'Pig'},
                    {'Name': 'Hue'},
                    {'Name': 'JupyterHub'},
                    {'Name': 'JupyterEnterpriseGateway'},
                    {'Name': 'Livy'},
                ],

                Configurations=[{
                    "Classification": "spark-env",
                    "Properties": {},
                    "Configurations": [{
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3",
                            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                        }
                    }]
                },
                    {
                        "Classification": "spark-hive-site",
                        "Properties": {
                            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                        }
                    },
                    {
                        "Classification": "spark-defaults",
                        "Properties": {
                            "spark.submit.deployMode": "cluster",
                            "spark.speculation": "false",
                            "spark.sql.adaptive.enabled": "true",
                            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                        }
                    },
                    {
                        "Classification": "spark",
                        "Properties": {
                            "maximizeResourceAllocation": "true"
                        }
                    }
                ],
                
                StepConcurrencyLevel=1,
                
                Steps=[{
                    'Name': 'AbreCSV_SalvaPARQUET',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                 '--master', 'yarn',
                                 '--deploy-mode', 'cluster',
                                 's3://datalake-bruno-468906988086-igti-edc-tf/emr-code/pyspark/AbreCSV_SalvaPARQUET.py'
                                 ]
                    }
                }],
            )
    
    return {
        'statusCode': 200,
        'body': f"Started job flow {cluster_id['JobFlowId']}"
    }
