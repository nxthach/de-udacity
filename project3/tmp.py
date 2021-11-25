import os 
DWH_DB_USER='awsuser'
DWH_DB_PASSWORD='AdminPass123'
DWH_ENDPOINT='redshift-cluster-1.ccmn84cnjbnf.us-east-1.redshift.amazonaws.com'
DWH_PORT='5439'
DWH_DB='dev'

conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
%sql $conn_string


import boto3

KEY='AKIA433DURC4ATQP2POF'
SECRET='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'

s3 = boto3.resource('s3',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )