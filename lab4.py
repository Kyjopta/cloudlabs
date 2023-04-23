import boto3
import os
import pandas as pd
import botocore.exceptions

global ec2_client
ec2_client = boto3.client("ec2", region_name="us-east-1", aws_access_key_id = 'AKIAZTOAFOPKV2JVL4XX', aws_secret_access_key = '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')
global s3_client
s3_client = boto3.client('s3', region_name="us-east-1", aws_access_key_id = 'AKIAZTOAFOPKV2JVL4XX', aws_secret_access_key = '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')

def create_key_pair():
    key_pair = ec2_client.create_key_pair(KeyName="ec2-key-pair")
    private_key = key_pair["KeyMaterial"]
    with os.fdopen(os.open("/tmp/aws_ec2_key.pem", os.O_WRONLY | os.O_CREAT, 0o400), "w+") as handle:
        handle.write(private_key)
    
#create_key_pair()

def create_instance():
    instances = ec2_client.run_instances(
    ImageId="ami-00d4ad33aaf7045d7",
    MinCount=1,
    MaxCount=1,
    InstanceType="t4g.nano",
    KeyName="ec2-key-pair"
    )
    print(instances["Instances"][0]["InstanceId"])
    
#create_instance()

def get_public_ip(instance_id):
    ip = []
    tmp= get_running_instances()
    if instance_id in tmp:
        reservations = ec2_client.describe_instances(InstanceIds=[instance_id]).get("Reservations")
        ip.append(reservations[0]['Instances'][0]['PublicIpAddress'])
        return ip
    else:
        print("There is no public IP for instance you mentioned. Most likely that instance is stopped or non-existent yet.")
        
#get_public_ip('i-057318efa869e0a2')

            
def get_running_instances():
    reservations = ec2_client.describe_instances(Filters=[
        {
            "Name": "instance-state-name",
            "Values": ["running"],
        },
    ]).get("Reservations")
    machines= []
    for reservation in reservations:
        for instance in reservation["Instances"]:
            machines.append(instance["InstanceId"])
    return machines
#get_running_instances()

def ssh(instance_id):
    tmp=get_running_instances()
    if instance_id in tmp:
        ip = get_public_ip(instance_id)
        print(f"Your ssh command to connect to an instance: ssh -i aws_ec2_key.pem ec2-user@{ip[0]}")
    else:
        print("There is no public IP for instance you mentioned. Most likely that instance is stopped or non-existent yet.")
        
#ssh('i-057318efa869e0a2c')
        
def stop_instance(instance_id):
    response = ec2_client.stop_instances(InstanceIds=[instance_id])
    print(response)
    print(f"Instance {instance_id} was successfully stopped")
    
def start_instance(instance_id):
    response = ec2_client.start_instances(InstanceIds=[instance_id])
    print(response)
    print(f"Instance {instance_id} was successfully started")
    
def terminate_instance(instance_id):
    response = ec2_client.terminate_instances(InstanceIds=[instance_id])
    print(response)
    print(f"Instance {instance_id} was terminated")
    
def get_instance_info(instance_id):
    response = ec2_client.describe_instance_status(InstanceIds=[instance_id])
    print(f"Instance {instance_id} info:")
    print(response)
    
    
#stop_instance('i-057318efa869e0a2c')



def bucket_element_exists(bucket_name, s3_obj_name):
    try:
        s3_client.get_object(Bucket = bucket_name, Key = s3_obj_name)
    except:
        return False
    return True
    
def buckets_list():
    response = s3_client.list_buckets()
    buckets= []
    for bucket in response['Buckets']:
        buckets.append(bucket["Name"])
    #print(f'Existing buckets:{buckets}')
    return buckets

#buckets_list()

def bucket_exists(bucket_name):
    tmp = buckets_list()
    if bucket_name not in tmp:
        return False
    return True

def create_bucket(bucket_name):
    tmp = buckets_list()
    if bucket_name in tmp:
        print("Error, such backet already exists")
        return
    try:
        response = s3_client.create_bucket(Bucket=bucket_name)
        print(response)
    except botocore.exceptions.ParamValidationError:
        print("Error, invalid name. Bucket name must contain only letters, numbers and '-'")
        return

#create_bucket("novaklab4")    


def upload(file_name, bucket_name):
    if not bucket_exists(bucket_name):
        print(F"Error. No such bucket {bucket_name}")
        return
    if bucket_element_exists(bucket_name, file_name):
        print(F"Error.File already exists {file_name}")
        return
    else:
        with open(file_name, "rb") as f:
            s3_client.upload_fileobj(f, bucket_name, file_name)
            print('File was uploaded successfully')


def read_csv(bucket_name, obj_name):
    if not bucket_exists(bucket_name):
        print(F"Error. No such bucket {bucket_name}")
        return
    if not bucket_element_exists(bucket_name, obj_name):
        print(F"Error. No such file {obj_name}")
        return
    obj = s3_client.get_object(
        Bucket=bucket_name,
        Key=obj_name
    )
    data = pd.read_csv(obj['Body'])
    print('Printing the data frame...')
    print(data.head(10))
    

def destroy_bucket(bucket_name):
    if bucket_exists(bucket_name):
        response = s3_client.delete_bucket(Bucket=bucket_name)
        print(response)
    else:
        print('Error.Bucket does not exist')


#upload('WannaCry.pdf', 'novaklab4')
#read_csv('novaklab', 'data.csv')
