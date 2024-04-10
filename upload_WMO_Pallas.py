import boto3
import os
import glob

# %%
with open('UASDC_Participant_S3_Creds.txt', 'r') as f:
    lines = [line.rstrip() for line in f]

# access key sent separately 
aws_access_key_id = lines[7]
# secret access key sent separately
aws_secret_access_key = lines[9]
# bucket name
bucket_name = lines[1]

# %%
# path to file
local_filepaths = r"C:\Users\le\OneDrive - Ilmatieteen laitos\WMO-DC\Pallas\mSKW/WMO_ready/*.nc"

# %%
files = glob.glob(local_filepaths)
# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

for local_filepath in glob.glob(local_filepaths):
    # s3_filepath = "49/" + os.path.basename(local_filepath)[6:]
    s3_filepath = "49/" + os.path.basename(local_filepath)
       # Upload the file to the S3 bucket
    try:
        s3.upload_file(local_filepath, bucket_name, s3_filepath)
        print(f"File {local_filepath} uploaded to {bucket_name}/{s3_filepath}")
    except Exception as e:
        print(f"An error occurred: {e}")

# %%
# Listing all files in the s3 bucket
try:
    response = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        print("Files in the bucket:")
        for item in response['Contents']:
            print(item['Key'])
    else:
        print("No files found in the bucket.")
except Exception as e:
    print(f"Failed to list files in the bucket: {e}")
    