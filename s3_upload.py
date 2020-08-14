import aws_secrets
import sys
import os
import boto3
from io import BytesIO
import gzip
import shutil


# Uploads the file to s3 as a gzipped file.
def upload_gzipped(client, bucket, key, fp, compressed_fp=None, content_type='text/plain'):
  # If compressed_fp is None, the compression is performed in memory.
  if not compressed_fp:
    compressed_fp = BytesIO()
  with gzip.GzipFile(fileobj=compressed_fp, mode='wb') as gz:
    shutil.copyfileobj(fp, gz)
  compressed_fp.seek(0)
  client.upload_fileobj(compressed_fp, bucket, key, {
      'ContentType': content_type,
      'ContentEncoding': 'gzip'
  })


# Check if a bucket exists in a given s3 client.
def bucket_exists(client, bucket):
  response = client.list_buckets()
  for existing_bucket in response["Buckets"]:
    if existing_bucket["Name"] == bucket:
      return True
  return False


# Uploads the file to s3 and lists the number of objects in the bucket after the
# operation.
def upload_to_s3_bucket(file_name, bucket="billing-uploads"):
  # Create s3 session & client
  session = boto3.session.Session()
  client = session.client(service_name="s3",
                          aws_access_key_id=aws_secrets.access_id,
                          aws_secret_access_key=aws_secrets.access_key,
                          endpoint_url="http://s3.wasabibeta.com")

  # Assigns to object name the file name without the extension
  object_name, _ = os.path.splitext(os.path.basename(file_name))

  # If bucket does not exist, create it
  if not bucket_exists(client, bucket):
    print("Bucket \"{}\" does not exist... creating it!".format(bucket))
    client.create_bucket(Bucket=bucket)

  # Try to upload the file
  print("Uploading (gzipped) {} with key \"{}\"...".format(file_name, object_name))
  with open(file_name, 'rb') as fp:
    upload_gzipped(client, bucket, object_name, fp)
  print("Successfully uploaded file!")

  # Print contents of bucket (for debugging)
  obj_list = client.list_objects(Bucket=bucket)
  # Specifically in this case, the number of objects
  print("There are now {} objects in the bucket.".format(len(obj_list['Contents'])))


# Runs when run as a script
if __name__ == "__main__":
  if len(sys.argv) > 1:
    file_name = sys.argv[1]
    upload_to_s3_bucket(file_name)

  else:
    print("Please specify which file to upload as a positional argument after the script name.")