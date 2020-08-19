import aws_secrets
import sys
import os
import boto3
import time
from botocore.exceptions import ClientError
from io import BytesIO
import gzip
import shutil
from datetime import date, datetime

debugging = False


# Downloads the gzipped file from s3 and unzips it.
def download_gzipped(client, bucket, key, fp, compressed_fp=None):
  # If compressed_fp is None, the compression is performed in memory.
  if not compressed_fp:
    compressed_fp = BytesIO()
  client.download_fileobj(bucket, key, compressed_fp)
  compressed_fp.seek(0)
  with gzip.GzipFile(fileobj=compressed_fp, mode='rb') as gz:
    shutil.copyfileobj(gz, fp)


# Downloads the file with the specified file name from s3.
# Returns True if the download is successful, and False if the file was not found.
# Raises an exception otherwise.
def download_from_s3_bucket(file_name, object_name=None, bucket="billing-uploads"):
  # Create s3 session & client
  session = boto3.session.Session()
  client = session.client(service_name="s3",
                          aws_access_key_id=aws_secrets.access_id,
                          aws_secret_access_key=aws_secrets.access_key,
                          endpoint_url="http://s3.wasabibeta.com")

  # Assigns to object name the file name without the extension
  if object_name is None:
    object_name, _ = os.path.splitext(os.path.basename(file_name))

  try:
    with open(file_name, 'wb') as fp:
      # Download the gz file and unzip it
      download_gzipped(client, bucket, object_name, fp)
      print("Successfully downloaded file!")

      # Now clean up the bucket, since we no longer need the object
      # client.delete_object(Bucket=bucket, Key=object_name)
      # print("Cleaned up object from bucket.")
      print("Not cleaning up, for debugging")

    return True

  except ClientError as e:
    # Delete our created (empty) file, since error
    if os.path.exists(file_name):
      print("Deleting empty file \"{}\".".format(file_name))
      os.remove(file_name)
    # Only catch the (common) 404 error, nothing else
    if e.response['Error']['Code'] == "404":
      print("The file does not exist.")
      return False

    raise


# Function to be run for the daily fetch operation
def daily_pull():
  today = date.today()
  today = today.strftime("%Y-%m-%d")
  now = datetime.now().replace(microsecond=0)
  # Allows manual setting of a date for debugging purposes
  if debugging:
    today = datetime(2020, 8, 5, 0, 0).strftime("%Y-%m-%d")
  # Directory to which the reports get downloaded
  fname = os.path.join("/home", "users", "akendall", "gen2", "billing reports",
                       "BucketUtilization-{}.csv".format(today))
  print("[{}] Attempting to download the daily report to \"{}\"...".format(now, fname))
  download_from_s3_bucket(fname)


# TODO
def import_all():
  download_from_s3_bucket(file_name="tables_global.txt", bucket="global-uploads")
  with open("tables_global.txt", "r") as global_fp:
    tables_global = global_fp.read().splitlines()
    print(tables_global)

  download_from_s3_bucket(file_name="tables_billing.txt", bucket="billing-uploads")
  with open("tables_billing.txt", "r") as billing_fp:
    tables_billing = billing_fp.read().splitlines()
    print(tables_billing)


# Runs when run as a script
if __name__ == "__main__":
  # Changes the working directory to be relative to the current file's folder
  abspath = os.path.abspath(__file__)
  dname = os.path.dirname(abspath)
  os.chdir(dname)

  if len(sys.argv) > 1:
    import_all()
  else:
    # Try to download the daily pull
    daily_pull()
