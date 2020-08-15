import mysql_secrets as mysqlcreds
import mysql.connector as mysqlc
from datetime import date, datetime
import sys
import csv
import os
from s3_upload import upload_to_s3_bucket

debugging = False


# Checks the last time the db & table were updated and returns the time elapsed.
def check_update_time_change(cnx):
  cursor = cnx.cursor()
  query = "SELECT UPDATE_TIME FROM information_schema.tables \
    WHERE TABLE_SCHEMA = 'BA_Billing' AND TABLE_NAME = 'BucketUtilization';"

  cursor.execute(query)

  # Unwrap the update time from a single tuple
  for update_time, in cursor:
    print("Update time: {}".format(update_time))

  now = datetime.now()
  print("Time right now: {}".format(now))

  elapsed_time = now - update_time
  elapsed_time = int(elapsed_time.total_seconds() / 60)
  print("Time elapsed since last update: {} minutes".format(elapsed_time))

  return elapsed_time


# Runs query to fetch bucket utilization records for today and writes the results into a csv file.
def fetch_daily_bucket_utils(cnx):
  cursor = cnx.cursor()
  today = date.today()
  today = today.strftime("%Y-%m-%d")
  # Allows manual setting of a date for debugging purposes
  if debugging:
    today = datetime(2020, 7, 27, 0, 0).strftime("%Y-%m-%d")
  query = "SELECT * FROM BucketUtilization WHERE EndTime='{} 00:00:00';".format(today)
  print("Querying: \"{}\"".format(query))
  cursor.execute(query)
  rows = cursor.fetchall()

  # Now write the results into a csv
  fname = os.path.join("bucket-utilization-daily-reports", "BucketUtilization-{}.csv".format(today))
  file_exists = os.path.exists(fname)
  with open(fname, "a") as fp:
    bucket_util_file = csv.writer(fp)
    if not file_exists:
      headers = [i[0] for i in cursor.description]  # Include a header row
      bucket_util_file.writerow(headers)
    bucket_util_file.writerows(rows)
  print("Wrote fetched data to \"{}\".".format(fname))

  # Return the file name to which the data was written
  return fname


# The daily routine of this script:
# 1) Check the elapsed time since last update to the BucketUtilization table.
# 2) If 10 minutes or less have passed, that means there was a recent change that must be
# pulled, since the cron job for this script is scheduled for every 10 minutes.
# 3) Fetch the latest records (from EndTime = today) and export them to CSV.
# 4) Log what happened to a text file.
def daily_routine(cnx):
  # Calculate the elapsed time to determine whether to (re)fetch data
  elapsed_time = 0  #check_update_time_change(cnx) <--- old value, testing w/o time check
  now = datetime.now().replace(microsecond=0)

  # If debugging mode: skip the check and force the CSV export
  if debugging or True:  #elapsed_time <= 10:  <--- old condition, testing w/o time check
    file_name = fetch_daily_bucket_utils(cnx)
    upload_to_s3_bucket(file_name)

    # Log the activity
    with open("./cron-log.txt", "a") as cron_log:
      if debugging:
        cron_log.write("[{}] Debugging mode -> forced fetch data from MySQL.\n".format(now))
      else:
        # cron_log.write("[{}] Fetched data from MySQL, since elapsed time was {}.\n".format(
        #     now, elapsed_time))
        cron_log.write("[{}] Fetched data from MySQL, no elapsed time check for now.\n".format(now))
  # Nothing to do here, just log that we checked and stop
  else:
    with open("./cron-log.txt", "a") as cron_log:
      cron_log.write("[{}] Ran script and did not fetch data.\n".format(now))


def export_all(cnx):
  cursor = cnx.cursor()

  # First, get the list of tables from the desired databases
  cursor.execute("SHOW TABLES FROM BA_Global;")
  tables_global = cursor.fetchall()
  cursor.execute("SHOW TABLES FROM BA_Billing;")
  tables_billing = cursor.fetchall()

  # Tables to exclude from parsing & writing
  tables_exclude = [
      "AccessKeyData", "PolicyData", "BucketData", "PolicyVersionData", "BucketUtilization"
  ]

  # Write the lists to files to retrieve later
  with open("tables_list.txt", "w") as tables_list:
    for tbl_g, in tables_global:
      if not tbl_g in tables_exclude:
        tables_list.write("{}\n".format(tbl_g))
    for tbl_b, in tables_billing:
      if not tbl_b in tables_exclude:
        tables_list.write("{}\n".format(tbl_b))

  # Go through the BA_Global list and select everything into a big dump
  for tbl, in tables_global:
    if not tbl in tables_exclude:
      print("Starting to fetch the contents from \"BA_Global.{}\"...".format(tbl))
      cursor.execute("SELECT * FROM BA_Global.{}".format(tbl))
      rows = cursor.fetchall()
      print("Done fetching. Now trying to write to CSV...")

      # Now write the results into a csv
      fname = os.path.join("BA_Global", "{}.csv".format(tbl))
      with open(fname, "w") as fp:
        bucket_util_file = csv.writer(fp)
        headers = [i[0] for i in cursor.description]  # Include a header row
        bucket_util_file.writerow(headers)
        bucket_util_file.writerows(rows)
      print("Wrote fetched data to \"{}\".".format(fname))
      upload_to_s3_bucket(fname, bucket="global-uploads")

  # Do the same with BA_Billing
  for tbl, in tables_billing:
    if not tbl in tables_exclude:
      print("Starting to fetch the contents from \"BA_Billing.{}\"...".format(tbl))
      cursor.execute("SELECT * FROM BA_Billing.{}".format(tbl))
      rows = cursor.fetchall()
      print("Done fetching. Now trying to write to CSV...")

      # Now write the results into a csv
      fname = os.path.join("BA_Billing", "{}.csv".format(tbl))
      with open(fname, "w") as fp:
        bucket_util_file = csv.writer(fp)
        headers = [i[0] for i in cursor.description]  # Include a header row
        bucket_util_file.writerow(headers)
        bucket_util_file.writerows(rows)
      print("Wrote fetched data to \"{}\".".format(fname))
      upload_to_s3_bucket(fname, bucket="billing-uploads")


# Establishes a connection to a MySQL database with a specified dbname and hostname.
# Operation is the function to execute after connecting. Function must take in a connection.
def connect_to_db(host="db03.beta1", db="BA_Billing", operation=daily_routine):
  print("Connecting to {} for database \"{}\"...".format(host, db))

  # Config used to access db.
  config = {"user": mysqlcreds.user, "password": mysqlcreds.password, "host": host, "database": db}

  try:
    connection = mysqlc.connect(**config)
    print("Connection established.")

    # Higher order function -> "operation" can be any function with mandatory arg "connection"
    operation(connection)

    connection.close()
    print("Closed connection to database.")

  except mysqlc.Error as err:
    print(err)


# Runs when the code is run as a script.
if __name__ == "__main__":
  # Changes the working directory to be relative to the current file's folder
  abspath = os.path.abspath(__file__)
  dname = os.path.dirname(abspath)
  os.chdir(dname)

  # Let the daily shenanigans begin!
  connect_to_db(operation=export_all)
