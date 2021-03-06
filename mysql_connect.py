import mysql_secrets as mysqlcreds
import mysql.connector as mysqlc
from datetime import date, datetime
import sys
import csv
import os
from s3_upload import upload_to_s3_bucket
import argparse

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
# 1) Check the elapsed time since last update to the BucketUtilization table. NOT DOING ANYMORE
# 2) If 10 minutes or less have passed, that means there was a recent change that must be
# pulled, since the cron job for this script is scheduled for every 10 minutes. N/A ANYMORE
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


# Export all the tables from BA_Billing and BA_Global from MySQL into CSV.
def export_all(cnx, start_from_scratch=False, ignore_tables_done=False):
  now = datetime.now().replace(microsecond=0)  # For logging purposes
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
  with open("tables_global.txt", "w") as tables_list:
    for tbl_g, in tables_global:
      if not tbl_g in tables_exclude:
        tables_list.write("{}\n".format(tbl_g))
    upload_to_s3_bucket("tables_global.txt", bucket="global-uploads")
  with open("tables_billing.txt", "w") as tables_list:
    for tbl_b, in tables_billing:
      if not tbl_b in tables_exclude:
        tables_list.write("{}\n".format(tbl_b))
    upload_to_s3_bucket("tables_billing.txt", bucket="billing-uploads")

  # Add the tables already processed to the list of exclusions
  if not ignore_tables_done:
    try:
      with open("tables_done.txt", "r") as already_processed:
        tables_exclude.extend(already_processed.read().splitlines())
    except:
      pass

  # Go through the BA_Global list and select everything into a big dump
  for tbl, in tables_global:
    if not tbl in tables_exclude:
      print("\n> Starting to fetch the contents from \"BA_Global.{}\"...".format(tbl))

      # Look up autoinc column
      cursor.execute(
          "SHOW COLUMNS FROM BA_Global.{} WHERE Extra LIKE '%auto_increment%';".format(tbl))
      try:
        auto_inc_col = cursor.fetchall()[0][0]
        no_auto_inc = False
      except:
        print("No autoincrement column for this table... Will fetch everything.")
        auto_inc_col = None
        no_auto_inc = True

      # Get the latest autoinc value
      if not no_auto_inc:
        cursor.execute("SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES \
          WHERE TABLE_SCHEMA = 'BA_Global' AND TABLE_NAME = '{}';".format(tbl))
        next_auto_inc = str(cursor.fetchall()[0][0])

      # Fetch from the very beginning for a clean export, or pick up from where it left off last
      if start_from_scratch or no_auto_inc:
        now = datetime.now().replace(microsecond=0)
        print("[{}] Fetching from scratch.".format(now))
        cursor.execute("SELECT * FROM BA_Global.{};".format(tbl))
      else:
        # Get last written autoinc value from file
        try:
          with open(os.path.join("BA_Global", "{}-lastAI.txt".format(tbl)), "r") as auto_inc_log:
            last_auto_inc = int(auto_inc_log.read())
        except:
          print("Auto increment file not found in \"BA_Global\"; defaulting to zero.")
          last_auto_inc = 0

        now = datetime.now().replace(microsecond=0)
        print("[{}] Picking up autoinc values between {} and {}.".format(
            now, last_auto_inc, next_auto_inc))
        cursor.execute("SELECT * FROM BA_Global.{} WHERE {} >= {} AND {} < {};".format(
            tbl, auto_inc_col, last_auto_inc, auto_inc_col, next_auto_inc))

      # Now write the results into a csv
      fname = os.path.join("BA_Global", "{}.csv".format(tbl))
      with open(fname, "w") as fp:
        csv_file = csv.writer(fp)
        headers = [i[0] for i in cursor.description]  # Include a header row
        csv_file.writerow(headers)
      # Infinite loop to make sure only 1000 rows max written at once
      while True:
        rows = cursor.fetchmany(1000)
        if not rows:
          break
        with open(fname, "a", encoding="utf-8") as fp:
          csv_file = csv.writer(fp)
          csv_file.writerows(rows)
      print("[{}] Wrote fetched data to \"{}\".".format(now, fname))

      # Upload it to a s3 bucket
      upload_to_s3_bucket(fname, bucket="global-uploads")

      # Save the value for next time
      if not no_auto_inc:
        with open(os.path.join("BA_Global", "{}-lastAI.txt".format(tbl)), "w") as auto_inc_log:
          auto_inc_log.write(next_auto_inc)

      # Record that the table was uploaded successfully
      with open("tables_done.txt", "a") as tables_done:
        tables_done.write("{}\n".format(tbl))

  # Do the same with BA_Billing
  for tbl, in tables_billing:
    if not tbl in tables_exclude:
      print("\n> Starting to fetch the contents from \"BA_Billing.{}\"...".format(tbl))

      # Look up autoinc column
      cursor.execute(
          "SHOW COLUMNS FROM BA_Billing.{} WHERE Extra LIKE '%auto_increment%';".format(tbl))
      try:
        auto_inc_col = cursor.fetchall()[0][0]
        no_auto_inc = False
      except:
        print("No autoincrement column for this table... Will fetch everything.")
        auto_inc_col = None
        no_auto_inc = True

      # Get the latest autoinc value
      if not no_auto_inc:
        cursor.execute("SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES \
          WHERE TABLE_SCHEMA = 'BA_Billing' AND TABLE_NAME = '{}';".format(tbl))
        next_auto_inc = str(cursor.fetchall()[0][0])

      # Fetch from the very beginning for a clean export, or pick up from where it left off last
      if start_from_scratch or no_auto_inc:
        now = datetime.now().replace(microsecond=0)
        print("[{}] Fetching from scratch.".format(now))
        cursor.execute("SELECT * FROM BA_Billing.{};".format(tbl))
      else:
        # Get last written autoinc value from file
        try:
          with open(os.path.join("BA_Billing", "{}-lastAI.txt".format(tbl)), "r") as auto_inc_log:
            last_auto_inc = int(auto_inc_log.read())
        except:
          print("Auto increment file not found in \"BA_Billing\"; defaulting to zero.")
          last_auto_inc = 0

        now = datetime.now().replace(microsecond=0)
        print("[{}] Picking up autoinc values between {} and {}.".format(
            now, last_auto_inc, next_auto_inc))
        cursor.execute("SELECT * FROM BA_Billing.{} WHERE {} >= {} AND {} < {};".format(
            tbl, auto_inc_col, last_auto_inc, auto_inc_col, next_auto_inc))

      # Now write the results into a csv
      fname = os.path.join("BA_Billing", "{}.csv".format(tbl))
      with open(fname, "w") as fp:
        csv_file = csv.writer(fp)
        headers = [i[0] for i in cursor.description]  # Include a header row
        csv_file.writerow(headers)
      # Infinite loop to make sure only 1000 rows max written at once
      while True:
        rows = cursor.fetchmany(1000)
        if not rows:
          break
        with open(fname, "a", encoding="utf-8") as fp:
          csv_file = csv.writer(fp)
          csv_file.writerows(rows)

      now = datetime.now().replace(microsecond=0)
      print("[{}] Wrote fetched data to \"{}\".".format(now, fname))

      # Upload it to a s3 bucket
      upload_to_s3_bucket(fname, bucket="billing-uploads")

      # Save the value for next time
      if not no_auto_inc:
        with open(os.path.join("BA_Billing", "{}-lastAI.txt".format(tbl)), "w") as auto_inc_log:
          auto_inc_log.write(next_auto_inc)

      # Record that the table was uploaded successfully
      with open("tables_done.txt", "a") as tables_done:
        tables_done.write("{}\n".format(tbl))


# Helper function to change MySQL column types into ClickHouse types.
# Not optimized, and chunks all the numeric types to 64 bits.
def convert_mysql_to_clickhouse(col_type):
  col_type = col_type.lower()
  if "bool" in col_type:
    return "UInt8"
  if "int" in col_type:
    if "unsigned" in col_type:
      return "UInt64"
    return "Int64"
  if "float" in col_type or "double" in col_type or "real" in col_type:
    return "Float64"
  if "varchar" in col_type or "text" in col_type:
    return "String"
  if "datetime" in col_type:
    return "DateTime"
  if "date" in col_type:
    return "Date"

  print("Couldn't match \"{}\"".format(col_type))
  return "???"  # Manual handling will be necessary outside the basic scope


# Parses the result of a "DESCRIBE table" MySQL query into a ClickHouse-compatible string.
def parse_mysql_schema(table_schema, return_primary_key=False):
  output_list = []  # Temporary list, to be joined into a string later
  primary_key = ""
  auto_inc = ""

  for col in table_schema:
    # Split the column data into variables we can work with
    field, col_type, nullable, key, _, extra = col

    # Field always goes first with backticks surrounding it
    col_string = "`{}` ".format(field)

    # Check if the column is nullable
    if nullable == "YES":
      col_string += "Nullable({}),\n  ".format(convert_mysql_to_clickhouse(col_type))
    else:
      col_string += "{},\n  ".format(convert_mysql_to_clickhouse(col_type))

    # Append it to the result list
    output_list.append(col_string)

    # Check whether this column is the primary key
    if key == "PRI":
      primary_key = field

    # And check if this is the autoinc column (useful for daily updates)
    if extra == "auto_increment":
      auto_inc = field

  # Now join 'em up
  out_string = "".join(output_list)
  out_string = out_string[:-4]  # We don't need the last comma and new line

  # Do we return just the ClickHouse query string, or do we also return the primary key & autoinc?
  if return_primary_key:
    return out_string, primary_key, auto_inc
  return out_string


# Reads the table list file line-by-line and creates a "rough draft" CH schema for each.
def export_schemas(cnx, db_name="all"):
  if db_name == "all":
    export_schemas(cnx, "BA_Global")
    export_schemas(cnx, "BA_Billing")
    return
  cursor = cnx.cursor()

  # Proper name needed for opening the list of tables
  if "Global" in db_name:
    lst = "global"
  elif "Billing" in db_name:
    lst = "billing"
  else:
    print(
        "Not BA_Global or BA_Billing, so defaulting to \"tables_{}.txt\" for table info...".format(
            db_name))
    lst = db_name

  # Get the list of tables
  with open("tables_{}.txt".format(lst), "r") as tbls:
    tables_list = tbls.read().splitlines()

  # Iterate through the list and generate the schemas
  for tbl in tables_list:
    # Describe query from MySQL
    cursor.execute("DESCRIBE {}.{};".format(db_name, tbl))
    mysql_schema = cursor.fetchall()

    # Parse the schema to generate ClickHouse format
    columns, order_by, _ = parse_mysql_schema(mysql_schema, True)

    # Write the full CH query to its own DB-generation file
    fname = os.path.join("GDB_dbstarter", db_name, "{}.txt".format(tbl))
    with open(fname, "w") as clickhouse_schema:
      clickhouse_schema.write("""\
CREATE TABLE {}.{}
(
  {}
)
ENGINE = ReplacingMergeTree
ORDER BY {}
""".format(db_name, tbl, columns, order_by))


# Establishes a connection to a MySQL database with a specified dbname and hostname.
# Operation is the function to execute after connecting. Function must take in a connection.
def connect_to_db(host, db="BA_Billing", operation=daily_routine, **kwargs):
  print("Connecting to {} for database \"{}\"...".format(host, db))

  # Config used to access db.
  config = {"user": mysqlcreds.user, "password": mysqlcreds.password, "host": host, "database": db}

  try:
    connection = mysqlc.connect(**config)
    print("Connection established.")

    # Higher order function -> "operation" can be any function with mandatory arg "connection"
    operation(connection, **kwargs)

    connection.close()
    print("Closed connection to database.")

  except mysqlc.Error as err:
    now = datetime.now().replace(microsecond=0)
    print("[{}] {}".format(now, err))

    # Recursively try again if it fails due to a MySQL timeout error
    if "2013 (HY000)" in str(err):
      print("Detected MySQL connection timeout error. Trying again...")
      connect_to_db(host=host, db=db, operation=operation, **kwargs)


# Runs when the code is run as a script.
if __name__ == "__main__":
  # Changes the working directory to be relative to the current file's folder
  abspath = os.path.abspath(__file__)
  dname = os.path.dirname(abspath)
  os.chdir(dname)

  # Begin parsing the user input from the command line
  parser = argparse.ArgumentParser(description="Various related functions to MySQL exports.")
  parser.add_argument("--host",
                      type=str,
                      default="db01.ashburn",
                      help="The DB hostname, default db01.ashburn")

  # Export the GDB tables into CSV format
  parser.add_argument("--export-gdb",
                      action="store_true",
                      help="Export GDB tables from MySQL into CSV format")
  parser.add_argument("--start-over",
                      action="store_true",
                      help="If --export-gdb is set, this determines whether to fetch everything \
        or pick up from where it last left off")
  parser.add_argument("-f",
                      "--force",
                      action="store_true",
                      help="If --export-gdb is set, setting this will ignore tables_done.txt \
                      when starting the export")

  # Export the table schemas of the specified database into a ClickHouse-friendly format
  parser.add_argument("--export-schemas",
                      action="store_true",
                      help="Export GDB tables' schemas into ClickHouse-friendly format. \
        If --db is not specified, then the default behavior is BA_Global & BA_Billing")
  parser.add_argument("--db",
                      type=str,
                      default="all",
                      help="If --export-schemas is set, the DB from which the export is done")

  # Export BA_Billing.BucketUtilization as part of the daily updates
  parser.add_argument("--export-BucketUtilization",
                      action="store_true",
                      help="Export BA_Billing.BucketUtilization from MySQL into CSV")

  # Put 'em all into "args" for usage
  args = parser.parse_args()

  # Now, check what we actually want to do, based on the arguments passed
  if args.export_gdb:
    connect_to_db(host=args.host,
                  operation=export_all,
                  start_from_scratch=args.start_over,
                  ignore_tables_done=args.force)
  elif args.export_schemas:
    connect_to_db(host=args.host, operation=export_schemas, db_name=args.db)
  elif args.export_BucketUtilization:
    connect_to_db(host=args.host, operation=daily_routine)
  else:
    print("You didn't specify any arguments with the script. "
          "Run with --help or -h for available options.")
