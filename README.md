# MySQL Data Transfer Scripts
Scripts related to exporting from MySQL, uploading to S3 buckets, downloading, etc.

## Configuration
To set the credentials for AWS, create a file `aws_secrets.py` and set `access_id` and `access_key` variables appropriately.
Similarly, set the `user` and `password` variables for `mysql_secrets.py` on that end.

The three "main" scripts--`mysql_connect.py`, `s3_download.py`, and `s3_upload.py`--should all be in the same directory.

To use `mysql_connect.py` and its functionalities, there should be two folders "BA_Billing", "BA_Global", and "GDB_dbstarter" in the same directory, as well as the secret files.
"GDB_dbstarter" should contain another two folders "BA_Billing" and "BA_Global". 

In other words, it should look like this:
```
- mysql_connect.py
- s3_upload.py
- s3_download.py
- BA_Global/
- BA_Billing/
- GDB_dbstarter/
  - BA_Global/
  - BA_Billing/
- aws_secrets.py
- mysql_secrets.py
```

Also, certain python modules will need to be installed. They are all listed at the top of each .py script. These are mostly libraries for s3 and MySQL.

## Usage
`mysql_connect.py` is an all-purpose script that handles anything related to the MySQL database. Run the script with `-h` or `--help` for instructions. 
It has three main uses atm:
- `--export-gdb`: Export GDB tables (BA_Billing and BA_Global) to CSV from a host of your choice. Certain tables are excluded: AccessKeyData, PolicyData, BucketData, PolicyVersionData, and BucketUtilization
  - `--start-over`: If this flag is set, fetch everything in the tables. If not set, it will only fetch the data not previously fetched. (The script keeps track of what was already fetched by checking autoincrement values.)
  - `--force` or `-f`: Ignore `tables_done.txt`, meaning it will not skip any tables and redo the tables already processed before with this script. You will most likely want to include this flag.
- `--export-schemas`: Export the schemas of the GDB tables into a ClickHouse-friendly format.
  - `--db {dbname}`: Specify the database name to fetch only BA_Billing or BA_Global (or other databases).
- `--export-BucketUtilization`: Export BA_Billing.BucketUtilization to CSV. This is separate because it was done before the other two functionalities were implemented. 
One more flag to note: `--host {hostname}` allows for setting the host. Default is db01.ashburn.

The other two scripts, `s3_download.py` and `s3_upload.py`, are much simpler. They have a single purpose: download gzipped files from the s3 bucket or upload files after gzipping them to the s3 bucket. The code is relatively simple compared to `mysql_connect.py` so feel free to change it to your liking.

One caveat for `s3_download.py` is that you can run it two ways: `python3 s3_download.py` (with no arguments after the script name) will run the daily pull for the BucketUtilization table from the bucket "billing-uploads". Running the script `python3 s3_download.py asdfasfasd` (any single argument after the script name) will import everything else (i.e. download all the other CSVs from the buckets). Currently, the functions are set to not clean up after downloading (as uploading a new CSV will overwrite the old one for the full export), but this can easily be changed or run differently. The BucketUtilization CSV has a date stamp as part of the file name, so this might be worth cleaning up after import.

## Troubleshooting
Feel free to contact me at jk2537@cornell.edu
