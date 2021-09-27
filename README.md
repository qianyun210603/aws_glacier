# AWS S3 Glacier

Convinient script for Amazon S3 Glacier by Python3

### Prerequisites
The script requires Python 3 (It is developed under python 3.8.3)
- pandas
- boto3
- base64
- tabulate
- binascii
- hashlib

All prerequisites are available in `pip`.

### Install on Linux
* Setup AWS configures, can be done via AWS CLI.
    ```bash
    aws configure
    ```
    Then input API keys and secrets as requried.
* Download the script to proper path.
    ```bash
    wget https://raw.githubusercontent.com/qianyun210603/aws_glacier/master/aws_glacier.py -O /usr/local/bin/aws_glacier
    chmod +x /usr/local/bin/aws_glacier
    ```

### Usage
```commandline
# aws_glacier -h
usage: aws_glacier.py [-h] [--no-watch-dog] -v VAULT {list,inventory_update,download,process_job} ...

AWS Glacier Operator

positional arguments:
  {list,inventory_update,download,process_job}
                        sub-command help
    list                List archives according to local record (note might be outdated)
    inventory_update    Submit inventory update request
    download            Download archive by name and/or archive id
    process_job         Check status of submitted jobs and process if ready

optional arguments:
  -h, --help            show this help message and exit
  --no-watch-dog
  -v VAULT, --vault VAULT
```

##### Inventory Check and Update
- `list` subcommand lists archive according to local record stored in `$HOME/.aws_glacier/inventory_list_{vault}.json`
  ```commandline
  # aws_glacier list -h
  usage: aws_glacier list [-h] [-c COLUMNS] [-f FILTER]
  
  optional arguments:
    -h, --help            show this help message and exit
    -c COLUMNS, --columns COLUMNS
                          Columns of archive list, one or more from {FileName,Size,CreationDate,LastModify,ArchiveId,SHA256TreeHash},sepearted by comma (,).
    -f FILTER, --filter FILTER
                          Regex to filter FileName
  ```
- `inventory_update` subcommand submits a job to update the records in `$HOME/.aws_glacier/inventory_list_{vault}.json`.
  if `--no-watchdog` argument is not specified, it will also start a `process_job` process on the background for future job result retrieval.
  ```commandline
  # aws_glacier inventory_update -h
  usage: aws_glacier inventory_update [-h]
  
  optional arguments:
    -h, --help  show this help message and exit
  ```

##### Download
`download` subcommand submit a download request of archives by name and/or archive id. 
If `--no-watchdog` argument is not specified, it will also start a `process_job` process on the background for future job result retrieval.
```commandline
# aws_glacier download -h
usage: aws_glacier download [-h] [-id ARCHIVE_ID [ARCHIVE_ID ...]] [-n ARCHIVE_NAME [ARCHIVE_NAME ...]]

optional arguments:
  -h, --help            show this help message and exit
  -id ARCHIVE_ID [ARCHIVE_ID ...], --archive-id ARCHIVE_ID [ARCHIVE_ID ...]
                        Archive ids
  -n ARCHIVE_NAME [ARCHIVE_NAME ...], --archive-name ARCHIVE_NAME [ARCHIVE_NAME ...]
                        Archive names
```

##### Job Status Check and Processing
`process_job` subcommand monitors status of submitted jobs and process them when ready.
```commandline
# aws_glacier process_job -h
usage: aws_glacier process_job [-h] [--download-chunk-size DOWNLOAD_CHUNK_SIZE] [--log-file LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  --download-chunk-size DOWNLOAD_CHUNK_SIZE
                        download chunksize
  --log-file LOG_FILE   log file name
```

##### Upload File(s)
`upload` subcommand uploads file to AWS vaults as 'archives'.
```commandline
python aws_glacier.py upload -h
usage: aws_glacier.py upload [-h] [-f FILE_PATHS [FILE_PATHS ...]] [--num-threads NUM_THREADS] [--upload-chunk-size UPLOAD_CHUNK_SIZE]

optional arguments:
  -h, --help            show this help message and exit
  -f FILE_PATHS [FILE_PATHS ...], --file-paths FILE_PATHS [FILE_PATHS ...]
                        Files to upload
  --num-threads NUM_THREADS
                        No. of threads for parallel upload.
  --upload-chunk-size UPLOAD_CHUNK_SIZE
                        Upload chunksize (MB, between 4-4096 and power of 2)
```


##### Delete Archive(s)
`delete` subcommand deletes archives by name and/or archive id. 
```commandline
# aws_glacier delete -h
usage: aws_glacier delete [-h] [-id ARCHIVE_ID [ARCHIVE_ID ...]] [-n ARCHIVE_NAME [ARCHIVE_NAME ...]]

optional arguments:
  -h, --help            show this help message and exit
  -id ARCHIVE_ID [ARCHIVE_ID ...], --archive-id ARCHIVE_ID [ARCHIVE_ID ...]
                        Archive ids
  -n ARCHIVE_NAME [ARCHIVE_NAME ...], --archive-name ARCHIVE_NAME [ARCHIVE_NAME ...]
                        Archive names
```

### Future plan
- [ ] Upload related functionalitis
- [ ] Archive deletion
- [ ] Auto update local records after upload and deletion