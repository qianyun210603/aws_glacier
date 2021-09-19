# AWS S3 Glacier

Convinient script for Amazon S3 Glacier by Python3

### Prerequisites
- python 3.8+ (Might work with lower python3 version)
- pandas
- boto3
- base64

- All prerequisites are available in `pip`.

### Install on Linux
* Setup AWS configures, can be done via AWS CLI.
    ```bash
    aws configure
    ```
    Then input API keys and secrets as requried.
* Download the script to proper path.
    ```bash
    wget https://raw.githubusercontent.com/qianyun210603/aws_glacier/master/aws_glacier.py -O /usr/local/bin/aws_glacier
    dos2unix /usr/local/bin/aws_glacier
    chmod +x /usr/local/bin/aws_glacier
    ```

### Usage
```bash
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

