#!/usr/bin/env python

import sys
import argparse
import boto3
import pandas as pd
import traceback
import time
import os.path
import json
from functools import lru_cache
import xml.etree.ElementTree as ET
import base64
from tabulate import tabulate

MAX_RETRY = 10
glacier = boto3.client('glacier')


def submit_inventory_update(_args):
    init_response = glacier.initiate_job(
        vaultName=_args.vault,
        jobParameters={
            'Description': f'inventory job @ {pd.Timestamp.now().isoformat()}',
            'Type': 'inventory-retrieval',
        })
    if init_response['ResponseMetadata']['HTTPStatusCode'] // 100 == 2:
        print(f"Inventory update job submitted, job id: {init_response['jobId']}")
    else:
        print(f"Inventory update job submission failed")


def submit_downloads(_args):
    archive_list = get_inventory_list(_args.vault)
    archive_id_list = _args.archive_id + archive_list.loc[
        archive_list.FileName.isin(_args.archive_name), 'ArchiveId'].tolist()
    for aid in set(archive_id_list):
        search_filename = archive_list.loc[archive_list.ArchiveId == aid, 'FileName']
        fname = 'Unknow' if search_filename.empty else search_filename.iloc[0]
        jobParameters = {
            'Description': f'Download {fname}',
            'Type': 'archive-retrieval',
            'ArchiveId': aid
        }
        print(jobParameters)
        init_response = glacier.initiate_job(
            vaultName=_args.vault,
            jobParameters={
                'Description': f'Download {fname}',
                'Type': 'archive-retrieval',
                'ArchiveId': aid
            })
        if init_response['ResponseMetadata']['HTTPStatusCode'] // 100 == 2:
            print(f"Download job for {fname} submitted, job id: {init_response['jobId']}")
        else:
            print(f"Download job for {fname} failed")


@lru_cache
def get_meta_foler():
    path = os.path.join(os.path.expanduser('~'), '.aws_glacier')
    os.makedirs(path, exist_ok=True)
    return path


def parse_description(description):
    if description.startswith('<m>'):
        root = ET.fromstring(description)
        # noinspection PyTypeChecker
        return pd.Series(
            [base64.b64decode(root.find('p').text).decode('utf-8'), pd.Timestamp(root.find('lm').text)],
            index=['FileName', 'LastModify'])
    return pd.Series([description, pd.NaT], index=['FileName', 'LastModify'])


@lru_cache
def get_inventory_list(vault):

    inventory_filename = os.path.join(get_meta_foler(), f'inventory_list_{vault}.json')
    if os.path.exists(inventory_filename):
        with open(inventory_filename, 'r') as f:
            inventory_dict = json.load(f)
            archive_list = inventory_dict['ArchiveList']
            df = pd.DataFrame(archive_list)
            df.CreationDate = pd.to_datetime(df.CreationDate)
            return pd.concat([df, df.ArchiveDescription.apply(parse_description)], axis=1)
    return pd.DataFrame(
        columns=['ArchiveId', 'ArchiveDescription', 'CreationDate', 'Size', 'SHA256TreeHash', 'FileName', 'LastModify']
    )


def download_job(job_output):
    filename, _ = parse_description(job_output['archiveDescription'])
    usename = filename
    suffix = 0
    while os.path.exists(usename):
        usename = filename + '.' + str(suffix)
    print(f"Start downloading {filename} to {usename}")
    total_length = float(job_output['ResponseMetadata']['HTTPHeaders']['content-length'])
    bytes_written = 0
    with open(filename, "wb") as f:
        for chunk in job_output['body'].iter_chunks(chunk_size=64*1024*1024):
            bytes_written += f.write(chunk)
            print("{} of {} ({:.2%}) written".format(bytes_written, total_length, bytes_written/total_length))
    print(f"Finish downloading {filename} to {usename}")


def check_and_handle_jobs(_args):
    job_processed = set()
    retry_count = MAX_RETRY
    myout = open(_args.log_file, "w") if _args.log_file else sys.stdout
    try:
        while True:
            myout.write("Checking Jobs:")
            jobs = glacier.list_jobs(vaultName=_args.vault)
            if jobs['ResponseMetadata']['HTTPStatusCode'] // 100 != 2:
                myout.write("Cannot get job list, retry after 10 seconds ...")
                retry_count -= 1
                if retry_count <= 0:
                    myout.write("Maximum retris reached, exit!")
                time.sleep(10)
                continue
            job_df = pd.DataFrame(jobs['JobList'])
            for jid, action in job_df.loc[job_df.Completed, ['JobId', 'Action']].values:
                if jid not in job_processed:
                    myout.write(f'Processing ready job: {jid}')
                    res = glacier.get_job_output(vaultName=_args.vault, jobId=jid)
                    if action == 'InventoryRetrieval':
                        with open(os.path.join(get_meta_foler(), f'inventory_list_{_args.vault}.json'), 'wb') as f:
                            f.write(res['body'].read())
                        myout.write("Inventory list updated!")
                    elif action == "ArchiveRetrieval":
                        download_job(res)
                    job_processed.add(jid)
            if job_df.Completed.all():
                break
            job_df.CreationDate = pd.to_datetime(job_df.CreationDate)
            earliest = job_df.loc[~job_df.Completed, 'CreationDate'].min()
            until = earliest + pd.Timedelta('5H')
            myout.write(f"Earlist created job at {earliest.isoformat()}, wait until {until.isoformat()}")
            time.sleep(
                (until - pd.Timestamp.utcnow()).total_seconds()
            )
    except Exception:
        myout.write(traceback.format_exc())
    finally:
        myout.close()


def list_inventory(_args):

    def size_formatter(size):
        for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return "{:.03f} {}".format(size, unit)
            size /= 1024.0
        return "{:.03f}{}".format(size, unit)

    cols = [x.strip() for x in _args.columns.split(',')]
    inventory_list = get_inventory_list(_args.vault).sort_values(by='FileName')
    filtered = inventory_list.loc[inventory_list.FileName.str.match(_args.filter), cols]
    filtered.Size = filtered.Size.apply(size_formatter)
    print(tabulate(filtered[cols], showindex=False, headers=cols))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='AWS Glacier Operator')
    parser.add_argument("--no-watch-dog", action='store_true')
    parser.add_argument('-v', '--vault', type=str, required=True)

    subparsers = parser.add_subparsers(help='sub-command help', dest='command')

    parser_list_inventory = subparsers.add_parser(
        'list', help="List archives according to local record (note might be outdated)"
    )
    parser_list_inventory.add_argument('-c', '--columns', default="FileName,Size", type=str,
                                       help="Columns of archive list, one or more from "
                                            "{FileName,Size,CreationDate,LastModify,ArchiveId,SHA256TreeHash},"
                                            "sepearted by comma (,).")
    parser_list_inventory.add_argument('-f', '--filter', default="", type=str,
                                       help="Regex to filter FileName")
    parser_list_inventory.set_defaults(func=list_inventory)

    parser_update_inventory_list = subparsers.add_parser('inventory_update', help="Submit inventory update request")
    parser_update_inventory_list.set_defaults(func=submit_inventory_update)

    parser_download = subparsers.add_parser("download", help="Download archive by name and/or archive id")
    parser_download.add_argument('-id', '--archive-id', default=[], nargs='+',  help="Archive ids")
    parser_download.add_argument('-n', '--archive-name', default=[], nargs='+',  help="Archive names")
    parser_download.set_defaults(func=submit_downloads)

    parser_download = subparsers.add_parser("process_job", help="Check status of submitted jobs and process if ready")
    parser_download.add_argument('--download-chunk-size', type=int, default=16, help="download chunksize")
    parser_download.add_argument('--log-file', type=str, default="", help="log file name")
    parser_download.set_defaults(func=check_and_handle_jobs)

    args = parser.parse_args()

    # print(args)

    if 'func' in args:
        args.func(args)

    if not args.no_watch_dog and args.command in ('inventory_update', "download"):
        import subprocess
        subprocess.Popen(f"python aws_glacier.py -v {args.vault} process_job --log-file glacier.log",
                         shell=True)
        exit(0)
