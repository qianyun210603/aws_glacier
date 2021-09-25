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
import platform

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


def download_job(job_output, myout=sys.stdout):
    filename, _ = parse_description(job_output['archiveDescription'])
    usename = filename
    suffix = 0
    while os.path.exists(usename):
        usename = filename + '.' + str(suffix)
    myout.write(f"Start downloading {filename} to {usename}\n")
    total_length = float(job_output['ResponseMetadata']['HTTPHeaders']['content-length'])
    bytes_written = 0
    with open(filename, "wb") as f:
        for chunk in job_output['body'].iter_chunks(chunk_size=64*1024*1024):
            bytes_written += f.write(chunk)
            myout.write(("{} of {} ({:.2%}) written\n".format(bytes_written, total_length, bytes_written/total_length)))
            myout.flush()
    myout.write(f"Finish downloading {filename} to {usename}\n")
    myout.flush()


def check_and_handle_jobs(_args):
    job_processed = dict()
    retry_count = MAX_RETRY
    myout = open(_args.log_file, "w") if _args.log_file else sys.stdout
    status = {'Running': False}
    if os.path.exists(os.path.join(get_meta_foler(), 'watchdog_status.json')):
        myout.write("Loading status ...\n")
        with open(os.path.join(get_meta_foler(), 'watchdog_status.json'), 'r') as f:
            status = json.load(f)
        if status['Running']:
            myout.write("Another watchdog is running, exit.\n")
            myout.close()
            return
        status['Running'] = True
        with open(os.path.join(get_meta_foler(), 'watchdog_status.json'), 'w') as f:
            json.dump(status, f)
        job_processed.update(status.get("Completed", dict()))
    try:
        while True:
            myout.write("Checking Jobs:\n")
            jobs = glacier.list_jobs(vaultName=_args.vault)
            if jobs['ResponseMetadata']['HTTPStatusCode'] // 100 != 2:
                myout.write("Cannot get job list, retry after 10 seconds ...\n")
                retry_count -= 1
                if retry_count <= 0:
                    myout.write("Maximum retris reached, exit!\n")
                time.sleep(10)
                continue
            job_df = pd.DataFrame(jobs['JobList'])
            myout.flush()
            myout.write(str(job_processed) + '\n')
            for jid, action, cdate in job_df.loc[job_df.Completed, ['JobId', 'Action', 'CreationDate']].values:
                if job_processed.get(jid, "") != cdate:
                    myout.write(f'Processing ready job: {jid}\n')
                    res = glacier.get_job_output(vaultName=_args.vault, jobId=jid)
                    if action == 'InventoryRetrieval':
                        with open(os.path.join(get_meta_foler(), f'inventory_list_{_args.vault}.json'), 'wb') as f:
                            f.write(res['body'].read())
                        myout.write("Inventory list updated!\n")
                    elif action == "ArchiveRetrieval":
                        download_job(res, myout)
                    job_processed[jid] = cdate
                myout.flush()
            if job_df.Completed.all():
                status['Running'] = False
                status['Completed'] = job_df[['JobId', 'CreationDate']].set_index('JobId').to_dict()['CreationDate']
                with open(os.path.join(get_meta_foler(), 'watchdog_status.json'), 'w') as f:
                    json.dump(status, f)
                break
            remaining = job_df[~job_df.Completed].copy()
            remaining.CreationDate = pd.to_datetime(remaining.CreationDate)
            earliest = remaining.loc[:, 'CreationDate'].min()
            until = earliest + pd.Timedelta('5H')
            myout.write(f"Earlist created job at {earliest.isoformat()}, wait until {until.isoformat()}\n")
            myout.flush()
            time.sleep(
                (until - pd.Timestamp.utcnow()).total_seconds()
            )
    except Exception:
        myout.write(traceback.format_exc()+'\n')
    finally:
        myout.close()


def delete_archive(_args):
    archive_list = get_inventory_list(_args.vault)
    archive_id_list = _args.archive_id + archive_list.loc[
        archive_list.FileName.isin(_args.archive_name), 'ArchiveId'].tolist()
    for aid in archive_id_list:
        search_filename = archive_list.loc[archive_list.ArchiveId == aid, 'FileName']
        fname = 'Unknow' if search_filename.empty else search_filename.iloc[0]
        try:
            delete_res = glacier.delete_archive(vaultName=_args.vault, archiveId=aid)
            if delete_res['HTTPStatusCode']  // 100 == 2:
                print(f"{fname}(id: {aid}) deleted.")
            else:
                print("Error: ", str(delete_res))
        except glacier.exceptions.ResourceNotFoundException:
            print(f"{fname}(id: {aid}) not found.")


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
    parser.add_argument("--no-watchdog", action='store_true')
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

    parser_process = subparsers.add_parser("process_job", help="Check status of submitted jobs and process if ready")
    parser_process.add_argument('--download-chunk-size', type=int, default=16, help="download chunksize")
    parser_process.add_argument('--log-file', type=str, default="", help="log file name")
    parser_process.set_defaults(func=check_and_handle_jobs)

    parser_delete = subparsers.add_parser("delete", help="Delete archive by name and/or id.")
    parser_delete.add_argument('-id', '--archive-id', default=[], nargs='+',  help="Archive ids")
    parser_delete.add_argument('-n', '--archive-name', default=[], nargs='+',  help="Archive names")
    parser_delete.set_defaults(func=delete_archive)

    parser_debug = subparsers.add_parser("debug", help="Just for debugging")

    args = parser.parse_args()

    # print(args)

    if 'func' in args:
        args.func(args)

    if not args.no_watchdog and args.command in ('inventory_update', "download", 'debug'):
        import subprocess
        if 'windows' in platform.system().lower():
            subprocess.Popen(f"python aws_glacier.py -v {args.vault} process_job --log-file glacier.log &",
                             shell=True)
        if 'linux' in platform.system().lower():
            subprocess.Popen(f"aws_glacier -v {args.vault} process_job --log-file glacier.log &",
                             shell=True)
        exit(0)
