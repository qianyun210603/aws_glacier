#!/usr/bin/env python
import binascii
import threading
import concurrent.futures
import math
import sys
import hashlib
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


def upload_archive(_args):
    upload_results = []
    for fn in _args.file_paths:
        try:
            res = upload_one_file(_args.vault, fn, _args.upload_chunk_size, _args.num_threads)
            upload_results.append(res)
        except Exception:
            traceback.print_exc()
    raw_inventory_dict = _get_raw_inventory(_args.vault)
    if raw_inventory_dict is not None:
        raw_inventory_dict['ArchiveList'].extend(upload_results)
        with open(os.path.join(get_meta_foler(), f'inventory_list_{_args.vault}.json'), 'w') as f:
            json.dump(raw_inventory_dict, f)
        print("Local Inventory records updated")

def upload_one_file(vault_name, file_path, part_size, num_threads, upload_id=None):

    if not math.log2(part_size).is_integer():
        raise ValueError('part-size must be a power of 2')
    if part_size < 1 or part_size > 4096:
        raise ValueError('part-size must be more than 1 MB '
                         'and less than 4096 MB')

    print('Reading file...')
    file_to_upload = open(file_path, mode='rb')
    print('Opened single file.')

    part_size = part_size * 1024 * 1024

    file_size = file_to_upload.seek(0, 2)

    nowtime = pd.Timestamp.utcnow()
    encoded_filename =  base64.b64encode(os.path.basename(file_path).encode()).decode()
    arc_desc = f'<m><v>4</v><p>{encoded_filename}</p><lm>{nowtime.strftime("%Y%m%dT%H%M%SZ")}</lm></m>'

    if file_size < 4096:
        print('File size is less than 4 MB. Uploading in one request...')

        response = glacier.upload_archive(
            vaultName=vault_name,
            archiveDescription=arc_desc,
            body=file_to_upload)

        print(f'{file_path} uploaded successful.')
        print('Glacier tree hash: %s' % response['checksum'])
        print('Location: %s' % response['location'])
        print('Archive ID: %s' % response['archiveId'])
        print('Done.')
        file_to_upload.close()
        result = {
            'CreationDate': nowtime.strftime("%Y-%m-%dT%H:%M:%SZ"), 'ArchiveId': response['archiveId'],
            'ArchiveDescription': arc_desc, 'Size': file_size, 'SHA256TreeHash': response['checksum']
        }
        return result

    job_list = []
    list_of_checksums = []

    if upload_id is None:
        print('Initiating multipart upload...')
        response = glacier.initiate_multipart_upload(
            vaultName=vault_name,
            archiveDescription=arc_desc,
            partSize=str(part_size)
        )
        upload_id = response['uploadId']

        for byte_pos in range(0, file_size, part_size):
            job_list.append(byte_pos)
            list_of_checksums.append(None)

        num_parts = len(job_list)
        print('File size is {} bytes. Will upload in {} parts.'.format(file_size, num_parts))
    else:
        print('Resuming upload...')

        print('Fetching already uploaded parts...')
        response = glacier.list_parts(
            vaultName=vault_name,
            uploadId=upload_id
        )
        parts = response['Parts']
        part_size = response['PartSizeInBytes']
        while 'Marker' in response:
            print('Getting more parts...')
            response = glacier.list_parts(
                vaultName=vault_name,
                uploadId=upload_id,
                marker=response['Marker']
            )
            parts.extend(response['Parts'])

        for byte_pos in range(0, file_size, part_size):
            job_list.append(byte_pos)
            list_of_checksums.append(None)

        num_parts = len(job_list)

        for part_data in parts:
            byte_start = int(part_data['RangeInBytes'].partition('-')[0])
            file_to_upload.seek(byte_start)
            part = file_to_upload.read(part_size)
            checksum = calculate_tree_hash(part, part_size)

            if checksum == part_data['SHA256TreeHash']:
                job_list.remove(byte_start)
                part_num = byte_start // part_size
                list_of_checksums[part_num] = checksum

    print('Spawning threads...')
    fileblock = threading.Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures_list = {executor.submit(
            upload_part, job, vault_name, upload_id, part_size, file_to_upload,
            file_size, num_parts, fileblock): job // part_size for job in job_list}
        done, not_done = concurrent.futures.wait(
            futures_list, return_when=concurrent.futures.FIRST_EXCEPTION)
        if len(not_done) > 0:
            # an exception occured
            for future in not_done:
                future.cancel()
            for future in done:
                e = future.exception()
                if e is not None:
                    print('Exception occured: %r' % e)
            print('Resuming upload. Upload id: %s' % upload_id)
            file_to_upload.close()
            return upload_one_file(vault_name, file_path, part_size, num_threads, upload_id)
        else:
            # all threads completed without raising
            for future in done:
                job_index = futures_list[future]
                list_of_checksums[job_index] = future.result()

    if len(list_of_checksums) != num_parts:
        print('List of checksums incomplete. Recalculating...')
        list_of_checksums = []
        for byte_pos in range(0, file_size, part_size):
            part_num = int(byte_pos / part_size)
            print('Checksum %s of %s...' % (part_num + 1, num_parts))
            file_to_upload.seek(byte_pos)
            part = file_to_upload.read(part_size)
            list_of_checksums.append(calculate_tree_hash(part, part_size))

    total_tree_hash = calculate_total_tree_hash(list_of_checksums)

    print('Completing multipart upload...')
    response = glacier.complete_multipart_upload(
        vaultName=vault_name, uploadId=upload_id,
        archiveSize=str(file_size), checksum=total_tree_hash)
    print(f'{file_path} uploaded successful.')
    print('Calculated total tree hash: %s' % total_tree_hash)
    print('Glacier total tree hash: %s' % response['checksum'])
    print('Done.')
    file_to_upload.close()
    result = {
        'CreationDate': nowtime.strftime("%Y-%m-%dT%H:%M:%SZ"), 'ArchiveId': response['archiveId'],
        'ArchiveDescription': arc_desc, 'Size': file_size, 'SHA256TreeHash': response['checksum']
    }
    return result


def upload_part(byte_pos, vault_name, upload_id, part_size, fileobj, file_size,
                num_parts, fileblock):
    fileblock.acquire()
    fileobj.seek(byte_pos)
    part = fileobj.read(part_size)
    fileblock.release()

    range_header = 'bytes {}-{}/{}'.format(
        byte_pos, byte_pos + len(part) - 1, file_size)
    part_num = byte_pos // part_size
    percentage = part_num / num_parts

    print('Uploading part {0} of {1}... ({2:.2%})'.format(
        part_num + 1, num_parts, percentage))

    for i in range(MAX_RETRY):
        try:
            response = glacier.upload_multipart_part(
                vaultName=vault_name, uploadId=upload_id,
                range=range_header, body=part)
            checksum = calculate_tree_hash(part, part_size)
            if checksum != response['checksum']:
                print('Checksums do not match. Will try again.')
                continue

            # if everything worked, then we can break
            break
        except:
            print('Upload error:', sys.exc_info()[0])
            print('Trying again. Part {0}'.format(part_num + 1))
    else:
        print('After multiple attempts, still failed to upload part')
        print('Exiting.')
        sys.exit(1)

    del part
    return checksum


def calculate_tree_hash(part, part_size):
    checksums = []
    upper_bound = min(len(part), part_size)
    step = 1024 * 1024  # 1 MB
    for chunk_pos in range(0, upper_bound, step):
        chunk = part[chunk_pos:chunk_pos+step]
        checksums.append(hashlib.sha256(chunk).hexdigest())
        del chunk
    return calculate_total_tree_hash(checksums)


def calculate_total_tree_hash(list_of_checksums):
    tree = list_of_checksums[:]
    while len(tree) > 1:
        parent = []
        for i in range(0, len(tree), 2):
            if i < len(tree) - 1:
                part1 = binascii.unhexlify(tree[i])
                part2 = binascii.unhexlify(tree[i + 1])
                parent.append(hashlib.sha256(part1 + part2).hexdigest())
            else:
                parent.append(tree[i])
        tree = parent
    return tree[0]



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
    raw_inventory_dict = _get_raw_inventory(_args.vault)
    archive_list = get_inventory_list(raw_inventory_dict)
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
def _get_raw_inventory(vault):
    inventory_filename = os.path.join(get_meta_foler(), f'inventory_list_{vault}.json')
    if os.path.exists(inventory_filename):
        with open(inventory_filename, 'r') as f:
            inventory_dict = json.load(f)
            return inventory_dict
    return None


def get_inventory_list(inventory_dict):
    if inventory_dict is None:
        return pd.DataFrame(
            columns=['ArchiveId', 'ArchiveDescription', 'CreationDate', 'Size', 'SHA256TreeHash', 'FileName',
                     'LastModify']
        )
    archive_list = inventory_dict['ArchiveList']
    df = pd.DataFrame(archive_list)
    df.CreationDate = pd.to_datetime(df.CreationDate)
    return pd.concat([df, df.ArchiveDescription.apply(parse_description)], axis=1)



def download_job(job_output, chunk_size=64, myout=sys.stdout):
    filename, _ = parse_description(job_output['archiveDescription'])
    usename = filename
    suffix = 0
    while os.path.exists(usename):
        usename = filename + '.' + str(suffix)
    myout.write(f"Start downloading {filename} to {usename}\n")
    total_length = float(job_output['ResponseMetadata']['HTTPHeaders']['content-length'])
    bytes_written = 0
    with open(filename, "wb") as f:
        for chunk in job_output['body'].iter_chunks(chunk_size=chunk_size):
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
                        download_job(res, _args.download_chunk_size, myout)
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
    raw_inventory_dict = _get_raw_inventory(_args.vault)
    archive_list = get_inventory_list(raw_inventory_dict)
    archive_id_list = _args.archive_id + archive_list.loc[
        archive_list.FileName.isin(_args.archive_name), 'ArchiveId'].tolist()
    success = set()
    try:
        for aid in archive_id_list:
            search_filename = archive_list.loc[archive_list.ArchiveId == aid, 'FileName']
            fname = 'Unknow' if search_filename.empty else search_filename.iloc[0]
            try:
                delete_res = glacier.delete_archive(vaultName=_args.vault, archiveId=aid)
                if delete_res['ResponseMetadata']['HTTPStatusCode']  // 100 == 2:
                    success.add(aid)
                    print(f"{fname}(id: {aid}) deleted.")
                else:
                    print("Error: ", str(delete_res))
            except glacier.exceptions.ResourceNotFoundException:
                print(f"{fname}(id: {aid}) not found.")
    finally:
        if raw_inventory_dict is not None:
            raw_inventory_dict['ArchiveList'] = [
                x for x in raw_inventory_dict['ArchiveList'] if x['ArchiveId'] not in success
            ]
            with open(os.path.join(get_meta_foler(), f'inventory_list_{_args.vault}.json'), 'w') as f:
                json.dump(raw_inventory_dict, f)
            print("Local Inventory records updated")


def list_inventory(_args):

    def size_formatter(size):
        for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return "{:.03f} {}".format(size, unit)
            size /= 1024.0
        return "{:.03f}{}".format(size, unit)

    cols = [x.strip() for x in _args.columns.split(',')]
    inventory_list = get_inventory_list(_get_raw_inventory(_args.vault)).sort_values(by='FileName')
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
    parser_process.add_argument('--download-chunk-size', type=int, default=16,
                                help="download chunksize (MB, must be power of 2)")
    parser_process.add_argument('--log-file', type=str, default="", help="log file name")
    parser_process.set_defaults(func=check_and_handle_jobs)

    parser_delete = subparsers.add_parser("delete", help="Delete archive by name and/or id.")
    parser_delete.add_argument('-id', '--archive-id', default=[], nargs='+',  help="Archive ids")
    parser_delete.add_argument('-n', '--archive-name', default=[], nargs='+',  help="Archive names")
    parser_delete.set_defaults(func=delete_archive)

    parser_upload = subparsers.add_parser("upload", help="Upload files to vault")
    parser_upload.add_argument('-f', '--file-paths', default=[], nargs='+',  help="Files to upload")
    parser_upload.add_argument('--num-threads', type=int, default=2, help="No. of threads for parallel upload.")
    parser_upload.add_argument('--upload-chunk-size', type=int, default=16,
                                help="Upload chunksize (MB, between 4-4096 and power of 2)")
    parser_upload.set_defaults(func=upload_archive)

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
