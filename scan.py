import threading

import pandas as pd
import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import StringIO
import os
import psutil


def drop_uniques(datafr: pd.DataFrame, col_name: str, keep_test_col: bool) -> pd.DataFrame:
    data = pd.DataFrame(datafr.duplicated(subset=[col_name], keep=False), columns=['isDuplicated'])  # Mark uniques
    # and duplicates based on col_name

    duplicates = pd.concat([datafr, data], axis=1)

    duplicates = duplicates[duplicates.isDuplicated != 0]  # keep only duplicated rows

    if not keep_test_col:  # drop duplication test column if False
        duplicates.drop('isDuplicated', axis=1, inplace=True)

    return duplicates


def get_drives_labels() -> list:
    existing_drives = []
    drives_info = psutil.disk_partitions(all=True)

    for drive_info in drives_info:
        if os.path.exists(drive_info[0]):
            existing_drives.append(drive_info[0])

    return existing_drives


def get_all_directories() -> list:
    dirs = []
    ex = ['Windows']
    print('Starting scan')
    for i in get_drives_labels():
        dirs.extend([i + d for d in os.listdir(i) if d not in ex])  # Save drive letter+directory to list
    print(dirs)
    return dirs


def scan_files(directory) -> pd.DataFrame:
    df = pd.DataFrame(columns=['path', 'hash'])

    debug_log = open(f'log/files_error_{datetime.now().strftime("%d.%m.%Y_%H.%M.%S")}.log', 'w',
                     encoding='utf-8')  # Logging file

    for root, dirs, files in os.walk(directory):
        for f in files:
            full_dir = os.path.join(root, f)
            try:
                with open(full_dir, 'rb') as file:
                    print(full_dir)
                    f_hash = hashlib.sha3_512(file.read(20000)).hexdigest()
                    df = pd.concat([df, pd.DataFrame.from_records([{'path': fr'{full_dir}', 'hash': f_hash}])],
                                   ignore_index=True)
            except IOError as e:
                debug_log.write(fr'{full_dir}: {e}' '\n')

    buffer = StringIO()

    df.info(memory_usage="deep", buf=buffer)  # Get data frame info
    debug_log.write(f'Scanning ended: {datetime.now().strftime("%d.%m.%Y_%H.%M.%S.%f")}\n {buffer}')
    debug_log.close()

    return df


def start_scan() -> pd.DataFrame:

    if not os.path.isdir('log'):  # make log directory if it`s not exist
        os.mkdir('log')

    if not os.path.isdir('files'):  # make files directory if it`s not exist
        os.mkdir('files')

    dirs = get_all_directories()  # Get list of all catalogs

    with ThreadPoolExecutor(max_workers=len(dirs)) as pool:
        result = pd.concat(pool.map(scan_files, dirs))

    return drop_uniques(result, 'hash', keep_test_col=False)
