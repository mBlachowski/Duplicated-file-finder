import pandas as pd
import hashlib
import multiprocessing as mp
from sys import exc_info
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
    print('Starting scan')
    for i in get_drives_labels():
        dirs.extend([i+d for d in os.listdir(i)])   # Save drive letter+directory to list

    return dirs


def scan_files(directory) -> pd.DataFrame:

    df = pd.DataFrame(columns=['path', 'hash'])

    if not os.path.isdir('log'):  # make log directory if it`s not exist
        os.mkdir('log')

    if not os.path.isdir('files'):  # make files directory if it`s not exist
        os.mkdir('files')

    start_date = datetime.now().strftime('%d.%m.%Y_%H.%M.%S')
    err_log = open(f'log/files_error_{start_date}.log', 'w', encoding='utf-8')  # logging all file errors
    df_info = open(f'log/Info_{start_date}.log', 'w', encoding='utf-8')  # File to save dataframe info

    df_info.write(f'Scanning has started at: {start_date} \n')
    df_info.flush()
    print(directory)
    # for root, dirs, files in os.walk(directory):
    #     for f in files:
    #         full_dir = os.path.join(root, f)
    #         # noinspection PyBroadException
    #         try:
    #             with open(full_dir, 'rb') as file:
    #                 print(full_dir)
    #                 f_hash = hashlib.sha3_512(file.read()).hexdigest()
    #                 df = pd.concat([df, pd.DataFrame.from_records([{'path': fr'{full_dir}', 'hash': f_hash}])],
    #                                ignore_index=True)
    #         except BaseException:
    #             err_log.write(fr'{full_dir}: {exc_info()}' '\n')

    end_date = datetime.now().strftime('%d.%m.%Y_%H.%M.%S.%f')
    buffer = StringIO()
    df.info(memory_usage="deep", buf=buffer)  # Get data frame info
    df_info.write(buffer.getvalue() + '\n' + 'Scanning has been completed at: ' + end_date)  # write end date to file
    df_info.flush()

    df_info.close()
    err_log.close()

    return df


def start_scan() -> pd.DataFrame:

    dirs = get_all_directories()    # Get list of all catalogs

    pool = mp.Pool(len(dirs))
    result = pd.concat(pool.map(scan_files, dirs))

    pool.close()
    pool.join()

    return drop_uniques(result, 'hash', keep_test_col=False)
