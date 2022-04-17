import pandas as pd
import hashlib
import shutil
import multiprocessing as mp
from sys import exc_info
import glob
from datetime import datetime
from io import StringIO
import os
import psutil


# def drop_uniques(datafr: pd.DataFrame, col_name: str) -> pd.DataFrame:
# try:
#   data = datafr.duplicated(subset=[col_name], keep=False)

#  for i in data.iteritems():
#     if not i[1]:
#        datafr.drop(index=i[0], inplace=True)
# except:
#   with open('log/Scan_error.log', 'a', encoding='utf-8') as s_error:
#      s_error.write(f'\n-------------------------------------\n')
#     s_error.write(f'Error while saving to duplicates:{exc_info()}')

# return datafr

def drop_uniques(datafr: pd.DataFrame, col_name: str, keep_test_col: bool) -> pd.DataFrame:
    data = pd.DataFrame(datafr.duplicated(subset=[col_name], keep=False), columns=['isDuplicated'])  # Mark uniques
    # and duplicates based on col_name

    duplicates = pd.concat([datafr, data], axis=1)

    duplicates = duplicates[duplicates.isDuplicated != 0]   # keep only duplicated rows

    if not keep_test_col:   # drop duplication test column if False
        duplicates.drop('isDuplicated', axis=1, inplace=True)

    print(duplicates)

    return duplicates


def get_drives() -> list:  # Get drives letters
    existing_drives = []
    drives_info = psutil.disk_partitions(all=True)

    for drive_info in drives_info:
        if os.path.exists(drive_info[0]):
            existing_drives.append(drive_info[0])

    return existing_drives


def parallel_scan(drive_l: str) -> pd.DataFrame:
    total, used, free = shutil.disk_usage(drive_l)  # Get disk partition info
    df = pd.DataFrame(columns=['path', 'hash'])

    # convert to Gb
    total = round(((total / 1024) / 1024) / 1024, 2)
    free = round(((free / 1024) / 1024) / 1024, 2)
    used = round(((used / 1024) / 1024) / 1024, 2)
    print(
        f'Scanning files in {drive_l}. {used}Gb is used of total {total}Gb. {free}Gb are free')

    if not os.path.isdir('log'):  # make log director if it`s not exist
        os.mkdir('log')

    start_date = datetime.now().strftime('%d.%m.%Y_%H.%M.%S')
    err_log = open(f'log/files_error_{start_date}_{drive_l[0]}.log', 'w', encoding='utf-8')  # logging all file errors
    df_info = open(f'log/Info_{start_date}_{drive_l[0]}.log', 'w', encoding='utf-8')  # File to save dataframe info
    df_info.write(f'Scanning has started at: {start_date} \n')
    df_info.flush()

    for file in glob.iglob(fr'{drive_l}\**\*.*', recursive=True):  # Scan loop
        try:
            if not os.path.islink(os.path.dirname(file)):   # Skip symlinks
                with open(file, 'rb') as f:
                    f_hash = hashlib.sha3_512(f.read(2048)).hexdigest()
                    df = pd.concat([df, pd.DataFrame.from_records([{'path': fr'{file}', 'hash': f_hash}])],
                                   ignore_index=True)
        except PermissionError:
            err_log.write(fr'{file}: Permission error:{exc_info()}' '\n')
        except ValueError:
            err_log.write(fr'{file}: Value error:{exc_info()}''\n')
        except OSError:
            err_log.write(fr'{file}: OSError:{exc_info()}''\n')
        except UnicodeEncodeError:
            err_log.write(fr'{file}: UnicodeEncodeError:{exc_info()}''\n')
        except MemoryError:
            err_log.write(fr'{file}: Memory error:{exc_info()}''\n')
            exit(2)

    end_date = datetime.now().strftime('%d.%m.%Y_%H.%M.%S.%f')
    buffer = StringIO()
    df.info(memory_usage="deep", buf=buffer)  # Get data frame info
    df_info.write(buffer.getvalue() + '\n' + 'Scanning has been completed at: ' + end_date)  # write end date to file
    df_info.flush()

    df.to_csv(fr'files\{drive_l[0]}.csv')  # write paths and hash from one partition to csv(only for debug. this may be
    # deleted in final version)

    df_info.close()
    err_log.close()

    return df


def scan_files() -> pd.DataFrame:
    drive_labels = get_drives()

    pool = mp.Pool(len(drive_labels))
    result = pd.concat(pool.map(parallel_scan, drive_labels))

    pool.close()
    pool.join()

    return drop_uniques(result, 'hash', keep_test_col=False)
