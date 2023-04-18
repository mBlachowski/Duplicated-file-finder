import os
from ctypes import windll
import sys
import scan


def main():

    if sys.platform == 'win32' and not windll.shell32.IsUserAnAdmin():  # check administrator permission for Windows
        windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
        exit(1)

    elif sys.platform == 'linux' and os.geteuid() != 0:  # Check permission for linux
        print('You need root privilege')
        exit(1)

    scan.start_scan().to_csv('files/duplicates.csv')  # Start scan

    input('press any key to end script')


if __name__ == '__main__':
    main()
