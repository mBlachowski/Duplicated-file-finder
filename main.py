import os
from ctypes import windll, c_long, byref
import sys
import scan


def main():

    if sys.platform == 'win32':  # Disable redirection from system32 to Syswow64 on 64 bit machines and check
        # admin permission for Windows

        if not windll.shell32.IsUserAnAdmin():
            windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
            exit(1)

        dll_hook = windll.kernel32
        wow64 = c_long(0)
        dll_hook.Wow64DisableWow64FsRedirection(byref(wow64))
    elif sys.platform == 'linux' and os.geteuid() != 0:  # Check permission for linux
        print('You need root privilege')
        raise EnvironmentError

    scan.scan_files().to_csv('files/duplicates.csv')  # Start scan

    input('press any key to end script')


if __name__ == '__main__':
    main()
