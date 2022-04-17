
from ctypes import windll, c_long, byref
import sys
import os
import scan


def main():
    if not windll.shell32.IsUserAnAdmin():
        windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
        exit(1)

    if os.name == 'nt':  # Disable redirection from system32 to Syswow64 on 64 bit machines if operating system is
        # Windows
        dll_hook = windll.kernel32
        wow64 = c_long(0)
        dll_hook.Wow64DisableWow64FsRedirection(byref(wow64))

    scan.scan_files().to_csv('files/duplicates.csv')  # Start scan

    input('press any key to end script')


if __name__ == '__main__':
    main()
