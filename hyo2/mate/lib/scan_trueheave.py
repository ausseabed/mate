import os
from pathlib import Path

from hyo2.mate.lib.scan import Scan
from hyo2.mate.lib.scan import ScanState, ScanResult


class ScanTrueheave(Scan):
    '''scan a trueheave file and provide some indicators of the contents'''

    def __init__(self, file_path):
        Scan.__init__(self, file_path)
        self.file_path = file_path

        self.file_exits = None
        self.file_non_zero_size = None

    def scan_datagram(self, progress_callback=None):
        # we would normally read the file here and cache interesting data
        # to use in the checks, but for the SVP files checking to see if they
        # exist and have a non-zero size is sufficient.

        svp_path = Path(self.file_path)
        self.file_exits = svp_path.is_file()
        self.file_non_zero_size = os.path.getsize(self.file_path)

        progress_callback(1.0)
