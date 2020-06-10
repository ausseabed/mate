import pygsf

from hyo2.mate.lib.scan import Scan
from hyo2.mate.lib.scan import ScanState, ScanResult


class ScanGsf(Scan):
    '''scan a GSF file and provide some indicators of the contents'''

    def __init__(self, file_path):
        Scan.__init__(self, file_path)
        self.reader = pygsf.GSFREADER(file_path)

    def scan_datagram(self, progress_callback=None):
        '''scan data to extract basic information for each type of datagram'''

        # summary type information stored in plain dict
        self.scan_result = {}
        # datagram objects
        self.datagrams = {}
        while self.reader.moreData():
            # update progress
            self.progress = 1.0 - (self.reader.moreData() / self.file_size)
            if progress_callback is not None:
                progress_callback(self.progress)

            number_of_bytes, record_identifier, datagram = \
                self.reader.readDatagram()

            if record_identifier == pygsf.PROCESSING_PARAMETERS:
                datagram.read()
                print(datagram.installationParameters)

    def filename_changed(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )

    def date_match(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )

    def bathymetry_availability(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )

    def backscatter_availability(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )

    def ray_tracing_availability(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )

    def has_minimum_pings(self, threshold=10):
        return False

    def ellipsoid_height_availability(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )

    def ellipsoid_height_setup(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )

    def runtime_parameters(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )

    def positions(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )
