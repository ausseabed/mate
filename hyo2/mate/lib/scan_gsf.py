from geojson import Feature, Point, FeatureCollection, LineString
from geojson.mapping import to_mapping
import os
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
                self._push_datagram(record_identifier, datagram)
            elif record_identifier == pygsf.SWATH_BATHYMETRY:
                datagram.read(headeronly=True)
                self._push_datagram(record_identifier, datagram)

    def filename_changed(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )

    def date_match(self) -> ScanResult:
        ''' Obtains the date from the first recorded swath bathy datagram and
        checks if the date formatted as YYYYMMDD is included in the filename.
        '''
        if pygsf.SWATH_BATHYMETRY not in self.datagrams:
            msg = (
                "Swath bathymetry datagram (record id {}) not found in file. "
                "Unable to date information"
                .format(pygsf.SWATH_BATHYMETRY)
            )
            return ScanResult(
                state=ScanState.WARNING,
                messages=msg)

        first_swath_datagram = self.datagrams[pygsf.SWATH_BATHYMETRY][0]
        first_date = first_swath_datagram.currentRecordDateTime()
        first_date_str = first_date.strftime('%Y%m%d')

        print(first_date_str)

        base_fn = os.path.basename(self.file_path)
        found = first_date_str in base_fn

        if found:
            return ScanResult(state=ScanState.PASS)
        else:
            msg = (
                "Could not find record date {} in filename"
                .format(first_date_str)
            )
            return ScanResult(
                state=ScanState.FAIL,
                messages=[msg]
            )

    def bathymetry_availability(self) -> ScanResult:
        if pygsf.SWATH_BATHYMETRY not in self.datagrams:
            msg = (
                "Swath bathymetry datagram (record id {}) not found in file."
                .format(pygsf.SWATH_BATHYMETRY)
            )
            return ScanResult(
                state=ScanState.WARNING,
                messages=msg)
        else:
            return ScanResult(state=ScanState.PASS)

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
        if pygsf.SWATH_BATHYMETRY not in self.datagrams:
            msg = (
                "Swath bathymetry datagram (record id {}) not found in file. "
                "Unable to extract position data"
                .format(pygsf.SWATH_BATHYMETRY)
            )
            return ScanResult(
                state=ScanState.WARNING,
                messages=msg)

        p_datagrams = self.datagrams[pygsf.SWATH_BATHYMETRY]
        position_points = []
        last_point = None
        for p_datagram in p_datagrams:
            pt = Point([p_datagram.longitude, p_datagram.latitude])
            if (last_point is not None and
                    pt.coordinates[0] == last_point.coordinates[0] and
                    pt.coordinates[1] == last_point.coordinates[1]):
                # skip any points that have the same location
                continue
            position_points.append(pt)
            last_point = pt
        line = LineString(position_points)
        feature = Feature(geometry=line)
        feature_collection = FeatureCollection([feature])
        data = {'map': to_mapping(feature_collection)}

        return ScanResult(
            state=ScanState.PASS,
            messages=[],
            data=data
        )

    def installation_parameters(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )
