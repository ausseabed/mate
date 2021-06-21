from collections import namedtuple
from geojson import Feature, Point, FeatureCollection, LineString
from geojson.mapping import to_mapping
from typing import List, Dict
from copy import copy
import os
import pygsf
import functools

from hyo2.mate.lib.scan import Scan
from hyo2.mate.lib.scan import ScanState, ScanResult


class ScanGsf(Scan):
    '''
    A Scan object that contains check information on the contents of a 
    Generic Sensor Format .gsf file
    
    :param file_path: The file path to the .gsf file
    :type file_path: str
    '''

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
            #print(self.progress)
            if progress_callback is not None:
                progress_callback(self.progress)

            number_of_bytes, record_identifier, datagram = \
                self.reader.readDatagram()
            
            if record_identifier not in self.scan_result.keys():
                self.scan_result[record_identifier] = copy(self.default_info)
                self.scan_result[record_identifier]['_seqNo'] = None
                
            # save datagram info
            self.scan_result[record_identifier]['byteCount'] += number_of_bytes
            self.scan_result[record_identifier]['recordCount'] += 1

            if record_identifier == pygsf.PROCESSING_PARAMETERS:
                datagram.read()
                self._push_datagram(record_identifier, datagram)
            elif record_identifier == pygsf.SENSOR_PARAMETERS:
                datagram.read()
                self._push_datagram(record_identifier, datagram)
            elif record_identifier == pygsf.HEADER:
                datagram.read()
                self._push_datagram(record_identifier, datagram)
            elif record_identifier == pygsf.SWATH_BATHYMETRY:
                datagram.read()
                if self.scan_result[record_identifier]['startTime'] is None:
                    self.scan_result[record_identifier]['startTime'] = \
                    datagram.currentRecordDateTime()
                self.scan_result[record_identifier]['stopTime'] = \
                datagram.currentRecordDateTime()
                self._push_datagram(record_identifier, datagram)
            elif record_identifier == pygsf.SWATH_BATHY_SUMMARY:
                datagram.read()
                self._push_datagram(record_identifier, datagram)
            elif record_identifier == pygsf.ATTITUDE_DATA:
                datagram.read()
                self._push_datagram(record_identifier, datagram)
            elif record_identifier == pygsf.SOUND_VELOCITY:
                datagram.read()
                self._push_datagram(record_identifier, datagram)
        self.scan_result[pygsf.SWATH_BATHYMETRY]['pingCount'] = \
        self.reader.getrecordcount()      
        
        return
        

    def get_installation_parameters(self):
        '''
        Gets the decoded contents of the PROCESSING_PARAMETERS datagram
        as a Python dict. If multiple PROCESSING_PARAMETERS datagrams are
        included only decoded parameters from the first will be included.
        Will return None if PROCESSING_PARAMETERS not present in *.all file
        '''
        if pygsf.PROCESSING_PARAMETERS not in self.datagrams:
            return None

        installationParametersDatagrams = self.datagrams[pygsf.PROCESSING_PARAMETERS]
        for installationParametersDatagram in installationParametersDatagrams:
            # skip datagrams with no params
            if len(installationParametersDatagram.installationParameters) == 0:
                continue
            return installationParametersDatagram.installationParameters

        return None

    def filename_changed(self) -> ScanResult:
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check unable to be implemented for GSF format"]
        )

    def date_match(self) -> ScanResult:
        ''' 
        Obtains the date from the first recorded SWATH_BATHYMETRY_PING and
        checks if the date formatted as YYYYMMDD is included in the filename.
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
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
        '''
        Checks the contents of a Generic Sensor Format .gsf file
        for all required records required when bathymetry processing
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''
        Datagram = namedtuple(
            'Datagram',
            'id critical error_message alternatives'
        )

        rt_datagrams = [
            Datagram(
                'DEPTH_ARRAY',
                True,
                "Critical: 'DEPTH_ARRAY' were not found. "\
                "Critical range and angle data missing, you are not "\
                "collecting the data required for post processing.",
                []
            ),
            Datagram(
                'ACROSS_TRACK_ARRAY',
                True,
                "Critical: 'ACROSS_TRACK_ARRAY' were not found. Critical range "\
                "and angle data missing, you are not collecting the data "\
                "required for post processing.",
                []
            ),
            Datagram(
                'ALONG_TRACK_ARRAY',
                True,
                "Critical: 'ALONG_TRACK_ARRAY' were not found. Critical "\
                "data missing, you are not collecting the "\
                "data required for post processing.",
                []
            ),
            Datagram(
                'SOUND_VELOCITY_PROFILE',
                False,
                "Warning: 'SOUND_VELOCITY_PROFILE' were not found. Ensure "\
                "sound velocity profiles are being logged seperately.",
                []
            ),
            Datagram(
                'ATTITUDE',
                False,
                "Warning: 'ATTITUDE' were not found. This "\
                "data will be logging in the ping header.",
                []
            )
        ]

        return self.__result_from_datagram_presence(rt_datagrams)

    def backscatter_availability(self) -> ScanResult:
        '''
        Checks the contents of a Generic Sensor Format .gsf file
        for all required records required when backscatter processing
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''
        Datagram = namedtuple(
            'Datagram',
            'id critical error_message alternatives'
        )

        rt_datagrams = [
            Datagram(
                'MEAN_CAL_AMPLITUDE_ARRAY',
                True,
                "Critical: backscatter information is missing "\
                "('MEAN_CAL_AMPLITUDE_ARRAY' or 'MEAN_REL_AMPLITUDE_ARRAY' datagram). "\
                " You will not be able to process backscatter without seabed image data. "\
                " If you intend processing backscatter check your setup.",
                []
            ),
            Datagram(
                'MEAN_REL_AMPLITUDE_ARRAY',
                True,
                "Critical: backscatter information is missing "\
                "('MEAN_CAL_AMPLITUDE_ARRAY' or 'MEAN_REL_AMPLITUDE_ARRAY' datagram). "\
                " You will not be able to process backscatter without seabed image data. "\
                " If you intend processing backscatter check your setup.",
                []
            )
        ]

        return self.__result_from_datagram_presence(rt_datagrams)

    def ray_tracing_availability(self) -> ScanResult:
        '''
        Checks the contents of a Generic Sensor Format .gsf file
        for all required records required when re ray tracing is required
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''
        Datagram = namedtuple(
            'Datagram',
            'id critical error_message alternatives'
        )

        rt_datagrams = [
            Datagram(
                'BEAM_ANGLE_FORWARD_ARRAY',
                True,
                "Critical: 'BEAM_ANGLE_FORWARD_ARRAY' were not found. "\
                "Critical range and angle data missing, you are not "\
                "collecting the data required for post processing.  If "\
                "you are collecting processed depths it is possible to back "\
                "process however it is not desirable and is a complex process.",
                []
            ),
            Datagram(
                'BEAM_ANGLE_ARRAY',
                True,
                "Critical: 'BEAM_ANGLE_ARRAY' were not found. Critical range "\
                "and angle data missing, you are not collecting the data "\
                "required for post processing.  If you are collecting "\
                "processed depths it is possible to back process however it "\
                "is not desirable and is a complex process.",
                []
            ),
            Datagram(
                'TRAVEL_TIME_ARRAY',
                True,
                "Critical: 'TRAVEL_TIME_ARRAY' were not found. Critical "\
                "range and angle data missing, you are not collecting the "\
                "data required for post processing.  If you are collecting "\
                "processed depths it is possible to back process however it "\
                "is not desirable and is a complex process.",
                []
            ),
            Datagram(
                'SOUND_VELOCITY_PROFILE',
                False,
                "Warning: 'SOUND_VELOCITY_PROFILE' were not found. Ensure "\
                "sound velocity profiles are being logged seperately.",
                []
            ),
            Datagram(
                'ATTITUDE',
                False,
                "Warning: 'ATTITUDE' were not found. This "\
                "data will be logging in the ping header.",
                []
            )
        ]

        return self.__result_from_datagram_presence(rt_datagrams)
        
    def __result_from_datagram_presence(self, required_datagrams):
        '''
        Util function, checks the datagrams that have been read during the
        scan against the `required_datagrams` list. Critical vs Warning, and
        associated messages are extracted from the attributes of the datagram
        tuples in `required_datagrams`
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''
        # tl;dr this cuts down on amount of code that was duplicated across
        # a number of check functions
        present_datagrams = self.datagrams
        present_arrays = self.__dg_to_dict(self.datagrams[pygsf.SWATH_BATHYMETRY][0])
        # identify empty data arrays
        empty_data = [
            key
            for key in present_arrays
            if (isinstance(present_arrays[key], list) and len(present_arrays[key]) == 0)
                ]
        # remove empty arrays from present arrays
        for empty in empty_data:
            present_arrays.pop(empty)
        datagram_names = [type(val[0]).__name__ for key, val in present_datagrams.items()]
        present_datagrams = list(present_arrays.keys()) + datagram_names

        all_critical = True
        all_noncritical = True
        missing_critical = []
        missing_noncritical = []
        present = []
        messages = []

        for required_datagram in required_datagrams:
            # the bathy datagram was not read from file
            not_found = required_datagram.id not in present_datagrams
            if not_found:
                # it may be that one of the alternative datagrams exist, so
                # loop through these to see if they exist
                found_in_alts = functools.reduce(
                    lambda a,b : (b in present_datagrams) or a, \
                        required_datagram.alternatives,
                    False)
                not_found = not found_in_alts
            if not_found and required_datagram.critical:
                all_critical = False
                missing_critical.append(required_datagram.id)
                messages.append(required_datagram.error_message)
            elif not_found and not required_datagram.critical:
                all_noncritical = False
                missing_noncritical.append(required_datagram.id)
                messages.append(required_datagram.error_message)
            else:
                present.append(required_datagram.id)

        # include a lists of missing datagrams in result object
        data = {
            'missing_critical': missing_critical,
            'missing_noncritical': missing_noncritical,
            'present': present
        }

        if not all_critical:
            return ScanResult(
                state=ScanState.FAIL,
                messages=messages,
                data=data
            )
        elif not all_noncritical:
            return ScanResult(
                state=ScanState.WARNING,
                messages=messages,
                data=data
            )
        else:
            return ScanResult(
                state=ScanState.PASS,
                messages=messages,
                data=data
            )

    def has_minimum_pings(self, thresh=10):
        '''
        check if we have minimum number of required pings in all multibeam
        data records (SWATH_BATHYMETRY_PING)
        (minimum number is 10)
        return: True/False
        '''
        if self.scan_result[pygsf.SWATH_BATHYMETRY]['pingCount'] > thresh:
            return True
        return False
    
    def is_missing_pings_tolerable(self, thresh=1.0):
        '''
        check for the number of missing pings in all multibeam
        data datagrams (D or X, F or f or N, S or Y)
        (allow the difference <= 1%)
        return: True/False
        '''
        for d_type in [pygsf.SWATH_BATHYMETRY]:
            if d_type in self.scan_result.keys():
                rec = self.scan_result[d_type]
                if rec['pingCount'] == 0:
                    continue
                if rec['missedPings'] * 100.0 / rec['pingCount'] > thresh:
                    return False
        return True

    def ellipsoid_height_availability(self) -> ScanResult:
        '''
        check the presence of height records and if exists ensure the data
        in the height records is changing
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`        
        '''

        Datagram = namedtuple(
            'Datagram',
            'id critical error_message alternatives'
        )

        rt_datagrams = [
            Datagram(
                'height',
                True,
                "Critical: 'height' data was not found.",
                []
            )
        ]
            
        if self.__result_from_datagram_presence(rt_datagrams).state == ScanState.PASS:
            bool = []
            i=0
            while i+1 < len(self.datagrams[pygsf.SWATH_BATHYMETRY]):
                bool.append(self.datagrams[pygsf.SWATH_BATHYMETRY][i+1].height \
                             == self.datagrams[pygsf.SWATH_BATHYMETRY][i].height)
                i+=1
            if bool.count(bool[0]) == len(bool) and bool[0] == True:
                return ScanResult(
                state=ScanState.WARNING,
                messages="Warning: height data within this file not changing.",
                data={'value': self.datagrams[pygsf.SWATH_BATHYMETRY][0].height}
                )
            else:
                return ScanResult(
                state=ScanState.PASS
                )
        else:
            return self.__result_from_datagram_presence(rt_datagrams)
                

    def ellipsoid_height_setup(self) -> ScanResult:
        '''
        This check is unable to be implemented due to generic nature of
        gsf file.  Still outputs a warning in current version of QAX
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`        
        '''
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check unable to be implemented for GSF format"]
        )

    def __dg_to_dict(self, datagram) -> Dict:
        '''
        Utils function to convert a datagram into a python
        dict that can be serialised. The assumption is that all attributes
        not starting with "__" are json serialisable and this may not be
        the case.
        '''
        # ignore the following attributes. They are either unecessary or
        # are know to not be json serialisable.
        ignore_attrs = {
            'offset',
            'fileptr',
            'data',
            'typeOfDatagram',
            'numberOfBytes',
            'header',
            'parameters',
            'read',
        }
        dg_dict = {}
        # get a list of all the attributes
        attrs = [
            a
            for a in dir(datagram)
            if not (a.startswith('__') or a in ignore_attrs)
        ]
        # loop through each attribute and add it to the dict
        for attr in attrs:
            value = getattr(datagram, attr)
            dg_dict[attr] = value
        return dg_dict
    
    def _merge_position(self, positions: List, to_merge_list: List):
        '''
        Adds position (Latitude and Longitude) to the `to_merge` list based
        on the timestamps included in both `positions` and `to_merge`

        :param position: List of position datagrams from where the Latitude
            and Longitude will be extracted
        :param to_merge_list: List of dictionaries that will have the Latitude
            and Longitude added.
        '''
        # to keep this code efficient we assume the position datagrams
        # are ordered according to time. We match a position datagram
        # with a runtime params datagram by using the position before the
        # runtime params datagram.
        position_iterator = iter(positions)
        position = next(position_iterator)
        position_prev = position
        for to_merge_item in to_merge_list:
            tmi_time = to_merge_item["Time"]
            while position is not None and position.Time <= tmi_time:
                position_prev = position
                try:
                    position = next(position_iterator)
                except StopIteration:
                    break

            to_merge_item["Latitude"] = position_prev.Latitude
            to_merge_item["Longitude"] = position_prev.Longitude

    def runtime_parameters(self) -> ScanResult:
        '''
        Extracts SENSOR_PARAMETERS records for inclusion in the `data` dict.
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''

        if pygsf.SENSOR_PARAMETERS not in self.datagrams:
            return ScanResult(
                state=ScanState.WARNING,
                messages="Sensor parameters record not found in file",
                data={})

        r_datagrams = self.datagrams[pygsf.SENSOR_PARAMETERS]
        runtime_parameters = []
        string = ""
        for r_datagram in r_datagrams:
            if r_datagram.parameters() != string:
                dg_dict = self.__dg_to_dict(r_datagram)
                runtime_parameters.append(dg_dict)
                string = r_datagram.parameters()

        data = {}

        if 'P' in self.datagrams:
            # then we can build a point based dataset of where the parameters
            # changed
            position_datagrams = self.datagrams['P']
            self._merge_position(position_datagrams, runtime_parameters)
            map = self._to_points_geojson(runtime_parameters)
            data['map'] = map

        return ScanResult(
            state=ScanState.PASS,
            messages=(
                "{} Runtime parameter (R) datagrams found in file"
                .format(len(runtime_parameters))
            ),
            data=data
        )

    def positions(self) -> ScanResult:
        '''
        Extracts positions from SWATH_BATHYMETRY records. Scan result includes
        geojson line definition comprised of these unique positions.
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''    
    
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
        '''
        Extracts installation parameters which are contained in the 
        PROCESSING_PARAMETERS records.
        '''

        if pygsf.PROCESSING_PARAMETERS not in self.datagrams:
            return ScanResult(
                state=ScanState.FAIL,
                messages=(
                    "Processing Parameters record not found in file"),
                data={})

        ips = self.get_installation_parameters()
        if ips is None:
            return ScanResult(
                state=ScanState.FAIL,
                messages=(
                    "Failed to find Processing Parameters record that "
                    "contained data"),
                data={})

        data = {}
        for ip_param_name, ip_param_value in ips.items():
            # todo: in future we'll need to perform some translations between
            # the raw field names and a standardised version across all
            # file formats
            data[ip_param_name] = ip_param_value

        return ScanResult(
            state=ScanState.PASS,
            messages=[],
            data=data
        )
