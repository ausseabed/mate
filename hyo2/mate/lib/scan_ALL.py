from collections import namedtuple
from copy import copy
from datetime import *
from geojson import Feature, Point, FeatureCollection, LineString
from geojson.mapping import to_mapping
from typing import List, Dict
import functools
import os
import struct
import pyall

from hyo2.mate.lib.scan import Scan, A_NONE, A_PARTIAL, A_FULL, A_FAIL, A_PASS
from hyo2.mate.lib.scan import ScanState, ScanResult


class ScanALL(Scan):
    '''
    A Scan object that contains check information on the contents of a Kongsberg .all file
      :param file_path: The file path to the .all file
      :type file_path: str
    
    '''

    def __init__(self, file_path):
        Scan.__init__(self, file_path)
        self.reader = open(self.file_path, 'rb')
        self.all_reader = pyall.ALLReader(self.file_path)

    def get_size_n_pings(self, pings):
        '''
        return bytes in the file which contain specified
        number of pings
        '''
        c_bytes = 0
        result = {}
        self.all_reader.rewind()

        while self.all_reader.readDatagramHeader():
            # read datagram header
            header = self.all_reader.readDatagramHeader()
            num_bytes, stx, dg_type, em_model, record_date, record_time, \
                counter, serial_number = header

            dg_type, datagram = self.all_reader.readDatagram()

            c_bytes += num_bytes
            if dg_type in ['D', 'X', 'F', 'f', 'N', 'S', 'Y']:
                if dg_type not in result.keys():
                    result[dg_type] = {
                        'seqNo': counter,
                        'count': 0,
                    }
                if counter > result[dg_type]['seqNo']:
                    result[dg_type]['count'] += 1
                    if result[dg_type]['count'] > pings:
                        break
                result[dg_type]['seqNo'] = counter
            else:
                return None
        return c_bytes

    def scan_datagram(self, progress_callback=None):
        '''scan data to extract basic information for each type of datagram'''

        # summary type information stored in plain dict
        self.scan_result = {}
        # datagram objects
        self.datagrams = {}
        while self.all_reader.moreData():
            # update progress
            self.progress = 1.0 - (self.all_reader.moreData() / self.file_size)
            if progress_callback is not None:
                progress_callback(self.progress)

            # read datagram header
            header = self.all_reader.readDatagramHeader()
            num_bytes, stx, dg_type, em_model, record_date, record_time, \
                counter, serial_number = header

            time_stamp = pyall.to_DateTime(record_date, record_time)

            dg_type, datagram = self.all_reader.readDatagram()

            if dg_type not in self.scan_result.keys():
                self.scan_result[dg_type] = copy(self.default_info)
                self.scan_result[dg_type]['_seqNo'] = None

            # save datagram info
            self.scan_result[dg_type]['byteCount'] += num_bytes
            self.scan_result[dg_type]['recordCount'] += 1
            if self.scan_result[dg_type]['startTime'] is None:
                self.scan_result[dg_type]['startTime'] = time_stamp
            self.scan_result[dg_type]['stopTime'] = time_stamp

            if dg_type == 'I':
                # Instrument parameters
                # we care about this datagram so read its contents
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'h':
                # height
                if 'h' not in self.datagrams:
                    # only read the first h datagram, this is all we need
                    # to check the height type (assuming all h datagrams)
                    # share the same type
                    datagram.read()
                    self._push_datagram(dg_type, datagram)
            elif dg_type == 'R':
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'A':
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'n':
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'P':
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'G':
                # problem
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'U':
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'D':
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'X':
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'F':
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'f':
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'N':
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'S':
                datagram.read()
                self._push_datagram(dg_type, datagram)
            elif dg_type == 'Y':
                datagram.read()
                self._push_datagram(dg_type, datagram)

            if dg_type in ['D', 'X', 'F', 'f', 'N', 'S', 'Y']:
                this_count = counter
                last_count = self.scan_result[dg_type]['_seqNo']
                if last_count is None:
                    last_count = this_count
                if this_count - last_count >= 1:
                    self.scan_result[dg_type]['missedPings'] += \
                        this_count - last_count - 1
                    self.scan_result[dg_type]['pingCount'] += 1
                self.scan_result[dg_type]['_seqNo'] = this_count
        return

    def get_installation_parameters(self):
        '''
        Gets the decoded contents of the I datagram (installation parameters)
        as a Python dict. If multiple I datagrams are included only decoded
        parameters from the first will be included.
        Will return None if datagram not present in *.all file
        '''
        if 'I' not in self.datagrams:
            return None

        installationParametersDatagrams = self.datagrams['I']
        for installationParametersDatagram in installationParametersDatagrams:
            # skip datagrams with no params
            if len(installationParametersDatagram.installationParameters) == 0:
                continue
            return installationParametersDatagram.installationParameters

        return None

    def get_datagram_format_version(self):
        '''
        gets the version of the datagram format used by this file
        '''
        rec = self.get_installation_parameters()
        dsv = None
        if rec is not None and 'DSV' in rec.keys():
            dsv = rec['DSV']
        return dsv
    
    def get_active_sensors(self):
        installationParameters = self.get_installation_parameters()
        activeSensors = {'Position': installationParameters['APS'],
                         'RollPitch': installationParameters['ARO'],
                         'Heave': installationParameters['AHE'],
                         'Heading': installationParameters['AHS'],
                         'Attvel': installationParameters['VSN']}
        return activeSensors

    def filename_changed(self) -> ScanResult:
        '''
        Check if the filename is different from what recorded
        in the datagram. Requires I - Installation Parameters datagram
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''
        if 'I' not in self.datagrams:
            # then there's no way to check, so fail test
            return ScanResult(
                state=ScanState.FAIL,
                messages=[
                    "'I' datagram not found, cannot extract original filename"]
            )

        base_fn = os.path.basename(self.file_path)

        found_filenames = []
        installationParametersDatagrams = self.datagrams['I']
        for installationParametersDatagram in installationParametersDatagrams:

            if 'RFN' not in installationParametersDatagram.installationParameters:
                # then filename hasn't been included
                continue

            rfn = installationParametersDatagram.installationParameters['RFN']
            found_filenames.append(rfn)

            matched = base_fn == rfn
            if matched:
                return ScanResult(state=ScanState.PASS)

        msg = (
            "Filename {} did not match any filenames specified within the "
            "installation parameters (I) datagram. The following were found; "
            "{}".format(base_fn, ', '.join(found_filenames))
        )

        # then none of the installation parameter datagrams included the
        # filename that this data has been loaded from.
        return ScanResult(
            state=ScanState.FAIL,
            messages=[msg],
            data={'found_filenames': found_filenames}
        )

    def date_match(self) -> ScanResult:
        '''
        Compare the date as in the I datagram and the date as written
        in the filename
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''
        dt = 'unknown'

        if 'I' not in self.datagrams:
            # then there's no way to check, so fail test
            return ScanResult(
                state=ScanState.FAIL,
                messages=[
                    "'I' datagram not found, cannot extract startTime"]
            )

        base_fn = os.path.basename(self.file_path)

        installationParametersDatagrams = self.datagrams['I']
        # assume we just use the first one we find
        rec = installationParametersDatagrams[0]

        found = str(rec.RecordDate) in base_fn

        if found:
            return ScanResult(state=ScanState.PASS)
        else:
            msg = (
                "Could not find record date {} in filename"
                .format(rec.RecordDate)
            )
            return ScanResult(
                state=ScanState.FAIL,
                messages=[msg]
            )

    def bathymetry_availability(self) -> ScanResult:
        '''
        Checks the contents of a Kongsberg .all file for all required datagrams when bathymetry processing
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''

        # define a named tuple for bathy datagrams. Provides a means to define
        # then reference this info.
        Datagram = namedtuple(
            'Datagram',
            'id critical error_message alternatives'
        )

        bathy_datagrams = [
            Datagram(
                'I',
                False,
                "Warning: installation parameters are missing please ensure that you have your lever arms and vessel frame parameters collected elsewhere.",
                []
            ),
            Datagram(
                'R',
                False,
                "Warning: runtime parameters are missing these are critical for backscatter processing streams.  If just collecting bathymetry, please consider other users of this data.",
                []
            ),
            Datagram(
                'A',
                True,
                "Critical: runtime parameters are missing these are critical for backscatter processing streams.  If just collecting bathymetry, please consider other users of this data.",
                []
            ),
            Datagram(
                'n',
                False,
                "Warning: your network attitude and velocity is not being logged.  If you intend working in deeper water with a frequency modulated chirp you will need to interface this data.",
                []
            ),
            Datagram(
                'P',
                True,
                "Critical: position data missing, you will not be able to process this data without ensuring this data is being collected.",
                []
            ),
            Datagram(
                'G',
                False,
                "Warning: surface sound velocity data is missing, ensure your sensor is working or collect as many profiles as possible to attempt to compensate.",
                []
            ),
            Datagram(
                'U',
                False,
                "Warning: no sound velocity profile data has been collected in the raw file, please ensure you are collecting this data elsewhere.",
                []
            ),
            Datagram(
                'D',
                False,
                "Warning: neither datagram 'D' or 'X' were found, processed depth information is missing.",
                ['X']
            ),
            Datagram(
                'F',
                True,
                "Critical: neither datagram 'F', 'f' or 'N' were found. Critical range and angle data missing, you are not collecting the data required for post processing.  If you are collecting processed depths it is possible to back process however it is not desirable and is a complex process.",
                ['f', 'N']
            )
        ]

        return self.__result_from_datagram_presence(bathy_datagrams)

    def backscatter_availability(self) -> ScanResult:
        '''
        Checks the contents of a Kongsberg .all file for all required datagrams when backscatter processing
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''

        # define a named tuple for datagrams. Provides a means to define
        # then reference this info.
        Datagram = namedtuple(
            'Datagram',
            'id critical error_message alternatives'
        )

        bs_datagrams = [
            Datagram(
                'S',
                True,
                "Critical: backscatter information is missing ('S' or 'Y' datagram).  You will not be able to process backscatter without seabed image data.  If you intend processing backscatter check your setup.",
                ['Y']
            )
        ]
        return self.__result_from_datagram_presence(bs_datagrams)

    def ray_tracing_availability(self) -> ScanResult:
        '''
        Checks the contents of a Kongsberg .all file for all required datagrams when recalculating ray tracing
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''

        # define a named tuple for datagrams. Provides a means to define
        # then reference this info.
        Datagram = namedtuple(
            'Datagram',
            'id critical error_message alternatives'
        )

        rt_datagrams = [
            Datagram(
                'I',
                False,
                "Warning: installation parameters are missing please ensure that you have your lever arms and vessel frame parameters collected elsewhere.",
                []
            ),
            Datagram(
                'R',
                False,
                "Warning: runtime parameters are missing these are critical for backscatter processing streams.  If just collecting bathymetry, please consider other users of this data.",
                []
            ),
            Datagram(
                'A',
                True,
                "Critical: runtime parameters are missing these are critical for backscatter processing streams.  If just collecting bathymetry, please consider other users of this data.",
                []
            ),
            Datagram(
                'n',
                False,
                "Warning: your network attitude and velocity is not being logged.  If you intend working in deeper water with a frequency modulated chirp you will need to interface this data.",
                []
            ),
            Datagram(
                'P',
                True,
                "Critical: position data missing, you will not be able to process this data without ensuring this data is being collected.",
                []
            ),
            Datagram(
                'G',
                False,
                "Warning: surface sound velocity data is missing, ensure your sensor is working or collect as many profiles as possible to attempt to compensate.",
                []
            ),
            Datagram(
                'U',
                False,
                "Warning: no sound velocity profile data has been collected in the raw file, please ensure you are collecting this data elsewhere.",
                []
            ),
            Datagram(
                'F',
                True,
                "Critical: neither datagram 'F', 'f' or 'N' were found. Critical range and angle data missing, you are not collecting the data required for post processing.  If you are collecting processed depths it is possible to back process however it is not desirable and is a complex process.",
                ['f', 'N']
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

        present_datagrams = self.datagrams.keys()

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
                    lambda a,b : (b in present_datagrams) or a, required_datagram.alternatives,
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

    def is_missing_pings_tolerable(self, thresh=1.0):
        '''
        check for the number of missing pings in all multibeam
        data datagrams (D or X, F or f or N, S or Y)
        (allow the difference <= 1%)
        return: True/False
        '''
        for d_type in ['D', 'X', 'F', 'f', 'N', 'S', 'Y']:
            if d_type in self.scan_result.keys():
                rec = self.scan_result[d_type]
                if rec['pingCount'] == 0:
                    continue
                if rec['missedPings'] * 100.0 / rec['pingCount'] > thresh:
                    return False
        return True

    def has_minimum_pings(self, thresh=10):
        '''
        check if we have minimum number of requied pings in all multibeam
        data datagrams (D or X, F or f or N, S or Y)
        (minimum number is 10)
        return: True/False
        '''
        for d_type in ['D', 'X', 'F', 'f', 'N', 'S', 'Y']:
            if d_type in self.scan_result.keys():
                rec = self.scan_result[d_type]
                if rec['pingCount'] < thresh:
                    return False
        return True

    def ellipsoid_height_availability(self) -> ScanResult:
        '''
        check the presence of the datagrams h and the height type is 0 (The
        height is derived from the GGK or GGA datagram and is the height of
        the water level at the vertical datum (possibly motion corrected).)
        All parsed 'h' datagrams must have the height type of 0 to pass. BUT
        currently only the first 'h' datagram is read (for performance reasons)
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`        
        '''

        if 'h' not in self.datagrams:
            # then there's no way to check, so fail test
            return ScanResult(
                state=ScanState.FAIL,
                messages=["'h' datagram not found"]
            )

        height_datagrams = self.datagrams['h']
        first_height_datagram = height_datagrams[0]
        if first_height_datagram.HeightType == 0:
            return ScanResult(
                state=ScanState.PASS
            )
        else:
            return ScanResult(
                state=ScanState.FAIL,
                messages=[(
                    "Height 'h' datagram was included but HeightType did not "
                    "match expected value of 0 (found {})"
                    .format(first_height_datagram.HeightType)
                )]
            )

    def ellipsoid_height_setup(self) -> ScanResult:
        '''
        Decode the raw n - network attitude and velocity input data to try determine positioning system
        in use.  This is done for the active system only
        
        If positioning system is able to be determined decode raw P - position data input to check
        the correct positioning string is interfaced that contains ellipsoid heights.  This is done
        for the active system only
        
        
        Example is that if a PosMV or F180 is interfaced the raw position string
        needs to be a GGK message
        
        
        if a seapath binary message is interfaced then there is no way to determine
        which positioning system is interfaced and the user will need to check documentation
        to ensure the string interfaced for positioning contains ellipsoid heights

        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''

        if 'n' not in self.datagrams:
            return ScanResult(
                state=ScanState.WARNING,
                messages="Unable to conduct height setup check.  No network attitude and velocity datagrams",
                data={})

        activeSensors = self.get_active_sensors()
        for datagram in self.datagrams['n']:
            if int(format(datagram.SystemDescriptor, '08b')[2:4],2) == int(activeSensors['Attvel'])+1:
                inputtelegramsize = datagram.Attitude[0][6]
                break
        for datagram in self.datagrams['P']:
            if int(format(datagram.Descriptor, '08b')[6:],2) == int(activeSensors['Position'])+1:
                rawposinput = datagram.data[2:5].decode("utf-8")
                break
                
        if inputtelegramsize == 137:
            inertialSystem = 'PosMV'
        elif inputtelegramsize == 45 or inputtelegramsize == 43:
            inertialSystem = 'Other'
        else:
            intertialSystem = 'F180'

        data = {
            'inertial_pos_system': inertialSystem,
            'pos_string': rawposinput
        }

        if inertialSystem == 'PosMV' and rawposinput == 'GGK' or \
                inertialSystem == 'F180' and rawposinput == 'GGK':
            return ScanResult(
                state=ScanState.PASS,
                messages='',
                data=data
            )
        elif inertialSystem == 'Other':
            return ScanResult(
                state=ScanState.WARNING,
                messages="Unable to determine positioning system.  Check position input contains ellipsoid heights",
                data=data
            )
        else:
            return ScanResult(
                state=ScanState.FAIL,
                messages="Ellipsoid heights not being logged change your position input to a GGK string",
                data=data
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
        Extracts runtime parameters for inclusion in the `data` dict.
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''

        if 'R' not in self.datagrams:
            return ScanResult(
                state=ScanState.WARNING,
                messages="Runtime parameters datagram (R) not found in file",
                data={})

        r_datagrams = self.datagrams['R']
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
        Extracts positions from position datagram. Scan result includes
        geojson line definition comprised of these unique positions.
        
        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''

        if 'P' not in self.datagrams:
            return ScanResult(
                state=ScanState.WARNING,
                messages="Posistion datagram (P) not found in file",
                data={})

        p_datagrams = self.datagrams['P']
        position_points = []
        last_point = None
        for p_datagram in p_datagrams:
            pt = Point([p_datagram.Longitude, p_datagram.Latitude])
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
        Extracts installation parameters from datagram (of same name).
        '''

        if 'I' not in self.datagrams:
            return ScanResult(
                state=ScanState.FAIL,
                messages=(
                    "Installation Parameters datagram (I) not found in file"),
                data={})

        ips = self.get_installation_parameters()
        if ips is None:
            return ScanResult(
                state=ScanState.FAIL,
                messages=(
                    "Failed to find Installation Parameters datagram (I) that "
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
