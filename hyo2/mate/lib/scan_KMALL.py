from collections import namedtuple
from copy import copy
from datetime import *
from geojson import Feature, Point, FeatureCollection, LineString
from geojson.mapping import to_mapping
from typing import List, Dict
import functools
import os
import struct
from KMALL import kmall

from hyo2.mate.lib.scan import Scan, A_NONE, A_PARTIAL, A_FULL, A_FAIL, A_PASS
from hyo2.mate.lib.scan import ScanState, ScanResult


class ScanKMALL(Scan):
    '''
    A Scan object that contains check information on the contents of a Kongsberg .all file

    :param file_path: The file path to the .all file
    :type file_path: str
    '''

    def __init__(self, file_path):
        Scan.__init__(self, file_path)
        self.reader = open(self.file_path, 'rb')
        self.kmall_reader = kmall(self.file_path)

    def scan_datagram(self, progress_callback=None):
        '''scan data to extract basic information for each type of datagram'''

        # summary type information stored in plain dict
        self.scan_result = {}
        # datagram objects
        self.datagrams = {}
        while not self.kmall_reader.eof:
            # update progress
            if self.kmall_reader.FID is not None:
                self.progress = 1.0 - ((self.kmall_reader.file_size - self.kmall_reader.FID.tell()) / self.kmall_reader.file_size)
                if progress_callback is not None:
                    progress_callback(self.progress)

            # read datagram information
            self.kmall_reader.decode_datagram()
            self.kmall_reader.read_datagram()
            numBytesDgm, dgmType, dgmVersion, dgm_version, systemID, \
                dgtime, dgdatetime = self.kmall_reader.datagram_data['header'].values()
            dg_type = dgmType.decode('utf-8').strip('#')

            if dg_type not in self.scan_result.keys():
                self.scan_result[dg_type] = copy(self.default_info)
                self.scan_result[dg_type]['_seqNo'] = None

            self.scan_result[dg_type]['byteCount'] += numBytesDgm
            self.scan_result[dg_type]['recordCount'] += 1
            if self.scan_result[dg_type]['startTime'] is None:
                self.scan_result[dg_type]['startTime'] = dgdatetime
            self.scan_result[dg_type]['stopTime'] = dgdatetime

            if dgmType == b'#IIP':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#IOP':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#IBE':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data['BISTText'])
            if dgmType == b'#IBR':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data['BISTText'])
            if dgmType == b'#IBS':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data['BISTText'])
            if dgmType == b'#MRZ':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#MWC':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#SPO':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#SKM':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#SVP':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#SVT':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#SCL':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#SDE':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#SHI':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#CPO':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#CHE':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)
            if dgmType == b'#FCF':
                self._push_datagram(dg_type, self.kmall_reader.datagram_data)

        filename, totalpings, NpingsMissed, MissingMRZCount = self.kmall_reader.check_ping_count()
        self.scan_result['MRZ']['missedPings'] = NpingsMissed
        self.scan_result['MRZ']['pingCount'] = totalpings
        self.scan_result['MRZ']['missingPackets'] = MissingMRZCount
        return

    def get_installation_parameters(self):
        '''
        Gets the decoded contents of the IIP datagram (installation parameters)
        as a Python dict. If multiple IIP datagrams are included only decoded
        parameters from the first will be included.
        Will return None if datagram not present in *.kmall file
        '''
        if 'IIP' not in self.datagrams:
            return None

        installationParametersDatagrams = self.datagrams['IIP']
        for installationParametersDatagram in installationParametersDatagrams:
            # skip datagrams with no params
            if len(installationParametersDatagram['install_txt']) == 0:
                continue
            return installationParametersDatagram['install_txt']

        return None

    def installation_parameters(self) -> ScanResult:
        '''
        Gets the installation parameters in a ScanResult format
        '''

        ip = self.get_installation_parameters()

        if ip is None:
            return ScanResult(
                state=ScanState.FAIL,
                messages=(
                    "Installation Parameters datagram not found in file"),
                data={})

        data = {}
        for ip_param_name, ip_param_value in ip.items():
            # todo: in future we'll need to perform some translations between
            # the raw field names and a standardised version across all
            # file formats
            data[ip_param_name] = ip_param_value

        return ScanResult(
            state=ScanState.PASS,
            messages=[],
            data=data
        )

    def get_active_sensors(self):
        installationParameters = self.get_installation_parameters()
        active = []
        for key, value in installationParameters.items():
            if value == 'ACTIVE':
                active.append(key)
        for sensor in active:
            if 'position' in sensor:
                activeSensors = {'Position': sensor[:-15]}
            if 'motion' in sensor:
                activeSensors['RollPitch'] = sensor[:-15]
                activeSensors['Heave'] = sensor[:-15]
                activeSensors['Heading'] = sensor[:-15]
                activeSensors['Attvel'] = sensor[:-15]
        return activeSensors

    def filename_changed(self) -> ScanResult:
        '''
        Check if the filename is different from what recorded
        in the datagram. Requires I - Installation Parameters datagram

        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''
        return ScanResult(
            state=ScanState.WARNING,
            messages=["Check not implemented for GSF format"]
        )

    def date_match(self) -> ScanResult:
        '''
        Compare the date as in the IIP datagram and the date as written
        in the filename

        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''

        if 'IIP' not in self.datagrams:
            # then there's no way to check, so fail test
            return ScanResult(
                state=ScanState.FAIL,
                messages=[
                    "'I' datagram not found, cannot extract startTime"]
            )

        base_fn = os.path.basename(self.file_path)

        installationParametersDatagrams = self.datagrams['IIP']
        # assume we just use the first one we find
        rec = installationParametersDatagrams[0]['header']

        found = rec['dgdatetime'].strftime('%Y%m%d') in base_fn

        if found:
            return ScanResult(state=ScanState.PASS)
        else:
            msg = (
                "Could not find record date {} in filename"
                .format(rec['dgdatetime'])
            )
            return ScanResult(
                state=ScanState.FAIL,
                messages=[msg]
            )

    def bathymetry_availability(self) -> ScanResult:
        '''
        Checks the contents of a Kongsberg .kmall file for all required datagrams when bathymetry processing

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
                'IIP',
                False,
                "Warning: installation parameters are missing please ensure that you have your lever arms and vessel frame parameters collected elsewhere.",
                []
            ),
            Datagram(
                'IOP',
                False,
                "Warning: runtime parameters are missing these are critical for backscatter processing streams.  If just collecting bathymetry, please consider other users of this data.",
                []
            ),
            Datagram(
                'SKM',
                True,
                "Critical: runtime parameters are missing these are critical for backscatter processing streams.  If just collecting bathymetry, please consider other users of this data.",
                []
            ),
            Datagram(
                'SPO',
                True,
                "Critical: position data missing, you will not be able to process this data without ensuring this data is being collected.",
                []
            ),
            Datagram(
                'SVT',
                False,
                "Warning: surface sound velocity data is missing, ensure your sensor is working or collect as many profiles as possible to attempt to compensate.",
                []
            ),
            Datagram(
                'SVP',
                False,
                "Warning: no sound velocity profile data has been collected in the raw file, please ensure you are collecting this data elsewhere.",
                []
            ),
            Datagram(
                'MRZ',
                False,
                "Warning: neither datagram 'D' or 'X' were found, processed depth information is missing.",
                []
            )
        ]

        return self.__result_from_datagram_presence(bathy_datagrams)

    def backscatter_availability(self) -> ScanResult:
        '''
        Checks the contents of a Kongsberg .kmall file for all required datagrams when backscatter processing

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
                'MRZ',
                True,
                "Critical: backscatter information is missing ('S' or 'Y' datagram).  You will not be able to process backscatter without seabed image data.  If you intend processing backscatter check your setup.",
                []
            )
        ]
        return self.__result_from_datagram_presence(bs_datagrams)

    def ray_tracing_availability(self) -> ScanResult:
        '''
        Checks the contents of a Kongsberg .kmall file for all required datagrams when recalculating ray tracing

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
                'IIP',
                False,
                "Warning: installation parameters are missing please ensure that you have your lever arms and vessel frame parameters collected elsewhere.",
                []
            ),
            Datagram(
                'IOP',
                False,
                "Warning: runtime parameters are missing these are critical for backscatter processing streams.  If just collecting bathymetry, please consider other users of this data.",
                []
            ),
            Datagram(
                'SKM',
                True,
                "Critical: runtime parameters are missing these are critical for backscatter processing streams.  If just collecting bathymetry, please consider other users of this data.",
                []
            ),
            Datagram(
                'SPO',
                True,
                "Critical: position data missing, you will not be able to process this data without ensuring this data is being collected.",
                []
            ),
            Datagram(
                'SVT',
                False,
                "Warning: surface sound velocity data is missing, ensure your sensor is working or collect as many profiles as possible to attempt to compensate.",
                []
            ),
            Datagram(
                'SVP',
                False,
                "Warning: no sound velocity profile data has been collected in the raw file, please ensure you are collecting this data elsewhere.",
                []
            ),
            Datagram(
                'MRZ',
                True,
                "Critical: neither datagram 'F', 'f' or 'N' were found. Critical range and angle data missing, you are not collecting the data required for post processing.  If you are collecting processed depths it is possible to back process however it is not desirable and is a complex process.",
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

        present_datagrams = self.datagrams.keys()

        all_critical = True
        all_noncritical = True
        missing_critical = []
        missing_noncritical = []
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

        # include a lists of missing datagrams in result object
        data = {
            'missing_critical': missing_critical,
            'missing_noncritical': missing_noncritical
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
                messages=messages
            )

    def is_missing_pings_tolerable(self, thresh=1.0):
        '''
        check for the number of missing pings in all multibeam
        data datagrams (D or X, F or f or N, S or Y)
        (allow the difference <= 1%)
        return: True/False
        '''
        for d_type in ['MRZ']:
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
        for d_type in ['MRZ']:
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

        if 'SHI' not in self.datagrams:
            # then there's no way to check, so fail test
            return ScanResult(
                state=ScanState.FAIL,
                messages=["'SHI' datagram not found"]
            )

        height_datagrams = self.datagrams['SHI']
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

        if 'IIP' not in self.datagrams:
            return ScanResult(
                state=ScanState.WARNING,
                messages="Unable to conduct height setup check.  No installation parameters datagrams",
                data={})

        activeSensors = self.get_active_sensors()
        motionSensorFmt = self.get_installation_parameters()[activeSensors['Attvel'] + '_data_format']
        rawposinput = self.get_installation_parameters()[activeSensors['Position'] + '_data_format']

        if motionSensorFmt == 'POS MV GRP 102/103':
            inertialSystem = 'PosMV'
        else:
            inertialSystem = 'Other'

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
            tmi_time = to_merge_item["dgtime"]
            while position is not None and position["header"]["dgtime"] <= tmi_time:
                position_prev = position
                try:
                    position = next(position_iterator)
                except StopIteration:
                    break

            to_merge_item["Latitude"] = position_prev['sensorData']['correctedLat_deg']
            to_merge_item["Longitude"] = position_prev['sensorData']['correctedLong_deg']

    def runtime_parameters(self) -> ScanResult:
        '''
        Extracts runtime parameters for inclusion in the `data` dict.

        :return: :class:`hyo2.mate.lib.scan.ScanResult`
        '''

        if 'IOP' not in self.datagrams:
            return ScanResult(
                state=ScanState.WARNING,
                messages="Runtime parameters datagram (IOP) not found in file",
                data={})

        r_datagrams = self.datagrams['IOP']
        runtime_parameters = []
        for r_datagram in r_datagrams:
            time = r_datagram['header']['dgtime']
            clone = r_datagram.copy()
            clone['runtime_txt'].update({'dgtime': time})
            runtime_parameters.append(clone['runtime_txt'])

        data = {}

        if 'SPO' in self.datagrams:
            # then we can build a point based dataset of where the parameters
            # changed
            position_datagrams = self.datagrams['SPO']
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

        if 'SPO' not in self.datagrams:
            return ScanResult(
                state=ScanState.WARNING,
                messages="Position datagram (SPO) not found in file",
                data={})

        p_datagrams = self.datagrams['SPO']
        position_points = []
        last_point = None
        for p_datagram in p_datagrams:
            pt = Point([p_datagram['sensorData']['correctedLong_deg'], p_datagram['sensorData']['correctedLat_deg']])
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
