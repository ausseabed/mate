from copy import copy
from datetime import *
import os
import struct
import pyall

from hyo2.mate.lib.scan import Scan, A_NONE, A_PARTIAL, A_FULL, A_FAIL, A_PASS


class ScanALL(Scan):
    '''scan an ALL file and provide some indicators of the contents'''

    def __init__(self, file_path):
        Scan.__init__(self, file_path)
        self.reader = open(self.file_path, 'rb')
        self.all_reader = pyall.ALLReader(self.file_path)

    def get_size_n_pings(self, pings):
        '''
        return bytes in the file which containg specified
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
        return c_bytes

    def __push_datagram(self, datagram_char, datagram):
        '''
        util function to cut boiler plate code on adding to the datagrams dict
        :param datagram_char: The single character identifier for this
            datagram. eg; `I`, `h`
        :param datagram: The datagram object
        '''

        # get the datagram name if it exists otherwise use the character id
        name = self.all_reader.getDatagramName(datagram_char)
        if name is None:
            name = datagram_char

        # keep a list of datagrams in the datagram dict. If one hasn't been
        # added with this name yet create a new array
        if name not in self.datagrams:
            self.datagrams[name] = []
        self.datagrams[name].append(datagram)

    def scan_datagram(self, progress_callback=None):
        '''scan data to extract basic information for each type of datagram'''

        # summary type information stored in plain dict
        self.scan_result = {}
        # datagram objects
        self.datagrams = {}
        while self.all_reader.moreData():
            # read datagram header
            header = self.all_reader.readDatagramHeader()

            num_bytes, stx, dg_type, em_model, record_date, record_time, \
                counter, serial_number = header
            time_stamp = pyall.to_DateTime(record_date, record_time)

            dg_type, datagram = self.all_reader.readDatagram()
            # print( (dg_type, datagram) )

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
                self.__push_datagram(dg_type, datagram)
            elif dg_type == 'h':
                # height
                if 'h' not in self.datagrams:
                    # only read the first h datagram, this is all we need
                    # to check the height type (assuming all h datagrams)
                    # share the same type
                    datagram.read()
                    self.__push_datagram(dg_type, datagram)
            elif dg_type in ['D', 'X', 'F', 'f', 'N', 'S', 'Y']:
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
        if 'I_Installation_Start' not in self.datagrams:
            return None

        installationParametersDatagrams = self.datagrams['I_Installation_Start']
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
        rec = self.get_datagram_info('I')
        dsv = None
        if rec is not None and 'DSV' in rec['other'].keys():
            dsv = rec['other']['DSV']
        return dsv

    def is_filename_changed(self):
        '''
        check if the filename is different from what recorded
        in the datagram. Requires `I` datagram
        '''
        if 'I_Installation_Start' not in self.datagrams:
            # then there's no way to check, so fail test
            return False

        installationParametersDatagrams = self.datagrams['I_Installation_Start']
        for installationParametersDatagram in installationParametersDatagrams:

            if 'RFN' not in installationParametersDatagram.installationParameters:
                # then filename hasn't been included
                continue

            rfn = installationParametersDatagram.installationParameters['RFN']
            matched = os.path.basename(self.file_path) != rfn
            if matched:
                return True

        # then none of the installation parameter datagrams included the
        # filename that this data has been loaded from.
        return False

    def is_date_match(self):
        '''
        compare the date as in the datagram I and the date as written
        in the filename recorded in the datagram I
        return: True/False
        '''
        dt = 'unknown'
        rec = self.get_datagram_info('I')
        if rec is not None:
            dt = rec['startTime'].strftime('%Y%m%d')
        return dt in os.path.basename(self.file_path)

    def bathymetry_availability(self):
        '''
        check the presence of all required datagrams for bathymetry processing
        (I, R, D or X, A, n, P, h, F or f or N, G, U)
        return: 'None'/'Partial'/'Full'
        '''
        presence = self.scan_result.keys()
        part1 = all(i in presence for i in ['I', 'R', 'A', 'n', 'P', 'G', 'U'])
        part2 = any(i in presence for i in ['D', 'X'])
        part3 = any(i in presence for i in ['F', 'f', 'N'])
        if part1 and part2 and part3:
            return A_FULL
        partial = any(i in presence
                      for i in ['A', 'P', 'D', 'X', 'F', 'f', 'N'])
        if partial:
            return A_PARTIAL
        return A_NONE

    def backscatter_availability(self):
        '''
        check the presence of all required datagrams for backscatter processing
        (I, R, D or X, A, n, P, h, F or f or N, G, U, S or Y)
        return: 'None'/'Partial'/'Full'
        '''
        presence = self.scan_result.keys()
        result1 = self.bathymetry_availability()
        part4 = any(i in presence for i in ['S', 'Y'])
        if result1 == A_FULL and part4:
            return A_FULL
        if result1 == A_NONE and not part4:
            return A_NONE
        return A_PARTIAL

    def ray_tracing_availability(self):
        '''
        check the presence of required datagrams for ray tracing
        (I, R, A, n, P, F or f or N, G, U)
        return: True/False
        '''
        presence = self.scan_result.keys()
        part0 = all(i in presence for i in ['I', 'R', 'A', 'n', 'P',
                                            'G', 'U'])
        part3 = any(i in presence for i in ['F', 'f', 'N'])
        return part0 and part3

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

    def ellipsoid_height_availability(self):
        '''
        check the presence of the datagrams h and the height type is 0 (The
        height is derived from the GGK or GGA datagram and is the height of
        the water level at the vertical datum (possibly motion corrected).)

        All parsed 'h' datagrams must have the height type of 0 to pass. BUT
        currently only the first 'h' datagram is read (for performance reasons)

        return: True/False
        '''

        if 'h_Height' not in self.datagrams:
            # then there's no way to check, so fail test
            return False

        heightDatagrams = self.datagrams['h_Height']
        firstHeightDatagram = heightDatagrams[0]
        return firstHeightDatagram.HeightType == 0
