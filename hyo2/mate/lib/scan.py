from datetime import datetime
from enum import Enum
from geojson import Feature, Point, FeatureCollection
from geojson.mapping import to_mapping
from typing import Optional, Dict, List, Any, Union
import os

A_NONE = 'None'
A_PARTIAL = 'Partial'
A_FULL = 'Full'
A_FAIL = 'Fail'
A_PASS = 'Pass'


class ScanState(str, Enum):
    '''
    Enumeration of strings representing check state pass, fail or warning
        
    '''
    
    PASS = "pass"
    FAIL = "fail"
    WARNING = "warning"


class ScanResult:
    '''
    Results of a scan check. Bundles the state pass, fail or warning with messages
    and any format specific output data
    
    :param state: Enumeration of pass, fail or warning
    :type state: :class:`hyo2.mate.lib.scan.ScanState`
    :param messages: List of messages or string representation of messages
    :type messages: List or str, optional
    :param data: Dictionary of check specific output data
    :type data: Dict, optional
    
    '''
    def __init__(
        self,
        state: ScanState,
        messages: Optional[Union[List, str]] = [],
        data: Dict = None
    ):
        self.state = state
        message_list = [messages] if isinstance(messages, str) else messages
        self.messages = message_list
        self.data = data

    def __repr__(self):
        return (
            "state {} \n"
            "messages: \n  {}\n"
            "data {} \n".format(self.state, "\n  ".join(self.messages), self.data)
        )


class Scan:
    '''abstract class to scan a raw data file'''

    file_path = None
    file_size = None
    reader = None
    progress = 0       # completed percentage (0 - 100)
    scan_result = {}

    default_info = {
        'byteCount': 0,
        'recordCount': 0,
        'pingCount': 0,
        'missedPings': 0,
        'startTime': None,
        'stopTime': None,
        'other': None,
    }

    def __init__(self, file_path):
        self.file_path = file_path
        self.file_size = 0
        if os.path.exists(file_path):
            self.file_size = os.path.getsize(file_path)

    def _time_str(self, unix_time):
        '''return time string in ISO format'''
        return datetime.utcfromtimestamp(unix_time)\
            .isoformat(timespec='milliseconds')

    def _push_datagram(self, datagram_char, datagram):
        '''
        util function to cut boiler plate code on adding to the datagrams dict
        :param datagram_char: The single character identifier for this
            datagram. eg; `I`, `h`
        :param datagram: The datagram object
        '''

        # Use the datagram id character as the key to store values in the
        # cache dict. Most users/developers are familiar with this
        name = datagram_char

        # keep a list of datagrams in the datagram dict. If one hasn't been
        # added with this name yet create a new array
        if name not in self.datagrams:
            self.datagrams[name] = []
        self.datagrams[name].append(datagram)

    def scan_datagram(self):
        '''
        scan data to extract basic information for each type of datagram
        and save to scan_result
        '''

    def get_datagram_info(self, datagram_type):
        '''return info about a specific type of datagram'''
        if datagram_type in self.scan_result.keys():
            return self.scan_result[datagram_type]
        return None

    def get_total_pings(self, datagram_type=None):
        '''return the nuber of pings'''
        if datagram_type is not None:
            if datagram_type not in self.scan_result.keys():
                return 0
            return self.scan_result[datagram_type]['pingCount']
        total = 0
        for datagram_type in self.scan_result.keys():
            total += self.scan_result[datagram_type]['pingCount']
        return total

    def get_missed_pings(self, datagram_type=None):
        '''return the nuber of missed pings'''
        if datagram_type is not None:
            if datagram_type not in self.scan_result.keys():
                return 0
            return self.scan_result[datagram_type]['missedPings']
        total = 0
        for datagram_type in self.scan_result.keys():
            total += self.scan_result[datagram_type]['missedPings']
        return total

    def total_datagram_bytes(self):
        '''return number of bytes of all datagrams'''
        total_bytes = 0
        for datagram_type in self.scan_result.keys():
            total_bytes += self.scan_result[datagram_type]['byteCount']
        return total_bytes

    def is_size_matched(self):
        '''check if number of bytes of all datagrams is equal to file size'''
        return (self.total_datagram_bytes() == self.file_size)

    def _to_points_geojson(self, items):
        '''
        Converts a list of dicts, where each dict contains a Latitude and
        Longitude into a geojson object
        '''

        features = []
        for item in items:
            pt = Point([item["Longitude"], item["Latitude"]])
            # remove the lat/lng otherwise it will be included in the geom
            # definition AND the properties for this point
            item_clone = item.copy()
            del item_clone["Longitude"]
            del item_clone["Latitude"]
            feature = Feature(geometry=pt, properties=item_clone)
            features.append(feature)

        fc = FeatureCollection(features)
        return to_mapping(fc)
