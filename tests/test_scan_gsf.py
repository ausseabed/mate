import unittest
import os
import time
from collections import namedtuple
from copy import copy
import pygsf
from hyo2.mate.lib.scan_gsf import ScanGsf
from hyo2.mate.lib import scan

TEST_FILE = "0007_20201115_101546_Investigator_em122_EM122.gsf"


class TestMateScanGsf(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_file = os.path.abspath(os.path.join(
                                         os.path.dirname(__file__),
                                         "test_data_remote", TEST_FILE))
        cls.test = ScanGsf(cls.test_file)

    def test_time_str(self):
        time_stamp = 1623823280
        self.assertEqual(self.test._time_str(time_stamp), \
                         '2021-06-16T06:01:20.000')
        
    def test_push_datagram(self):
        self.test.datagrams = {}
        datagram_char = 2
        datagram = pygsf.SWATH_BATHYMETRY_PING
        self.test._push_datagram(datagram_char, datagram)
        self.assertIsInstance(self.test.datagrams[datagram_char], list)
        self.assertIs(self.test.datagrams[datagram_char][0], pygsf.SWATH_BATHYMETRY_PING)

        
    def test_get_datagram_info(self):
        test_info = {
        'byteCount': 1500,
        'recordCount': 5,
        'pingCount': 200,
        'missedPings': 50,
        'startTime': None,
        'stopTime': None,
        'other': None,
    }
        record_identifier = 2
        self.test.scan_result[record_identifier] = copy(self.test.default_info)
        self.test.scan_result[record_identifier]['byteCount'] = 1500
        self.test.scan_result[record_identifier]['recordCount'] = 5
        self.test.scan_result[record_identifier]['pingCount'] = 200
        self.test.scan_result[record_identifier]['missedPings'] = 50
        
        self.assertEqual(self.test.get_datagram_info(record_identifier), test_info)

    # to do needs to be implemented in scan_gsf
    #def test_get_size_n_pings(self):
    #    self.test.get_size_n_pings(2)

    def test_pings(self):
        self.assertEqual(self.test.get_total_pings(2), 200)
        self.assertEqual(self.test.get_missed_pings(2), 50)
        self.assertEqual(self.test.get_total_pings('-'), 0)
        self.assertEqual(self.test.get_missed_pings('-'), 0)
        self.assertEqual(self.test.get_total_pings(), 200)
        self.assertEqual(self.test.get_missed_pings(), 50)
        self.assertFalse(self.test.is_missing_pings_tolerable())
        self.assertTrue(self.test.is_missing_pings_tolerable(26))
        self.assertTrue(self.test.has_minimum_pings())
        self.assertFalse(self.test.has_minimum_pings(201))

    def test_is_size_matched(self):
        self.test.file_size = 1500
        self.assertEqual(self.test.total_datagram_bytes(), 1500)
        self.assertTrue(self.test.is_size_matched())

    def test_filename_changed(self):
        self.assertEqual(
            self.test.filename_changed().state,
            scan.ScanState.WARNING
        )

    def test_date_match(self):
        # create test data for warning
        self.test.datagrams = {}
        self.assertEqual(
            self.test.date_match().state,
            scan.ScanState.WARNING
        )
        # create test data for pass
        datagram = pygsf.SWATH_BATHYMETRY_PING(self.test.reader.fileptr,0,2,0)
        self.test.datagrams = {2 : [datagram]}
        self.test.datagrams[2][0].time = 1605435342
        self.assertEqual(
            self.test.date_match().state,
            scan.ScanState.PASS
        )
        # create test data for fail
        self.test.datagrams[2][0].time = 1623874890
        self.assertEqual(
            self.test.date_match().state,
            scan.ScanState.FAIL
        )

    def test_bathymetry_availability(self):
        #setup data
        non_critical = {3: 'SOUND_VELOCITY_PROFILE', 12: 'ATTITUDE'}
        critical = ['DEPTH_ARRAY', 'ACROSS_TRACK_ARRAY', 'ALONG_TRACK_ARRAY']
        combined = critical + list(non_critical.values())
        sound_velocity = pygsf.SOUND_VELOCITY_PROFILE(self.test.reader.fileptr,0,3,0)
        attitude = pygsf.ATTITUDE(self.test.reader.fileptr,0,12,0)
        ping = pygsf.SWATH_BATHYMETRY_PING(self.test.reader.fileptr,0,2,0)
        self.test.datagrams = {2: [ping], 12: [attitude], 3: [sound_velocity]}
        self.test.datagrams[2][0].DEPTH_ARRAY = [2,2,2]
        self.test.datagrams[2][0].ACROSS_TRACK_ARRAY = [2,2,2]
        self.test.datagrams[2][0].ALONG_TRACK_ARRAY = [2,2,2]
        
        # test pass
        bathy_available_res = self.test.bathymetry_availability()

        self.assertEqual(
            bathy_available_res.state,
            scan.ScanState.PASS
        )
        self.assertEqual(
            bathy_available_res.data,
            {
                'missing_critical': [],
                'missing_noncritical': [],
                'present': combined
             }
        )
        
        # test warning
        self.test.datagrams.pop(3)
        combined.remove(non_critical[3])
        bathy_available_res = self.test.bathymetry_availability()
        self.assertEqual(
        bathy_available_res.state,
        scan.ScanState.WARNING
    )
        self.assertEqual(
        bathy_available_res.data,
        {
            'missing_critical': [],
            'missing_noncritical': [non_critical[3]],
            'present': combined
         }
    )
            
        # test fail
        self.test.datagrams = {2: [ping], 12: [attitude], 3: [sound_velocity]}
        setattr(self.test.datagrams[2][0], critical[0], [])
        setattr(self.test.datagrams[2][0], critical[1], [])
        setattr(self.test.datagrams[2][0], critical[2], [])
        bathy_available_res = self.test.bathymetry_availability()
        self.assertEqual(
        bathy_available_res.state,
        scan.ScanState.FAIL
    )
        self.assertEqual(
        bathy_available_res.data,
        {
            'missing_critical': critical,
            'missing_noncritical': [],
            'present': list(non_critical.values())
         }
    )

    def test_backscatter_availability(self):
        #setup data
        critical = ['MEAN_CAL_AMPLITUDE_ARRAY', 'MEAN_REL_AMPLITUDE_ARRAY']
        ping = pygsf.SWATH_BATHYMETRY_PING(self.test.reader.fileptr,0,2,0)
        self.test.datagrams = {2: [ping]}
        self.test.datagrams[2][0].MEAN_CAL_AMPLITUDE_ARRAY = [2,2,2]
        self.test.datagrams[2][0].MEAN_REL_AMPLITUDE_ARRAY = [2,2,2]
        
        # test pass
        backscatter_available_res = self.test.backscatter_availability()

        self.assertEqual(
            backscatter_available_res.state,
            scan.ScanState.PASS
        )
        self.assertEqual(
            backscatter_available_res.data,
            {
                'missing_critical': [],
                'missing_noncritical': [],
                'present': critical
             }
        )
            
        # test fail
        setattr(self.test.datagrams[2][0], critical[0], [])
        setattr(self.test.datagrams[2][0], critical[1], [])
        backscatter_available_res = self.test.backscatter_availability()
        self.assertEqual(
        backscatter_available_res.state,
        scan.ScanState.FAIL
    )
        self.assertEqual(
        backscatter_available_res.data,
        {
            'missing_critical': critical,
            'missing_noncritical': [],
            'present': []
         }
    )

    def test_ray_tracing_availability(self):
        #setup data
        non_critical = {3: 'SOUND_VELOCITY_PROFILE', 12: 'ATTITUDE'}
        critical = ['BEAM_ANGLE_FORWARD_ARRAY', 'BEAM_ANGLE_ARRAY', 'TRAVEL_TIME_ARRAY']
        combined = critical + list(non_critical.values())
        sound_velocity = pygsf.SOUND_VELOCITY_PROFILE(self.test.reader.fileptr,0,3,0)
        attitude = pygsf.ATTITUDE(self.test.reader.fileptr,0,12,0)
        ping = pygsf.SWATH_BATHYMETRY_PING(self.test.reader.fileptr,0,2,0)
        self.test.datagrams = {2: [ping], 12: [attitude], 3: [sound_velocity]}
        self.test.datagrams[2][0].BEAM_ANGLE_FORWARD_ARRAY = [2,2,2]
        self.test.datagrams[2][0].BEAM_ANGLE_ARRAY = [2,2,2]
        self.test.datagrams[2][0].TRAVEL_TIME_ARRAY = [2,2,2]
        
        # test pass
        ray_tracing_available_res = self.test.ray_tracing_availability()

        self.assertEqual(
            ray_tracing_available_res.state,
            scan.ScanState.PASS
        )
        self.assertEqual(
            ray_tracing_available_res.data,
            {
                'missing_critical': [],
                'missing_noncritical': [],
                'present': combined
             }
        )
        
        # test warning
        self.test.datagrams.pop(3)
        combined.remove(non_critical[3])
        ray_tracing_available_res = self.test.ray_tracing_availability()
        self.assertEqual(
        ray_tracing_available_res.state,
        scan.ScanState.WARNING
    )
        self.assertEqual(
        ray_tracing_available_res.data,
        {
            'missing_critical': [],
            'missing_noncritical': [non_critical[3]],
            'present': combined
         }
    )
            
        # test fail
        self.test.datagrams = {2: [ping], 12: [attitude], 3: [sound_velocity]}
        setattr(self.test.datagrams[2][0], critical[0], [])
        setattr(self.test.datagrams[2][0], critical[1], [])
        setattr(self.test.datagrams[2][0], critical[2], [])
        ray_tracing_available_res = self.test.ray_tracing_availability()
        self.assertEqual(
        ray_tracing_available_res.state,
        scan.ScanState.FAIL
    )
        self.assertEqual(
        ray_tracing_available_res.data,
        {
            'missing_critical': critical,
            'missing_noncritical': [],
            'present': list(non_critical.values())
         }
    )

    def test_ellipsoid_height_availability(self):
        # setup data
        self.test.datagrams = {}
        height_data = [0, 1, 2, 3, 4, 5]
        ident = 2
        for val in height_data:
            ping = pygsf.SWATH_BATHYMETRY_PING(self.test.reader.fileptr,0,2,0)
            if ident not in self.test.datagrams:
                self.test.datagrams[ident] = []
            self.test.datagrams[ident].append(ping)
            self.test.datagrams[2][val].height = val
        
        # test pass
        ellipsoid_height_available_res = self.test.ellipsoid_height_availability()
        self.assertEqual(
            ellipsoid_height_available_res.state,
            scan.ScanState.PASS
        )
        
        # test warning
        for data in height_data:
            setattr(self.test.datagrams[2][data], 'height', 1)
        ellipsoid_height_available_res = self.test.ellipsoid_height_availability()
        self.assertEqual(
            ellipsoid_height_available_res.state,
            scan.ScanState.WARNING
        )
        self.assertEqual(
            ellipsoid_height_available_res.data,
            {'value': 1}
        )
        
        # test fail
        self.test.datagrams = {}
        ping = pygsf.SWATH_BATHYMETRY_PING(self.test.reader.fileptr,0,2,0)
        self.test.datagrams = {2: [ping]}
        
        ellipsoid_height_available_res = self.test.ellipsoid_height_availability()
        self.assertEqual(
            ellipsoid_height_available_res.state,
            scan.ScanState.FAIL
        )

    def test_get_installation_parameters(self):
        data = {
            'REFERENCE TIME': '1970/001 00:00:00', 
            'PLATFORM_TYPE': 'SURFACE_SHIP', 
            'FULL_RAW_DATA': 'FALSE', 
            'ROLL_COMPENSATED': 'NO ', 
            'PITCH_COMPENSATED': 'NO ', 
            'HEAVE_COMPENSATED': 'YES', 
            'TIDE_COMPENSATED': 'NO ', 
            'NUMBER_OF_RECEIVERS': '1', 
            'NUMBER_OF_TRANSMITTERS': '1'
        }
        self.test.datagrams = {}
        inst_params = self.test.get_installation_parameters()
        self.assertIsNone(inst_params)
        
        inst_parameters = pygsf.PROCESSING_PARAMETER(self.test.reader.fileptr,0,4,0)
        self.test.datagrams = {4: [inst_parameters]}
        self.test.datagrams[4][0].installationParameters = data
        inst_params = self.test.get_installation_parameters()
        self.assertIsInstance(inst_params, dict)
        self.assertEquals(inst_params, data)

    def test_runtime_parameters(self):
        rt_params = self.test.runtime_parameters()
        self.assertEqual(
            rt_params.state,
            scan.ScanState.WARNING
        )

    def test_merge_positions(self):
        Position = namedtuple('Position', 'Latitude Longitude Time')
        positions = [
            Position(0.0, 0.0, 1000.0),
            Position(0.0, 1.0, 1001.0),
            Position(0.0, 2.0, 1002.0),
            Position(0.0, 3.0, 1003.0),
            Position(0.0, 4.0, 1004.0),
            Position(0.0, 5.0, 1005.0),
            Position(0.0, 6.0, 1006.0),
            Position(0.0, 7.0, 1007.0),
            Position(0.0, 8.0, 1008.0),
            Position(0.0, 9.0, 1009.0),
            Position(0.0, 10.0, 1010.0),
            Position(0.0, 11.0, 1011.0),
            Position(0.0, 12.0, 1012.0),
            Position(0.0, 13.0, 1013.0),
            Position(0.0, 14.0, 1014.0),
            Position(0.0, 15.0, 1015.0),
            Position(0.0, 16.0, 1016.0),
            Position(0.0, 17.0, 1017.0),
            Position(0.0, 18.0, 1018.0),
            Position(0.0, 19.0, 1019.0),
            Position(0.0, 20.0, 1020.0),
        ]

        to_merge = [
            {'v': 'a', 'Time': 900},
            {'v': 'b', 'Time': 1003},
            {'v': 'c', 'Time': 1008.5},
            {'v': 'd', 'Time': 1017},
            {'v': 'e', 'Time': 1021},
        ]

        self.test._merge_position(positions, to_merge)

        self.assertEqual(
            to_merge[0]["Longitude"],
            0.0
        )
        self.assertEqual(
            to_merge[1]["Longitude"],
            3.0
        )
        self.assertEqual(
            to_merge[2]["Longitude"],
            8.0
        )
        self.assertEqual(
            to_merge[3]["Longitude"],
            17.0
        )
        self.assertEqual(
            to_merge[4]["Longitude"],
            20.0
        )


def suite():
    s = unittest.TestSuite()
    s.addTests(unittest.TestLoader().loadTestsFromTestCase(TestMateScanGsf))
    return s
