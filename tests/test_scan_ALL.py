import unittest
import os
import time
from collections import namedtuple
from hyo2.mate.lib.scan_ALL import ScanALL
from hyo2.mate.lib import scan

TEST_FILE1 = "0200_MBES_EM122_20150203_010431_Supporter_GA4430.all"
TEST_FILE = "0243_P007_MBES_EM122_20150207_044356_Supporter_GA4430.all"


class TestMateScanALL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_file = os.path.abspath(os.path.join(
                                         os.path.dirname(__file__),
                                         "test_data_remote", TEST_FILE))
        cls.test = ScanALL(cls.test_file)
        cls.test.scan_datagram()

    def test_time_str(self):
        return self.test._time_str(time.time())

    def test_get_datagram_info(self):
        D = self.test.get_datagram_info('D')
        self.assertEqual(D, None)
        Y = self.test.get_datagram_info('Y')
        self.assertEqual(Y['byteCount'], 81386)

    def test_get_size_n_pings(self):
        self.test.get_size_n_pings(2)

    def test_pings(self):
        self.assertEqual(self.test.get_total_pings('N'), 11)
        self.assertEqual(self.test.get_missed_pings('N'), 0)
        self.assertEqual(self.test.get_total_pings('-'), 0)
        self.assertEqual(self.test.get_missed_pings('-'), 0)
        self.assertEqual(self.test.get_total_pings(), 31)
        self.assertEqual(self.test.get_missed_pings(), 0)
        self.assertTrue(self.test.is_missing_pings_tolerable())
        self.assertTrue(self.test.has_minimum_pings())

    def test_is_size_matched(self):
        self.assertTrue(self.test.is_size_matched())

    def test_filename_changed(self):
        self.assertEqual(
            self.test.filename_changed().state,
            scan.ScanState.FAIL
        )

    def test_date_match(self):
        self.assertEqual(
            self.test.date_match().state,
            scan.ScanState.PASS
        )

    def test_bathymetry_availability(self):
        bathy_available_res = self.test.bathymetry_availability()

        self.assertEqual(
            bathy_available_res.state,
            scan.ScanState.WARNING
        )

    def test_backscatter_availability(self):
        backscatter_available_res = self.test.backscatter_availability()

        self.assertEqual(
            backscatter_available_res.state,
            scan.ScanState.PASS
        )

    def test_ray_tracing_availability(self):
        rt_available_res = self.test.ray_tracing_availability()
        self.assertEqual(
            rt_available_res.state,
            scan.ScanState.WARNING
        )

    def test_ellipsoid_height_availability(self):
        self.assertEqual(
            self.test.ellipsoid_height_availability().state,
            scan.ScanState.PASS
        )

    def test_get_installation_parameters(self):
        inst_params = self.test.get_installation_parameters()
        self.assertIsInstance(inst_params, dict)

    def test_runtime_parameters(self):
        rt_params = self.test.runtime_parameters()
        self.assertEqual(
            rt_params.state,
            scan.ScanState.PASS
        )
        self.assertEqual(
            len(rt_params.data['runtime_parameters']),
            3
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
    s.addTests(unittest.TestLoader().loadTestsFromTestCase(TestMateScanALL))
    return s
