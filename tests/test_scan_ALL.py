import unittest
import os
import time
from hyo2.mate.lib.scan_ALL import ScanALL
from hyo2.mate.lib import scan

TEST_FILE1 = "0200_MBES_EM122_20150203_010431_Supporter_GA4430.all"
TEST_FILE = "0243_P007_MBES_EM122_20150207_044356_Supporter_GA4430.all"


class TestMateScanALL(unittest.TestCase):

    def setUp(self):
        self.test_file = os.path.abspath(os.path.join(
                                         os.path.dirname(__file__),
                                         "test_data", TEST_FILE))
        self.test = ScanALL(self.test_file)
        self.test.scan_datagram()

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

    def test_is_filename_changed(self):
        self.assertTrue(self.test.is_filename_changed())

    def test_is_date_match(self):
        self.assertTrue(self.test.is_date_match())

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
        self.assertTrue(self.test.ellipsoid_height_availability())

    def test_get_installation_parameters(self):
        inst_params = self.test.get_installation_parameters()
        self.assertIsInstance(inst_params, dict)


def suite():
    s = unittest.TestSuite()
    s.addTests(unittest.TestLoader().loadTestsFromTestCase(TestMateScanALL))
    return s