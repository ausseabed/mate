import unittest
import os
import time
import csv
from hyo2.mate.lib.scan_ALL import ScanALL
from hyo2.mate.lib import scan

# test data csv file path
test_data_file_path = os.path.abspath(os.path.join(
                                         os.path.dirname(__file__),
                                         "test_data", "QAQC_Test_Data.csv"))

# test data file object
test_data_file_object = None

# test data dictionary.
test_data_dict = {}


# load test data from ./test_data.csv file.
def load_test_data():
    global test_data_file_object, test_data_dict
    # open test data csv file.
    test_data_file_object = open(test_data_file_path, 'r')
    # read the csv file and return the test data dictionary.
    csv_reader = csv.reader(test_data_file_object, delimiter=',')
    headers = next(csv_reader)[1:]
    for row in csv_reader:
        test_data_dict[row[0]] = {key: value for key, value in zip(headers, row[1:])}
    print('')
    print('open and load data from QAQC_Test_Data.csv complete.')


# close and release the test data file object.
def close_test_data_file():
    global test_data_file_object
    if test_data_file_object is not None:
        test_data_file_object.close()
        test_data_file_object = None
        print('')
        print('close file QAQC_Test_Data.csv complete.')


TEST_FILE = "0086_20181111_231616_Bluefin.all"


class TestMateScanALL(unittest.TestCase):

    # class level setup function, execute once only before any test function.
    @classmethod
    def setUpClass(cls):
        cls.test_file = os.path.abspath(os.path.join(
                                         os.path.dirname(__file__),
                                         "test_data", TEST_FILE))
        cls.test = ScanALL(cls.test_file)
        cls.test.scan_datagram()

        load_test_data()
        print('setUpClass')

    # class level setup function, execute once only after all test function's execution.
    @classmethod
    def tearDownClass(cls):
        close_test_data_file()
        print('tearDownClass')

    def setUp(self):
        pass

    def test_time_str(self):
        return self.test._time_str(time.time())

    def test_get_datagram_format_version(self):
        self.assertEqual(self.test.get_datagram_format_version(), test_data_dict[TEST_FILE]['DSV'])

    def test_get_datagram_info(self):
        D = self.test.get_datagram_info('D')
        self.assertEqual(D, None)
        Y = self.test.get_datagram_info('Y')
        self.assertEqual(Y['byteCount'], 20892342)

    def test_get_size_n_pings(self):
        self.test.get_size_n_pings(1)

    def test_pings(self):
        self.assertEqual(self.test.get_total_pings('N'), 1733)
        self.assertEqual(self.test.get_missed_pings('N'), 0)
        self.assertEqual(self.test.get_total_pings('-'), 0)
        self.assertEqual(self.test.get_missed_pings('-'), 0)
        self.assertEqual(self.test.get_total_pings(), 5199)
        self.assertEqual(self.test.get_missed_pings(), 0)
        self.assertTrue(self.test.is_missing_pings_tolerable())
        self.assertTrue(self.test.has_minimum_pings())

    def test_is_size_matched(self):
        self.assertTrue(self.test.is_size_matched())

    def test_is_filename_changed(self):
        self.assertFalse(self.test.is_filename_changed())

    def test_is_date_match(self):
        self.assertTrue(self.test.is_date_match())

    def test_bathymetry_availability(self):
        bathy_available_res = self.test.bathymetry_availability()

        self.assertEqual(
            bathy_available_res.state,
            scan.ScanState.PASS
        )

    def test_backscatter_availability(self):
        backscatter_available_res = self.test.backscatter_availability()

        self.assertEqual(
            backscatter_available_res.state,
            scan.ScanState.PASS
        )

    def test_ray_tracing_availability(self):
        ray_tracing_available_res = self.test.ray_tracing_availability()

        self.assertEqual(
            ray_tracing_available_res.state,
            scan.ScanState.PASS
        )

    def test_ellipsoid_height_availability(self):
        self.assertTrue(self.test.ellipsoid_height_availability())

    def test_get_installation_parameters(self):
        inst_params = self.test.get_installation_parameters()
        self.assertIsInstance(inst_params, dict)
        for key, val in test_data_dict[TEST_FILE].items():
            if key in inst_params:
                self.assertEqual(inst_params[key], test_data_dict[TEST_FILE][key])

    def test_ellipsoid_height_setup(self):
        ellipsoid_height_set = self.test.ellipsoid_height_setup()

        self.assertEqual(
            ellipsoid_height_set.state,
            scan.ScanState.PASS
        )


def suite():
    s = unittest.TestSuite()
    s.addTests(unittest.TestLoader().loadTestsFromTestCase(TestMateScanALL))
    return s
