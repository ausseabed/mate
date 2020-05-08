import unittest
import os
import pytest
import time
from hyo2.mate.lib.scan_utils import get_scan
from hyo2.mate.lib.scan import ScanResult, ScanState

TEST_FILE1 = "0200_MBES_EM122_20150203_010431_Supporter_GA4430.all"
TEST_FILE = "0243_P007_MBES_EM122_20150207_044356_Supporter_GA4430.all"


class TestMateScan(unittest.TestCase):

    def setUp(self):
        self.test_file = os.path.abspath(os.path.join(
                                         os.path.dirname(__file__),
                                         "test_data", TEST_FILE))

    def test_get_scan_all(self):
        _, extension = os.path.splitext(self.test_file)
        extension = extension[1:]  # remove the `.` char from extension
        scan = get_scan(self.test_file, extension)
        self.assertEqual(type(scan).__name__, 'ScanALL')

    def test_get_scan_unsupported(self):
        with pytest.raises(NotImplementedError):
            # following fn should raise a `NotImplementedError` when called
            get_scan('doesnotexist.something', 'unsupported_type')


class TestMateScanResult(unittest.TestCase):

    def test_message_array(self):
        ''' Checks if scan result deals with an array of messages ok '''
        sr = ScanResult(
            ScanState.PASS,
            ["message one", "message two"]
        )
        self.assertSequenceEqual(sr.messages, ["message one", "message two"])

    def test_message_single(self):
        ''' Checks if scan result deals with messages being given as a single
            string
        '''
        sr = ScanResult(
            ScanState.PASS,
            "The only message"
        )
        # note the ScanResult messages should still be a list
        self.assertSequenceEqual(sr.messages, ["The only message"])


def suite():
    s = unittest.TestSuite()
    s.addTests(unittest.TestLoader().loadTestsFromTestCase(TestMateScan))
    s.addTests(unittest.TestLoader().loadTestsFromTestCase(TestMateScanResult))
    return s
