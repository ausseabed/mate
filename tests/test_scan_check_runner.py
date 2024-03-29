import json
import os
from ausseabed.qajson.model import QajsonCheck
import pytest
import time
import unittest

from hyo2.mate.lib.check_runner import CheckRunner
from hyo2.mate.lib.utils import get_scan

qajson = """
[
    {
        "info": {
            "id": "7761e08b-1380-46fa-a7eb-f1f41db38541",
            "name": "Filename checked",
            "description": "",
            "version": "1",
            "group": {
                "id": "123",
                "name": "123"
            }
        },
        "inputs": {
            "files": [
                {
                    "path": "test/one.all",
                    "description": "raw input",
                    "file_type": "Raw Files"
                },
                {
                    "path": "test/two.all",
                    "description": "raw input",
                    "file_type": "Raw Files"
                }
            ]
        }
    },
    {
        "info": {
            "id": "4a3f3371-3a21-44f2-93cf-d9ed19d0c002",
            "name": "Date checked",
            "description": "",
            "version": "1",
            "group": {
                "id": "123",
                "name": "123"
            }
        },
        "inputs": {
            "files": [
                {
                    "path": "test/one.all",
                    "description": "raw input",
                    "file_type": "Raw Files"
                },
                {
                    "path": "test/three.all",
                    "description": "raw input",
                    "file_type": "Raw Files"
                }
            ]
        }
    }
]
"""


class TestMateCheckRunner(unittest.TestCase):

    def setUp(self):
        self.checks_json = json.loads(qajson)

    def test_initialize(self):
        """ Checks if the initialization has grouped the right checks under
        each file.
        """
        qajson_checks = [QajsonCheck.from_dict(d) for d in self.checks_json]
        checkrunner = CheckRunner(qajson_checks)
        checkrunner.initialize()

        file_one_checks = checkrunner._file_checks[(
            'test/one.all', 'Raw Files')]

        # make sure the right checks have been grouped under each file
        fn_check = next(
            (x for x in file_one_checks
                if x.info.id == "7761e08b-1380-46fa-a7eb-f1f41db38541"),
            None
        )
        dc_check = next(
            (x for x in file_one_checks
                if x.info.id == "4a3f3371-3a21-44f2-93cf-d9ed19d0c002"),
            None
        )
        self.assertEqual(len(file_one_checks), 2)
        self.assertIsNotNone(fn_check)
        self.assertIsNotNone(dc_check)

        file_two_checks = checkrunner._file_checks[(
            'test/two.all', 'Raw Files')]
        self.assertEqual(len(file_two_checks), 1)
        file_three_checks = checkrunner._file_checks[(
            'test/three.all', 'Raw Files')]
        self.assertEqual(len(file_three_checks), 1)


def suite():
    s = unittest.TestSuite()
    s.addTests(
        unittest.TestLoader().loadTestsFromTestCase(TestMateCheckRunner))
    return s
