import copy
from datetime import datetime
import logging
import os
import traceback
from typing import Callable, List

from ausseabed.qajson.model import QajsonParam, QajsonOutputs, \
    QajsonExecution, QajsonInputs, QajsonCheck, QajsonExecution

from hyo2.mate.lib.utils import get_scan, get_check, is_check_supported

logger = logging.getLogger(__name__)


class CheckRunner:
    """ The `CheckRunner` coordinates the execution of multiple checks based
    on a given QA JSON based definition.

    This class is strongly aligned towards supporting Scan checks. These checks
    work by first loading the metadata/header, then performing multiple checks
    on the loaded data. This check runner therefore performs checks by
    iterating over files, and performing multiple checks on each before moving
    to the next file.
    """

    def __init__(self, checks_def: List[QajsonCheck]):
        """ `CheckRunner` constructor

        Args:
            checks_def (dict): definition of checks. This should conform to
                the checks block of the QA JSON schema.
        """
        self._input = checks_def
        # The check runner output will be added to the input qajson
        self._output = self._input
        self._file_checks = None

    @property
    def output(self) -> dict:
        """The output of all checks. Will be a clone of the input until checks
        have been run.

        Returns:
            Dict that is structured according to the QA JSON schema.
        """
        return self._output

    def initialize(self):
        """ Performs necessary preprocessing of the input before check
        execution can begin. This consists of remapping the input from a list
        of checks with files to a list of files with checks.
        """
        filechecks = {}
        for check in self._input:
            checkid = check.info.id
            checkversion = check.info.version
            if not is_check_supported(checkid, checkversion):
                # It's expected the QA JSON definition could include other
                # checks not supported by this application. Ignore these
                # checks.
                logger.warning(
                    "Check {} was ignored as it is not supported"
                    .format(check['info']['id']))
                continue
            inputs = check.inputs
            for input in inputs.files:
                filename = input.path
                filetype = input.file_type

                if (filename, filetype) in filechecks:
                    checklistforfile = filechecks[(filename, filetype)]
                    checklistforfile.append(check)
                else:
                    checklistforfile = []
                    checklistforfile.append(check)
                    filechecks[(filename, filetype)] = checklistforfile

        self._file_checks = filechecks

    def _add_output(self, check_id, filename, output):
        """ Adds the output to the appropriate location in the _output.
        """
        for check in self._output:
            outputcheckid = check.info.id

            # does this check match the check fro which the output was
            # generated for
            if check_id != outputcheckid:
                continue

            # does this check have the file specified that the output was
            # generated for. There may be duplicate checks, each with different
            # files, this makes sure we only attach the output to the right
            # one.
            inputs = check.inputs
            has_file = False
            for input in inputs.files:
                if input.path == filename:
                    has_file = True
                    break

            if not has_file:
                continue

            check.outputs = output
            return

        raise RuntimeError("Could not find check {} for file {}".format(
            check_id, filename
        ))

    def run_checks(
            self,
            progress_callback: Callable = None,
            qajson_update_callback: Callable = None,
            is_stopped: Callable = None):
        """ Excutes all checks on a file-by-file basis

        :param progress_callback Callable: function reference that is passed
            a float between the value of 0.0 and 1.0 to indicate progress
            of the checks. Optional.
        :param qajson_update_callback Callable: function reference that is
            to be called when the qajson related to this check has been
            updated by the check. Optional.
        :param is_stopped Callable: if this function returns True the check
            runner will stop processing (eventually)
        """
        if self._file_checks is None:
            raise RuntimeError("CheckRunner is not initialized")

        # to support accurate progress reporting get size of all files
        total_file_size = 0
        processed_files_size = 0
        for (filename, filetype), checklist in self._file_checks.items():
            if os.path.exists(filename):
                total_file_size += os.path.getsize(filename)

        for (filename, filetype), checklist in self._file_checks.items():
            if is_stopped is not None and is_stopped():
                return

            file_size = 0
            if os.path.exists(filename):
                file_size = os.path.getsize(filename)
            file_size_fraction = 1
            if total_file_size != 0:
                file_size_fraction = file_size / total_file_size
            _, extension = os.path.splitext(filename)
            # remove the `.` char from extension
            file_extension = extension[1:]

            # read metadata from header
            scan = get_scan(filename, file_extension, filetype)

            def prog_cb(scan_progress):
                p = scan_progress * file_size + processed_files_size
                if progress_callback is not None:
                    progress_callback(p / total_file_size)

            scan.scan_datagram(prog_cb)

            processed_files_size += file_size

            for checkdata in checklist:
                checkid = checkdata.info.id
                checkversion = checkdata.info.version

                checkparams = []
                if checkdata.inputs.params is not None:
                    checkparams = checkdata.inputs.params

                checkoutputs = QajsonOutputs()
                checkstatus = None
                checkerrormessage = None
                checkstart = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
                # get check based on id and version
                check = get_check(checkid, checkversion, scan, checkparams)
                try:
                    check.run_check()
                    checkstatus = "completed"
                    # merge two dicts; checkoutputs and check.output
                    checkoutputs = check.output
                except Exception as e:
                    checkstatus = "failed"
                    checkerrormessage = traceback.format_exc()
                checkend = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")

                checkoutputs.execution = QajsonExecution(
                    start=checkstart,
                    end=checkend,
                    status=checkstatus,
                    error=checkerrormessage
                )

                self._add_output(checkid, filename, checkoutputs)

            # qajson for all checks is updated on a file by file basis. So
            # call the update after a file has finished processing, and
            # not after each check.
            if qajson_update_callback is not None:
                qajson_update_callback()
