from typing import List

from hyo2.mate.lib.scan import Scan, ScanResult, ScanState
from ausseabed.qajson.model import QajsonParam, QajsonOutputs


class ScanCheck:
    # list including default params to be used for the check
    # objects included in list will have a `name` and `value` attribute
    default_params = []

    def __init__(self, scan: Scan, params: List[QajsonParam]):
        self.scan = scan
        self.params = params
        self._output = None  # QajsonOutputs

    @property
    def output(self) -> QajsonOutputs:
        """The output of the check. Will be empty until after `run_check` has
        been called.

        Returns:
            Dict that is structured according to the QA JSON schema.
        """
        return self._output

    def get_param(self, name: str) -> QajsonParam:
        """ Gets a parameter based on the given name. Will return None if
        parameter does not exist.
        """
        match = next((p for p in self.params if p.name == name), None)
        return match

    def run_check(self):
        """Executes the check. Specific logic is implemented in the child/
        concrete classes.

        Raises:
            NotImplementedError: If the `run_check` method has not been
                overwritten by the concrete implmenetation.
        """
        raise NotImplementedError("run_check must be overwritten")


class FilenameChangedCheck(ScanCheck):
    """Checks if the name of the file matches that recorded in the metadata/
    file header.
    """
    id = '7761e08b-1380-46fa-a7eb-f1f41db38541'
    name = "Filename checked"
    version = '1'

    def __init__(self, scan: Scan, params):
        ScanCheck.__init__(self, scan, params)

    def run_check(self):
        filename_changed = self.scan.is_filename_changed()

        messages = (
            ["Filename does not match name embedded within file contents"]
            if filename_changed
            else None
        )

        state = ScanState.FAIL if filename_changed else ScanState.PASS

        self._output = QajsonOutputs(
            execution=None,
            files=None,
            count=None,
            percentage=None,
            messages=messages,
            check_state=state
        )


class DateChangedCheck(ScanCheck):
    """Checks if the date included in the filename matches that recorded in
    the metadata/file header.
    """
    id = '4a3f3371-3a21-44f2-93cf-d9ed19d0c002'
    name = "Date checked"
    version = '1'

    def __init__(self, scan: Scan, params):
        ScanCheck.__init__(self, scan, params)

    def run_check(self):
        date_changed = not self.scan.is_date_match()

        messages = (
            ["File date does not match date embedded within file contents"]
            if date_changed
            else None
        )

        state = ScanState.FAIL if date_changed else ScanState.PASS

        self._output = QajsonOutputs(
            execution=None,
            files=None,
            count=None,
            percentage=None,
            messages=messages,
            check_state=state
        )


class BathymetryAvailableCheck(ScanCheck):
    """Checks bathymetry data is available.
    """
    id = '8c909ace-8759-4c2c-b86a-f76f888cd821'
    name = "Bathymetry Available"
    version = '1'

    def __init__(self, scan: Scan, params):
        ScanCheck.__init__(self, scan, params)

    def run_check(self):
        scan_result = self.scan.bathymetry_availability()

        self._output = QajsonOutputs(
            execution=None,
            files=None,
            count=None,
            percentage=None,
            messages=scan_result.messages,
            data=scan_result.data,
            check_state=scan_result.state
        )


class BackscatterAvailableCheck(ScanCheck):
    """Checks backscatter data is available.
    """
    id = 'bbce47c0-54c9-4c60-8de8-b174a8905091'
    name = "Backscatter Available"
    version = '1'

    def __init__(self, scan: Scan, params):
        ScanCheck.__init__(self, scan, params)

    def run_check(self):
        scan_result = self.scan.backscatter_availability()

        self._output = QajsonOutputs(
            execution=None,
            files=None,
            count=None,
            percentage=None,
            messages=scan_result.messages,
            data=scan_result.data,
            check_state=scan_result.state
        )


class RayTracingCheck(ScanCheck):
    """Checks Ray Tracing data is available.
    """
    id = '5421f3f2-6e37-4740-bf83-488bebde49f4'
    name = "Ray Tracing Available"
    version = '1'

    def __init__(self, scan: Scan, params):
        ScanCheck.__init__(self, scan, params)

    def run_check(self):
        scan_result = self.scan.ray_tracing_availability()

        self._output = QajsonOutputs(
            execution=None,
            files=None,
            count=None,
            percentage=None,
            messages=scan_result.messages,
            data=scan_result.data,
            check_state=scan_result.state
        )


class MinimumPingCheck(ScanCheck):
    """Checks for minimum number of required pings in all multibeam datagrams.
    If the params list includes a `threshold` entry this will be used,
    otherwise the default of 10 will be applied.
    """
    id = 'd762fd79-75bc-4aff-a9d2-e0c36e744e17'
    name = "Minimum Ping count"
    version = '1'
    default_params = [
        QajsonParam(name='threshold', value=20)
    ]

    def __init__(self, scan: Scan, params):
        ScanCheck.__init__(self, scan, params)

    def run_check(self):
        passed = None
        threshold_param = self.get_param('threshold')
        if threshold_param is not None:
            passed = self.scan.has_minimum_pings(threshold_param.value)
        else:
            passed = self.scan.has_minimum_pings()

        messages = (
            None
            if passed
            else (
                ["Minimum ping count is less than threshold count of 10"]
                if threshold_param is None
                else (
                    ["Minimum ping count is less than threshold count of {}".format(threshold_param.value)]
                )
            )
        )

        state = ScanState.PASS if passed else ScanState.FAIL

        self._output = QajsonOutputs(
            execution=None,
            files=None,
            count=None,
            percentage=None,
            messages=messages,
            check_state=state
        )


class EllipsoidHeightAvailableCheck(ScanCheck):
    """Checks Ellipsoid Height is available.
    """
    id = 'bbce47c0-54c9-4c60-8de8-b174a8905091'
    name = "Ellipsoid Height Available"
    version = '1'

    def __init__(self, scan: Scan, params):
        ScanCheck.__init__(self, scan, params)

    def run_check(self):
        eh_avail = self.scan.ellipsoid_height_availability()

        messages = (
            None
            if rt_avail
            else ["Ellipsoid height is not available"]
        )

        state = ScanState.PASS if eh_avail else ScanState.FAIL

        self._output = QajsonOutputs(
            execution=None,
            files=None,
            count=None,
            percentage=None,
            messages=messages,
            check_state=state
        )


class EllipsoidHeightSetupCheck(ScanCheck):
    '''
    Check the input positioning system string will likely contain heights
    that are referenced to the ellipsoid
    '''
    id = '9b39cae1-dbb6-4f8c-b71a-d6f8ed843808'
    name = "Ellipsoid Height Setup"
    version = '1'

    def __init__(self, scan: Scan, params):
        ScanCheck.__init__(self, scan, params)

    def run_check(self):
        scan_result = self.scan.ellipsoid_height_setup()

        self._output = QajsonOutputs(
            execution=None,
            files=None,
            count=None,
            percentage=None,
            messages=scan_result.messages,
            data=scan_result.data,
            check_state=scan_result.state
        )


class SvpExistsCheck(ScanCheck):
    """ Checks SVP file exists data is available.
    """
    id = 'e57b7811-5863-49b3-bd06-a73de0add615'
    name = "SVP File Available"
    version = '1'

    def __init__(self, scan: Scan, params):
        ScanCheck.__init__(self, scan, params)

    def run_check(self):
        check_state = ScanState.FAIL
        message = ""
        if (self.scan.file_exits and self.scan.file_non_zero_size):
            check_state = ScanState.PASS
        elif (self.scan.file_exits):
            # then file exists, but has zero size
            check_state = ScanState.FAIL
            message = "SVP file is empty (zero size)"
        else:
            check_state = ScanState.FAIL
            message = "SVP file does not exist"

        self._output = QajsonOutputs(
            execution=None,
            files=None,
            count=None,
            percentage=None,
            messages=message,
            data=None,
            check_state=check_state
        )


class TrueheaveExistsCheck(ScanCheck):
    """ Checks Trueheave file exists data is available.
    """
    id = '8da36d61-c986-4089-9642-ca2139577fab'
    name = "Trueheave File Available"
    version = '1'

    def __init__(self, scan: Scan, params):
        ScanCheck.__init__(self, scan, params)

    def run_check(self):
        check_state = ScanState.FAIL
        message = ""
        if (self.scan.file_exits and self.scan.file_non_zero_size):
            check_state = ScanState.PASS
        elif (self.scan.file_exits):
            # then file exists, but has zero size
            check_state = ScanState.FAIL
            message = "Trueheave file is empty (zero size)"
        else:
            check_state = ScanState.FAIL
            message = "Trueheave file does not exist"

        self._output = QajsonOutputs(
            execution=None,
            files=None,
            count=None,
            percentage=None,
            messages=message,
            data=None,
            check_state=check_state
        )
