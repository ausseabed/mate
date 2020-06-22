from hyo2.mate.lib.scan import Scan
from hyo2.mate.lib.scan_check import *
from hyo2.mate.lib.scan_ALL import ScanALL
from hyo2.mate.lib.scan_gsf import ScanGsf
from hyo2.mate.lib.scan_svp import ScanSvp
from hyo2.mate.lib.scan_trueheave import ScanTrueheave


# List of all check implementations
raw_data_checks = [
    BackscatterAvailableCheck,
    BathymetryAvailableCheck,
    DateChangedCheck,
    EllipsoidHeightAvailableCheck,
    EllipsoidHeightSetupCheck,
    FilenameChangedCheck,
    MinimumPingCheck,
    RayTracingCheck,
    RuntimeParametersCheck,
    PositionsCheck,
    InstallationParametersCheck,
]

svp_checks = [
    SvpExistsCheck
]

trueheave_checks = [
    TrueheaveExistsCheck
]

all_checks = raw_data_checks + svp_checks + trueheave_checks


def get_scan(path: str, file_extension: str, file_type: str) -> Scan:
    """Factory method to return a new Scan instance for the given file type.

    Args:
        path (str): Path to the file that will be read by the `Scan`
        file_extension (str): Extension of file to scan.
        file_type (str): type of file. eg "Raw Files", "SVP Files"

    Returns:
        New `Scan` instance

    Raises:
        NotImplementedError: if `file_type` is not supported
    """
    if (file_extension.lower() == 'all' and file_type == 'Raw Files'):
        return ScanALL(path)
    elif (file_extension.lower() == 'gsf' and file_type == 'Raw Files'):
        return ScanGsf(path)
    elif (file_type == 'SVP Files'):
        # could have any extension
        return ScanSvp(path)
    elif (file_type == 'Trueheave Files'):
        # could have any extension
        return ScanTrueheave(path)
    else:
        raise NotImplementedError(
            "File type {} is not supported".format(file_type))


def get_check(
    id: str, version: str, scan: Scan, params: list
) -> ScanCheck:
    """Factory method to return a new ScanCheck instance for the given id and
    version.

    Args:
        id (str): UUID for the check
        version (str): Version of the check to return
        scan (Scan): scan object that has parsed the file to be checked
        params (list): list of parameters that includes values needed for
            checks.

    Returns:
        New `ScanCheck` instance

    Raises:
        NotImplementedError: if check with `id` and `version` is not found
    """
    for check in all_checks:
        if id == check.id and version == check.version:
            return check(scan, params)

    raise NotImplementedError(
        "Check with id {} and version {} could not be found".format(
            id, version
        ))


def is_check_supported(id: str, version: str) -> bool:
    """ Indicates if the application supports this type of check.

    Args:
        id (str): UUID for the check
        version (str): Version of the check

    Returns:
        True if the check is supported (eg; it exists), otherwise false.
    """
    for check in all_checks:
        if id == check.id and version == check.version:
            return True
    return False
