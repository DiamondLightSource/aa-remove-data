import filecmp
import subprocess
import sys
from pathlib import Path

from aa_remove_data import __version__
from aa_remove_data.pb_utils import PBUtils


def test_cli_version():
    cmd = [sys.executable, "-m", "aa_remove_data", "--version"]
    assert subprocess.check_output(cmd).decode().strip() == __version__


def test_cli_print_header():
    cmd = ["aa-print-header", "tests/test_data/P:2021_short.pb"]
    expected = "Name: BL13I-VA-GAUGE-28:P, Type: SCALAR_DOUBLE, Year: 2021"
    assert subprocess.check_output(cmd).decode().strip() == expected


def test_cli_print_header_with_lines():
    cmd = ["aa-print-header", "tests/test_data/RAW:2025_short.pb", "--lines=3"]
    expected = (
        "Name: BL11K-EA-ADC-01:M4:CH4:RAW, Type: SCALAR_INT, Year: 2025\n"
        + "DATE                   SECONDS     NANO         VAL\n"
        + "2025-01-01 00:00:00           0      2588941    -1850\n"
        + "2025-01-01 00:00:00           0    102596158    -2544\n"
        + "2025-01-01 00:00:00           0    202583899    -2351"
    )
    assert subprocess.check_output(cmd).decode().strip() == expected


def test_cli_pb_2_txt():
    read = "tests/test_data/RAW:2025_short.pb"
    write = "tests/test_data/RAW:2025_short_test_cli_pb_2_txt.txt"
    expected = "tests/test_data/RAW:2025_short.txt"
    cmd = ["pb-2-txt", read, write]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical is True
    if are_identical:
        write = Path(write)
        write.unlink()


def test_cli_reduce_data_freq():
    read = "tests/test_data/RAW:2025_short.pb"
    write = "tests/test_data/RAW:2025_test_reduce_data_freq.pb"
    backup = "tests/test_data/tmp.pb"
    period = 3
    cmd = [
        "aa-reduce-data-freq",
        read,
        str(period),
        f"--new-filename={write}",
        f"--backup-filename={backup}",
    ]
    subprocess.run(cmd)
    backup = Path(backup)
    backup.unlink()
    write = Path(write)
    pb = PBUtils(write)
    for i in range(len(pb.samples) - 1):
        seconds_diff = pb.samples[i + 1].secondsintoyear - pb.samples[i].secondsintoyear
        nano_diff = pb.samples[i + 1].nano - pb.samples[i].nano
        assert seconds_diff >= 4 or (seconds_diff == 3 and nano_diff >= 0)
    write.unlink()


def test_cli_reduce_data_freq_backup():
    read = "tests/test_data/RAW:2025_short.pb"
    write = "tests/test_data/tmp.pb"
    backup = "tests/test_data/RAW:2025_short_backup.pb"
    period = 3
    cmd = [
        "aa-reduce-data-freq",
        read,
        str(period),
        f"--new-filename={write}",
        f"--backup-filename={backup}",
    ]
    subprocess.run(cmd)
    write = Path(write)
    write.unlink()
    backup = Path(backup)
    are_identical = filecmp.cmp(read, backup, shallow=False)
    assert are_identical
    backup.unlink()


def test_cli_reduce_by_factor():
    read = "tests/test_data/SCALAR_STRING_test_data.pb"
    write = "tests/test_data/results/SCALAR_STRING_test_reduce_by_factor.pb"
    backup = "tests/test_data/results/tmp.pb"
    expected = Path("tests/test_data/cli_expected_output/SCALAR_STRING_by_factor.pb")
    factor = "3"
    cmd = [
        "aa-reduce-data-by-factor",
        read,
        factor,
        f"--new-filename={write}",
        f"--backup-filename={backup}",
    ]
    subprocess.run(cmd)
    backup = Path(backup)
    backup.unlink()
    write = Path(write)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()
