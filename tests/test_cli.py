import filecmp
import subprocess
import sys
from os import PathLike
from pathlib import Path

from aa_remove_data import __version__
from aa_remove_data.pb_utils import PBUtils

test_data = Path("tests/test_data")
cli_output = Path("tests/test_data/cli_expected_output")
results = Path("tests/test_data/results_files")


def try_to_remove(filepath: PathLike):
    filepath = Path(filepath)
    if filepath.is_file():
        filepath.unlink()


def test_cli_version():
    cmd = [sys.executable, "-m", "aa_remove_data", "--version"]
    assert subprocess.check_output(cmd).decode().strip() == __version__


def test_cli_print_header():
    cmd = ["aa-print-header", test_data / "P:2021_short.pb"]
    expected = "Name: BL13I-VA-GAUGE-28:P, Type: SCALAR_DOUBLE, Year: 2021"
    assert subprocess.check_output(cmd).decode().strip() == expected


def test_cli_print_header_with_lines():
    cmd = ["aa-print-header", test_data / "RAW:2025_short.pb", "--lines=3"]
    expected = (
        "Name: BL11K-EA-ADC-01:M4:CH4:RAW, Type: SCALAR_INT, Year: 2025\n"
        + "DATE                   SECONDS     NANO         VAL\n"
        + "2025-01-01 00:00:00           0      2588941    -1850\n"
        + "2025-01-01 00:00:00           0    102596158    -2544\n"
        + "2025-01-01 00:00:00           0    202583899    -2351"
    )
    assert subprocess.check_output(cmd).decode().strip() == expected


def test_cli_pb_2_txt():
    read = test_data / "RAW:2025_short.pb"
    write = results / "RAW:2025_short_test_cli_pb_2_txt.txt"
    expected = test_data / "RAW:2025_short.txt"
    cmd = ["pb-2-txt", read, write]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical is True
    if are_identical:
        write = Path(write)
        write.unlink()


def test_cli_pb_2_txt_chunked():
    read = test_data / "RAW:2025_short.pb"
    write = results / "RAW:2025_short_test_cli_pb_2_txt_chunked.txt"
    expected = test_data / "RAW:2025_short.txt"
    cmd = ["pb-2-txt", read, write, "--chunk=6"]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical is True
    if are_identical:
        write = Path(write)
        write.unlink()


def test_cli_reduce_to_period():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_reduce_to_period.pb"
    expected = cli_output / "SCALAR_STRING_reduce_to_period.pb"
    period = "4.5"
    cmd = [
        "aa-reduce-data-to-period",
        read,
        period,
        f"--new-filename={write}",
    ]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_reduce_to_period_chunked():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_reduce_to_period_chunked.pb"
    expected = cli_output / "SCALAR_STRING_reduce_to_period.pb"
    period = "4.5"
    cmd = [
        "aa-reduce-data-to-period",
        read,
        period,
        f"--new-filename={write}",
        "--chunk=17",
    ]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_reduce_to_period_check_period():
    read = test_data / "RAW:2025_short.pb"
    write = results / "RAW:2025_test_reduce_to_period.pb"
    period = 3
    cmd = [
        "aa-reduce-data-to-period",
        read,
        str(period),
        f"--new-filename={write}",
    ]
    subprocess.run(cmd)
    pb = PBUtils(write)
    for i in range(len(pb.samples) - 1):
        seconds_diff = pb.samples[i + 1].secondsintoyear - pb.samples[i].secondsintoyear
        nano_diff = pb.samples[i + 1].nano - pb.samples[i].nano
        assert seconds_diff >= period + 1 or (seconds_diff == period and nano_diff >= 0)
    write.unlink()


def test_cli_reduce_to_period_backup():
    read = test_data / "RAW:2025_short.pb"
    write = results / "tmp.pb"
    backup = results / "RAW:2025_short_reduce_to_period_backup.pb"
    expected = read
    period = "3"
    cmd = [
        "aa-reduce-data-to-period",
        read,
        period,
        f"--new-filename={write}",
        f"--backup-filename={backup}",
    ]
    subprocess.run(cmd)
    try_to_remove(write)
    are_identical = filecmp.cmp(backup, expected, shallow=False)
    assert are_identical
    backup.unlink()


def test_cli_reduce_to_period_txt():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_reduce_to_period_txt.pb"
    expected = cli_output / "SCALAR_STRING_reduce_to_period.txt"
    txt_path = write.with_suffix(".txt")
    period = "4.5"
    cmd = [
        "aa-reduce-data-to-period",
        read,
        period,
        "-t",
        f"--new-filename={write}",
    ]
    subprocess.run(cmd)
    try_to_remove(write)
    are_identical = filecmp.cmp(txt_path, expected, shallow=False)
    assert are_identical
    txt_path.unlink()


def test_cli_reduce_to_period_non_existent_filename():
    read = test_data / "this/file/does_not_exist.pb"
    period = "10"
    cmd = ["aa-reduce-data-to-period", read, period]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert "FileNotFoundError:" in result.stderr


def test_cli_reduce_to_period_invalid_filename():
    read = test_data / "SCALAR_STRING_test_data.jpeg"
    period = "10"
    cmd = ["aa-reduce-data-to-period", read, period]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(read)}" in result.stderr


def test_cli_remove_reduce_to_period_invalid_new_filename():
    read = test_data / "dummy_file.pb"
    write = results / "SCALAR_STRING_reduce_to_period.hdf5"
    period = "10"
    cmd = ["aa-reduce-data-to-period", read, period, f"--new-filename={write}"]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(write)}" in result.stderr


def test_cli_reduce_to_period_invalid_backup_filename():
    read = test_data / "dummy_file.pb"
    backup = results / "SCALAR_STRING_reduce_to_period_backup.zip"
    period = "10"
    cmd = ["aa-reduce-data-to-period", read, period, f"--backup-filename={backup}"]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(backup)}" in result.stderr


def test_cli_reduce_by_factor():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_reduce_by_factor.pb"
    expected = cli_output / "SCALAR_STRING_reduce_by_factor.pb"
    factor = "3"
    cmd = [
        "aa-reduce-data-by-factor",
        read,
        factor,
        f"--new-filename={write}",
    ]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_reduce_by_factor_blocks():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_reduce_by_factor_blocks.pb"
    expected = cli_output / "SCALAR_STRING_reduce_by_factor_blocks.pb"
    factor = "4"
    cmd = [
        "aa-reduce-data-by-factor",
        read,
        factor,
        f"--new-filename={write}",
        "--block=5",
    ]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_reduce_by_factor_blocks_chunked():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_reduce_by_factor_blocks_chunked.pb"
    expected = cli_output / "SCALAR_STRING_reduce_by_factor_blocks.pb"
    factor = "4"
    cmd = [
        "aa-reduce-data-by-factor",
        read,
        factor,
        f"--new-filename={write}",
        "--block=5",
        "--chunk=11",
    ]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_reduce_by_factor_chunked():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_reduce_by_factor_chunked.pb"
    expected = cli_output / "SCALAR_STRING_reduce_by_factor.pb"
    factor = "3"
    cmd = [
        "aa-reduce-data-by-factor",
        read,
        factor,
        f"--new-filename={write}",
        "--chunk=13",
    ]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_reduce_by_factor_backup():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "tmp.pb"
    backup = results / "SCALAR_STRING_reduce_by_factor_backup.pb"
    expected = read
    factor = "3"
    cmd = [
        "aa-reduce-data-by-factor",
        read,
        factor,
        f"--new-filename={write}",
        f"--backup-filename={backup}",
    ]
    subprocess.run(cmd)
    try_to_remove(write)
    are_identical = filecmp.cmp(backup, expected, shallow=False)
    assert are_identical
    backup.unlink()


def test_cli_reduce_by_factor_txt():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_reduce_by_factor_txt.pb"
    expected = cli_output / "SCALAR_STRING_reduce_by_factor.txt"
    txt_path = write.with_suffix(".txt")
    factor = "3"
    cmd = [
        "aa-reduce-data-by-factor",
        read,
        factor,
        "-t",
        f"--new-filename={write}",
    ]
    subprocess.run(cmd)
    try_to_remove(write)
    are_identical = filecmp.cmp(txt_path, expected, shallow=False)
    assert are_identical
    txt_path.unlink()


def test_cli_reduce_by_factor_non_existent_filename():
    read = test_data / "this/file/does_not_exist.pb"
    factor = "10"
    cmd = ["aa-reduce-data-by-factor", read, factor]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert "FileNotFoundError:" in result.stderr


def test_cli_reduce_by_factor_invalid_filename():
    read = test_data / "SCALAR_STRING_test_data.jpeg"
    factor = "5"
    cmd = ["aa-reduce-data-by-factor", read, factor]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(read)}" in result.stderr


def test_cli_remove_reduce_by_factor_invalid_new_filename():
    read = test_data / "dummy_file.pb"
    write = results / "SCALAR_STRING_reduce_by_factor.hdf5"
    factor = "5"
    cmd = ["aa-reduce-data-by-factor", read, factor, f"--new-filename={write}"]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(write)}" in result.stderr


def test_cli_reduce_by_factor_invalid_backup_filename():
    read = test_data / "dummy_file.pb"
    backup = results / "SCALAR_STRING_reduce_by_factor_backup.zip"
    factor = "5"
    cmd = ["aa-reduce-data-by-factor", read, factor, f"--backup-filename={backup}"]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(backup)}" in result.stderr


def test_cli_remove_before():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_remove_before.pb"
    expected = cli_output / "SCALAR_STRING_remove_before.pb"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-before", read, ts, f"--new-filename={write}"]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_remove_before_chunked():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_remove_before_chunked.pb"
    expected = cli_output / "SCALAR_STRING_remove_before.pb"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-before", read, ts, f"--new-filename={write}", "--chunk=37"]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_remove_before_backup():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "tmp.pb"
    backup = results / "SCALAR_STRING_remove_before_backup.pb"
    expected = read
    ts = "1,1,0,1,5"
    cmd = [
        "aa-remove-data-before",
        read,
        ts,
        f"--new-filename={write}",
        f"--backup-filename={backup}",
    ]
    subprocess.run(cmd)
    try_to_remove(write)
    are_identical = filecmp.cmp(backup, expected, shallow=False)
    assert are_identical
    backup.unlink()


def test_cli_remove_before_txt():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_remove_before_txt.pb"
    expected = cli_output / "SCALAR_STRING_remove_before.txt"
    txt_path = write.with_suffix(".txt")
    ts = "1,1,0,1,5"
    cmd = [
        "aa-remove-data-before",
        read,
        ts,
        "-t",
        f"--new-filename={write}",
    ]
    subprocess.run(cmd)
    try_to_remove(write)
    are_identical = filecmp.cmp(txt_path, expected, shallow=False)
    assert are_identical
    txt_path.unlink()


def test_cli_remove_before_non_existent_filename():
    read = test_data / "this/file/does_not_exist.pb"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-before", read, ts]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert "FileNotFoundError:" in result.stderr


def test_cli_remove_before_invalid_filename():
    read = test_data / "SCALAR_STRING_test_data.jpeg"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-before", read, ts]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(read)}" in result.stderr


def test_cli_remove_before_invalid_new_filename():
    read = test_data / "dummy_file.pb"
    write = results / "SCALAR_STRING_remove_before.hdf5"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-before", read, ts, f"--new-filename={write}"]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(write)}" in result.stderr


def test_cli_remove_before_invalid_backup_filename():
    read = test_data / "dummy_file.pb"
    backup = results / "SCALAR_STRING_remove_before_backup.zip"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-before", read, ts, f"--backup-filename={backup}"]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(backup)}" in result.stderr


def test_cli_remove_after():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_remove_after.pb"
    expected = cli_output / "SCALAR_STRING_remove_after.pb"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-after", read, ts, f"--new-filename={write}"]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_remove_after_chunked():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_remove_after_chunked.pb"
    expected = cli_output / "SCALAR_STRING_remove_after.pb"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-after", read, ts, f"--new-filename={write}", "--chunk=37"]
    subprocess.run(cmd)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_remove_after_backup():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "tmp.pb"
    backup = results / "SCALAR_STRING_remove_after_backup.pb"
    expected = read
    ts = "1,1,0,1,5"
    cmd = [
        "aa-remove-data-after",
        read,
        ts,
        f"--new-filename={write}",
        f"--backup-filename={backup}",
    ]
    subprocess.run(cmd)
    try_to_remove(write)
    are_identical = filecmp.cmp(backup, expected, shallow=False)
    assert are_identical
    backup.unlink()


def test_cli_remove_after_txt():
    read = test_data / "SCALAR_STRING_test_data.pb"
    write = results / "SCALAR_STRING_remove_after_txt.pb"
    expected = cli_output / "SCALAR_STRING_remove_after.txt"
    txt_path = write.with_suffix(".txt")
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-after", read, ts, "-t", f"--new-filename={write}"]
    subprocess.run(cmd)
    try_to_remove(write)
    are_identical = filecmp.cmp(txt_path, expected, shallow=False)
    assert are_identical
    txt_path.unlink()


def test_cli_remove_after_non_existent_filename():
    read = test_data / "this/file/does_not_exist.pb"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-after", read, ts]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert "FileNotFoundError:" in result.stderr


def test_cli_remove_after_invalid_filename():
    read = test_data / "SCALAR_STRING_test_data.jpeg"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-after", read, ts]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(read)}" in result.stderr


def test_cli_remove_after_invalid_new_filename():
    read = test_data / "dummy_file.pb"
    write = results / "SCALAR_STRING_remove_after.hdf5"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-after", read, ts, f"--new-filename={write}"]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(write)}" in result.stderr


def test_cli_remove_after_invalid_backup_filename():
    read = test_data / "dummy_file.pb"
    backup = results / "SCALAR_STRING_remove_after_backup.zip"
    ts = "1,1,0,1,5"
    cmd = ["aa-remove-data-after", read, ts, f"--backup-filename={backup}"]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode != 0
    assert f"ValueError: Invalid file extension for {str(backup)}" in result.stderr
