import filecmp
import subprocess
import sys
from os import PathLike
from pathlib import Path

from typer.testing import CliRunner

from aa_remove_data import __version__
from aa_remove_data.archiver_data import ArchiverData
from aa_remove_data.remove_data import app

TEST_DATA = Path("tests/test_data")
CLI_OUTPUT = Path("tests/test_data/cli_expected_output")
RESULTS = Path("tests/test_data/results_files")

runner = CliRunner()


def test_cli_version():
    cmd = [sys.executable, "-m", "aa_remove_data", "--version"]
    assert subprocess.check_output(cmd).decode().strip() == __version__


def test_cli_reduce_to_period():
    read = TEST_DATA / "SCALAR_STRING_test_data.pb"
    write = RESULTS / "SCALAR_STRING_reduce_to_period.pb"
    expected = CLI_OUTPUT / "SCALAR_STRING_reduce_to_period.pb"
    period = "4.5"
    cmd = [
        "to-period",
        str(read),
        period,
        f"--new-filename={write}",
    ]
    result = result = runner.invoke(app, cmd)
    print(result.stdout)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_reduce_to_period_check_period():
    read = TEST_DATA / "RAW:2025_short.pb"
    write = RESULTS / "RAW:2025_test_reduce_to_period.pb"
    period = 3
    cmd = [
        "to-period",
        str(read),
        str(period),
        f"--new-filename={write}",
    ]
    result = result = runner.invoke(app, cmd)
    print(result.stdout)
    ad = ArchiverData(write)
    samples = list(ad.get_samples())
    for i in range(len(samples) - 1):
        seconds_diff = samples[i + 1].secondsintoyear - samples[i].secondsintoyear
        nano_diff = samples[i + 1].nano - samples[i].nano
        assert seconds_diff >= period + 1 or (seconds_diff == period and nano_diff >= 0)
    write.unlink()


def test_cli_reduce_to_period_backup():
    read = TEST_DATA / "RAW:2025_short.pb"
    write = RESULTS / "tmp.pb"
    backup = RESULTS / "RAW:2025_short_reduce_to_period_backup.pb"
    expected = read
    period = "3"
    cmd = [
        "to-period",
        str(read),
        period,
        f"--new-filename={write}",
        f"--backup-filename={backup}",
    ]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    try_to_remove(write)
    are_identical = filecmp.cmp(backup, expected, shallow=False)
    assert are_identical
    backup.unlink()


def test_cli_reduce_to_period_txt():
    read = TEST_DATA / "SCALAR_STRING_test_data.pb"
    write = RESULTS / "SCALAR_STRING_reduce_to_period_txt.pb"
    expected = CLI_OUTPUT / "SCALAR_STRING_reduce_to_period.txt"
    txt_path = write.with_suffix(".txt")
    period = "4.5"
    cmd = [
        "to-period",
        str(read),
        period,
        "-t",
        f"--new-filename={write}",
    ]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    try_to_remove(write)
    are_identical = filecmp.cmp(txt_path, expected, shallow=False)
    assert are_identical
    txt_path.unlink()


def test_cli_reduce_to_period_non_existent_filename():
    read = TEST_DATA / "this/file/does_not_exist.pb"
    period = "10"
    cmd = ["to-period", str(read), period]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    assert result.exit_code != 0
    assert isinstance(result.exception, FileNotFoundError)


def test_cli_reduce_to_period_invalid_filename():
    read = TEST_DATA / "SCALAR_STRING_test_data.jpeg"
    period = "10"
    cmd = ["to-period", str(read), period]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_cli_remove_reduce_to_period_invalid_new_filename():
    read = TEST_DATA / "dummy_file.pb"
    write = RESULTS / "SCALAR_STRING_reduce_to_period.hdf5"
    period = "10"
    cmd = ["to-period", str(read), period, f"--new-filename={write}"]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_cli_reduce_to_period_invalid_backup_filename():
    read = TEST_DATA / "dummy_file.pb"
    backup = RESULTS / "SCALAR_STRING_reduce_to_period_backup.zip"
    period = "10"
    cmd = ["to-period", str(read), period, f"--backup-filename={backup}"]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_cli_reduce_by_factor():
    read = TEST_DATA / "SCALAR_STRING_test_data.pb"
    write = RESULTS / "SCALAR_STRING_reduce_by_factor.pb"
    expected = CLI_OUTPUT / "SCALAR_STRING_reduce_by_factor.pb"
    factor = "3"
    cmd = [
        "by-factor",
        str(read),
        factor,
        f"--new-filename={write}",
    ]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_reduce_by_factor_backup():
    read = TEST_DATA / "SCALAR_STRING_test_data.pb"
    write = RESULTS / "tmp.pb"
    backup = RESULTS / "SCALAR_STRING_reduce_by_factor_backup.pb"
    expected = read
    factor = "3"
    cmd = [
        "by-factor",
        str(read),
        factor,
        f"--new-filename={write}",
        f"--backup-filename={backup}",
    ]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    try_to_remove(write)
    are_identical = filecmp.cmp(backup, expected, shallow=False)
    assert are_identical
    backup.unlink()


def test_cli_reduce_by_factor_txt():
    read = TEST_DATA / "SCALAR_STRING_test_data.pb"
    write = RESULTS / "SCALAR_STRING_reduce_by_factor_txt.pb"
    expected = CLI_OUTPUT / "SCALAR_STRING_reduce_by_factor.txt"
    txt_path = write.with_suffix(".txt")
    factor = "3"
    cmd = [
        "by-factor",
        str(read),
        factor,
        "-t",
        f"--new-filename={write}",
    ]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    try_to_remove(write)
    are_identical = filecmp.cmp(txt_path, expected, shallow=False)
    assert are_identical
    txt_path.unlink()


def test_cli_reduce_by_factor_non_existent_filename():
    read = TEST_DATA / "this/file/does_not_exist.pb"
    factor = "10"
    cmd = ["by-factor", str(read), factor]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, FileNotFoundError)


def test_cli_reduce_by_factor_invalid_filename():
    read = TEST_DATA / "SCALAR_STRING_test_data.jpeg"
    factor = "5"
    cmd = ["by-factor", str(read), factor]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_cli_remove_reduce_by_factor_invalid_new_filename():
    read = TEST_DATA / "dummy_file.pb"
    write = RESULTS / "SCALAR_STRING_reduce_by_factor.hdf5"
    factor = "5"
    cmd = ["by-factor", str(read), factor, f"--new-filename={write}"]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_cli_reduce_by_factor_invalid_backup_filename():
    read = TEST_DATA / "dummy_file.pb"
    backup = RESULTS / "SCALAR_STRING_reduce_by_factor_backup.zip"
    factor = "5"
    cmd = ["by-factor", str(read), factor, f"--backup-filename={backup}"]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_cli_remove_before():
    read = TEST_DATA / "SCALAR_STRING_test_data.pb"
    write = RESULTS / "SCALAR_STRING_remove_before.pb"
    expected = CLI_OUTPUT / "SCALAR_STRING_remove_before.pb"
    ts = "1,1,0,1,5"
    cmd = ["remove-before", str(read), ts, f"--new-filename={write}"]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_remove_before_backup():
    read = TEST_DATA / "SCALAR_STRING_test_data.pb"
    write = RESULTS / "tmp.pb"
    backup = RESULTS / "SCALAR_STRING_remove_before_backup.pb"
    expected = read
    ts = "1,1,0,1,5"
    cmd = [
        "remove-before",
        str(read),
        ts,
        f"--new-filename={write}",
        f"--backup-filename={backup}",
    ]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    try_to_remove(write)
    are_identical = filecmp.cmp(backup, expected, shallow=False)
    assert are_identical
    backup.unlink()


def test_cli_remove_before_txt():
    read = TEST_DATA / "SCALAR_STRING_test_data.pb"
    write = RESULTS / "SCALAR_STRING_remove_before_txt.pb"
    expected = CLI_OUTPUT / "SCALAR_STRING_remove_before.txt"
    txt_path = write.with_suffix(".txt")
    ts = "1,1,0,1,5"
    cmd = [
        "remove-before",
        str(read),
        ts,
        "-t",
        f"--new-filename={write}",
    ]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    try_to_remove(write)
    are_identical = filecmp.cmp(txt_path, expected, shallow=False)
    assert are_identical
    txt_path.unlink()


def test_cli_remove_before_non_existent_filename():
    read = TEST_DATA / "this/file/does_not_exist.pb"
    ts = "1,1,0,1,5"
    cmd = ["remove-before", str(read), ts]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, FileNotFoundError)


def test_cli_remove_before_invalid_filename():
    read = TEST_DATA / "SCALAR_STRING_test_data.jpeg"
    ts = "1,1,0,1,5"
    cmd = ["remove-before", str(read), ts]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_cli_remove_before_invalid_new_filename():
    read = TEST_DATA / "dummy_file.pb"
    write = RESULTS / "SCALAR_STRING_remove_before.hdf5"
    ts = "1,1,0,1,5"
    cmd = ["remove-before", str(read), ts, f"--new-filename={write}"]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_cli_remove_before_invalid_backup_filename():
    read = TEST_DATA / "dummy_file.pb"
    backup = RESULTS / "SCALAR_STRING_remove_before_backup.zip"
    ts = "1,1,0,1,5"
    cmd = ["remove-before", str(read), ts, f"--backup-filename={backup}"]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_cli_remove_after():
    read = TEST_DATA / "SCALAR_STRING_test_data.pb"
    write = RESULTS / "SCALAR_STRING_remove_after.pb"
    expected = CLI_OUTPUT / "SCALAR_STRING_remove_after.pb"
    ts = "1,1,0,1,5"
    cmd = ["remove-after", str(read), ts, f"--new-filename={write}"]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical
    write.unlink()


def test_cli_remove_after_backup():
    read = TEST_DATA / "SCALAR_STRING_test_data.pb"
    write = RESULTS / "tmp.pb"
    backup = RESULTS / "SCALAR_STRING_remove_after_backup.pb"
    expected = read
    ts = "1,1,0,1,5"
    cmd = [
        "remove-after",
        str(read),
        ts,
        f"--new-filename={write}",
        f"--backup-filename={backup}",
    ]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    try_to_remove(write)
    are_identical = filecmp.cmp(backup, expected, shallow=False)
    assert are_identical
    backup.unlink()


def test_cli_remove_after_txt():
    read = TEST_DATA / "SCALAR_STRING_test_data.pb"
    write = RESULTS / "SCALAR_STRING_remove_after_txt.pb"
    expected = CLI_OUTPUT / "SCALAR_STRING_remove_after.txt"
    txt_path = write.with_suffix(".txt")
    ts = "1,1,0,1,5"
    cmd = ["remove-after", str(read), ts, "-t", f"--new-filename={write}"]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    try_to_remove(write)
    are_identical = filecmp.cmp(txt_path, expected, shallow=False)
    assert are_identical
    txt_path.unlink()


def test_cli_remove_after_non_existent_filename():
    read = TEST_DATA / "this/file/does_not_exist.pb"
    ts = "1,1,0,1,5"
    cmd = ["remove-after", str(read), ts]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, FileNotFoundError)


def test_cli_remove_after_invalid_filename():
    read = TEST_DATA / "SCALAR_STRING_test_data.jpeg"
    ts = "1,1,0,1,5"
    cmd = ["remove-after", str(read), ts]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_cli_remove_after_invalid_new_filename():
    read = TEST_DATA / "dummy_file.pb"
    write = RESULTS / "SCALAR_STRING_remove_after.hdf5"
    ts = "1,1,0,1,5"
    cmd = ["remove-after", str(read), ts, f"--new-filename={write}"]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_cli_remove_after_invalid_backup_filename():
    read = TEST_DATA / "dummy_file.pb"
    backup = RESULTS / "SCALAR_STRING_remove_after_backup.zip"
    ts = "1,1,0,1,5"
    cmd = ["remove-after", str(read), ts, f"--backup-filename={backup}"]
    result = runner.invoke(app, cmd)

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def try_to_remove(filepath: PathLike):
    filepath = Path(filepath)
    if filepath.is_file():
        filepath.unlink()
