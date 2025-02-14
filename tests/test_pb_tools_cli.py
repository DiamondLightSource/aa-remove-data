import filecmp
import subprocess
import sys
from os import PathLike
from pathlib import Path

from typer.testing import CliRunner

from aa_remove_data import __version__
from aa_remove_data.pb_tools import app

TEST_DATA = Path("tests/test_data")
CLI_OUTPUT = Path("tests/test_data/cli_expected_output")
RESULTS = Path("tests/test_data/results_files")

runner = CliRunner()


def try_to_remove(filepath: PathLike):
    filepath = Path(filepath)
    if filepath.is_file():
        filepath.unlink()


def test_cli_version():
    cmd = [sys.executable, "-m", "aa_remove_data", "--version"]
    assert subprocess.check_output(cmd).decode().strip() == __version__


def test_cli_print_header():
    cmd = ["print-header", str(TEST_DATA / "P:2021_short.pb")]
    expected = "Name: BL13I-VA-GAUGE-28:P, Type: SCALAR_DOUBLE, Year: 2021\n"
    result = runner.invoke(app, cmd)
    assert result.stdout == expected


def test_cli_print_header_with_lines():
    cmd = ["print-header", str(TEST_DATA / "RAW:2025_short.pb"), "--lines=3"]
    expected = (
        "Name: BL11K-EA-ADC-01:M4:CH4:RAW, Type: SCALAR_INT, Year: 2025\n"
        + "DATE                   SECONDS     NANO         VAL\n"
        + "2025-01-01 00:00:00           0      2588941    -1850\n"
        + "2025-01-01 00:00:00           0    102596158    -2544\n"
        + "2025-01-01 00:00:00           0    202583899    -2351\n"
    )
    result = runner.invoke(app, cmd)
    assert result.stdout == expected


def test_cli_pb_2_txt():
    read = TEST_DATA / "RAW:2025_short.pb"
    write = RESULTS / "RAW:2025_short_test_cli_pb_2_txt.txt"
    expected = TEST_DATA / "RAW:2025_short.txt"
    cmd = ["pb-2-txt", str(read), str(write)]
    result = runner.invoke(app, cmd)
    print(result.stdout)
    are_identical = filecmp.cmp(write, expected, shallow=False)
    assert are_identical is True
    if are_identical:
        write = Path(write)
        write.unlink()
