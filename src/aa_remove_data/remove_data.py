import subprocess
from datetime import datetime
from pathlib import Path

import typer

from aa_remove_data.algorithms import (
    apply_min_period,
    reduce_by_factor,
    remove_after_ts,
    remove_before_ts,
)
from aa_remove_data.archiver_data import ArchiverData

app = typer.Typer()

FILENAME_ARGUMENT = typer.Argument(help="path/to/file.pb of PB file being processed")
NEW_FILENAME_OPTION = typer.Option(None, help="path/to/file.pb of new file to write to")
BACKUP_FILENAME_OPTION = typer.Option(None, help="path/to/file.pb of a backup file")
WRITE_TXT_OPTION = typer.Option(
    False, "--write-txt", "-t", help="Write result to text file"
)


@app.command()
def to_period(
    filename: Path = FILENAME_ARGUMENT,
    period: float = typer.Argument(help="Minimum period between each data point"),
    new_filename: Path | None = NEW_FILENAME_OPTION,
    backup_filename: Path | None = BACKUP_FILENAME_OPTION,
    write_txt: bool = WRITE_TXT_OPTION,
):
    """Reduce the frequency of data in a PB file by setting a minimum period between
    data points."""
    f, new_f, backup_f = process_filenames(filename, new_filename, backup_filename)
    if backup_f is not None:
        subprocess.run(["cp", f, backup_f], check=True)

    ad = ArchiverData(f)
    ad.process_and_write(new_f, write_txt, apply_min_period, [period])


@app.command()
def by_factor(
    filename: Path = FILENAME_ARGUMENT,
    factor: float = typer.Argument(help="Factor to reduce the data by"),
    new_filename: Path | None = NEW_FILENAME_OPTION,
    backup_filename: Path | None = BACKUP_FILENAME_OPTION,
    write_txt: bool = WRITE_TXT_OPTION,
):
    """Reduce the number of data points in a PB file by a certain factor."""
    f, new_f, backup_f = process_filenames(filename, new_filename, backup_filename)
    if backup_f is not None:
        subprocess.run(["cp", f, backup_f], check=True)

    ad = ArchiverData(f)
    ad.process_and_write(new_f, write_txt, reduce_by_factor, [factor], raw=True)


@app.command()
def remove_before(
    filename: Path = FILENAME_ARGUMENT,
    timestamp: str = typer.Argument(
        help="{month,day,hour,minute,second,nanosecond}. Month is required"
    ),
    new_filename: Path | None = NEW_FILENAME_OPTION,
    backup_filename: Path | None = BACKUP_FILENAME_OPTION,
    write_txt: bool = WRITE_TXT_OPTION,
):
    """Remove all data points before a certain timestamp in a PB file."""
    f, new_f, backup_f = process_filenames(filename, new_filename, backup_filename)
    if backup_f is not None:
        subprocess.run(["cp", f, backup_f], check=True)

    ad = ArchiverData(f)
    seconds, nano = process_timestamp(ad.header.year, timestamp)
    ad.process_and_write(new_f, write_txt, remove_before_ts, [seconds, nano])


@app.command()
def remove_after(
    filename: Path = FILENAME_ARGUMENT,
    timestamp: str = typer.Argument(
        help="{month,day,hour,minute,second,nanosecond}. Month is required"
    ),
    new_filename: Path | None = NEW_FILENAME_OPTION,
    backup_filename: Path | None = BACKUP_FILENAME_OPTION,
    write_txt: bool = WRITE_TXT_OPTION,
):
    """Remove all data points after a certain timestamp in a PB file."""
    f, new_f, backup_f = process_filenames(filename, new_filename, backup_filename)
    if backup_f is not None:
        subprocess.run(["cp", f, backup_f], check=True)

    ad = ArchiverData(f)
    seconds, nano = process_timestamp(ad.header.year, timestamp)
    ad.process_and_write(new_f, write_txt, remove_after_ts, [seconds, nano])


def validate_pb_file(filepath: Path, should_exist: bool = False):
    """Validate a file ensuring it has a .pb extension and, optionally, exists.

    Args:
        filepath (Path): Filepath being validated.
        should_exist (bool, optional): Requires file to exist. Defaults to False.

    Raises:
        ValueError: Raised if the filepath does not have a .pb extension.
        FileNotFoundError: Raised if the file should exist, and doesn't.
    """
    filepath = Path(filepath)
    if filepath.suffix != ".pb":
        raise ValueError(
            f"Invalid file extension for '{filepath}': '{filepath.suffix}'. "
            + "Expected '.pb'."
        )
    if should_exist and not filepath.is_file():
        raise FileNotFoundError(f"No such file: '{filepath}'")


def process_filenames(
    f: Path, new_f: Path | None, backup_f: Path | None
) -> tuple[Path, Path, Path | None]:
    """Process and validate filename, new filename and backup filenames provided by the
    user.

    Args:
        f (Path): Path to PB file beign processed.
        new_f (Path | None): Destination path for processed PB file.
        backup_f (Path | None): Path to backup file, a copy of the original PB file.

    Raises:
        ValueError: Raised if backup filename is the same as any of the others.

    Returns:
        tuple[Path, Path, Path | None]: Tuple containing valid filenames for the
        original, new and backup PB files.
    """
    validate_pb_file(f, should_exist=True)

    if backup_f is None and (new_f in (None, f)):
        backup_f = f.with_stem(f"{f.stem}_backup")
        new_f = f
    elif new_f is None:
        new_f = f
    if backup_f in (f, new_f):
        raise ValueError(
            f"Backup filename {backup_f} cannot be the same as filename or"
            + " new-filename"
        )
    validate_pb_file(new_f)
    if backup_f is not None:
        validate_pb_file(backup_f)
    return f, new_f, backup_f


def process_timestamp(year: int, timestamp: str) -> tuple[int, int]:
    """Convert a timestamp entered by a user into a number of seconds into the year and
    nanoseconds.

    Args:
        year (int): Year of the timestamp
        timestamp (str): String containing the month, day, hour, minute, second,
        nanosecond of the timestamp, seperated by commas.

    Raises:
        ValueError: Raised if the user entered too many values for the timestamp.

    Returns:
        tuple[int, int]: Tuple containing seconds and nanoseconds that capture the
        timestamp.
    """
    ts = [1, 1, 0, 0, 0, 0]
    for i, value in enumerate(timestamp.split(",")):
        ts[i] = int(value)
    nano = ts.pop(5)
    if len(ts) > 5:
        raise ValueError(
            "Give timestamp in the form 'month,day,hour,minute,second,nanosecond'. "
            + "Month is required. All must be integers."
        )
    month, day, hour, minute, second = ts
    diff = datetime(year, month, day, hour, minute, second) - datetime(year, 1, 1)
    seconds = int(diff.total_seconds())
    return seconds, nano
