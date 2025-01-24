import argparse
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from aa_remove_data.pb_utils import DEFAULT_CHUNK_SIZE, PBUtils


def get_nano_diff(sample1: Any, sample2: Any) -> int:
    """Get the difference in nano seconds between two samples.

    Args:
        sample1 (type): An Archiver Appliance sample.
        sample2 (type): Another Archiver Appliance sample.

    Returns:
        int: Difference in nanoseconds.
    """
    diff = (sample2.secondsintoyear - sample1.secondsintoyear) * 10**9 + (
        sample2.nano - sample1.nano
    )
    if not diff > 0:
        raise ValueError(
            f"diff ({diff}) is non-positive - ensure sample2 comes after sample1."
        )
    return diff


def get_seconds_diff(sample1: type, sample2: type) -> int:
    """Get the difference in whole seconds between two samples.

    Args:
        sample1 (type): An Archiver Appliance sample.
        sample2 (type): Another Archiver Appliance sample.

    Returns:
        int: Difference in seconds
    """
    diff = sample2.secondsintoyear - sample1.secondsintoyear
    if not diff >= 0:
        raise ValueError(
            f"diff ({diff}) is negative - ensure sample2 comes after sample1."
        )
    return diff


def apply_min_period(
    samples: list,
    period: float,
    initial_sample: type | None = None,
) -> list:
    """Reduce the frequency of a list of samples. Specify the desired minimum period.

    Args:
        samples (list): List of samples.
        period (float): Desired minimum period between adjacent samples.
        initial_sample (type, optional): An initial sample to find an initial diff.

    Returns:
        list: Reduced list of samples
    """
    seconds_delta = period
    nano_delta = (seconds_delta * 10**9) // 1
    diff = 0
    if not nano_delta >= 1:
        raise ValueError(f"Period ({period}) must be at least 1 nanosecond.")

    if seconds_delta >= 5:  # Save time for long periods by ignoring nano
        delta = seconds_delta
        get_diff = get_seconds_diff
    else:
        delta = nano_delta  # For short periods still count nano
        get_diff = get_nano_diff

    if initial_sample is not None:
        diff = get_diff(initial_sample, samples[0])
        if diff >= delta:
            reduced_samples = [samples[0]]
            diff = 0
        else:
            reduced_samples = []
    else:
        diff = 0
        reduced_samples = [samples[0]]
    for i in range(len(samples) - 1):
        diff += get_diff(samples[i], samples[i + 1])
        if diff >= delta:
            reduced_samples.append(samples[i + 1])
            diff = 0
    return reduced_samples


def get_index_at_timestamp(
    samples: list, seconds: int, nano: int = 0
) -> tuple[int, float]:
    """Get index of the sample closest to a timestamp.

    Args:
        samples (list): List of samples.
        seconds (int): Seconds portion of timestamp (into the year).
        nano (int, optional): Nanoseconds portion of timestamp. Defaults to 0.

    Returns:
        tuple[int, float]: Index of closest sample, difference in nanoseconds
        between the target timestamp and sample.
    """
    target = seconds * 10**9 + nano
    last_diff = target - (samples[0].secondsintoyear * 10**9 + samples[0].nano)
    for i, sample in enumerate(samples):
        diff = target - (sample.secondsintoyear * 10**9 + sample.nano)
        if abs(last_diff) < abs(diff):
            return i - 1, last_diff
        last_diff = diff
    return len(samples) - 1, last_diff


def remove_before_ts(samples: list, seconds: int, nano: int = 0) -> list:
    """Remove all samples before a certain timestamp.

    Args:
        samples (list): List of samples.
        seconds (int): Seconds portion of timestamp.
        nano (int, optional): Nanoseconds portion of timestamp. Defaults to 0.

    Returns:
        list: Reduced list of samples.
    """
    index, diff = get_index_at_timestamp(samples, seconds, nano)
    if diff > 0:
        return samples[index + 1 :]
    else:
        return samples[index:]


def remove_after_ts(samples: list, seconds: int, nano: int = 0) -> list:
    """Remove all samples after a certain timestamp.

    Args:
        samples (list): List of samples.
        seconds (int): Seconds portion of timestamp.
        nano (int, optional): Nanoseconds portion of timestamp. Defaults to 0.

    Returns:
        list: Reduced list of samples.
    """
    index, diff = get_index_at_timestamp(samples, seconds, nano)
    if diff >= 0:
        return samples[: index + 1]
    else:
        return samples[:index]


def keep_every_nth(
    samples: list, n: int, block_size: int = 1, initial: int = 0
) -> list:
    """Reduce the size of a list of samples, keeping every nth sample and
    removing the rest. The samples can be grouped together into blocks, so
    that every nth block is kept.

    Args:
        samples (list): List of samples
        n (int): Every nth sample (or block of samples) will be kept.
        block_size (int, optional): Number of samples per block. Defaults to 1.
        initial (int, optional): End point of processing from a previous chunk.

    Returns:
        list: Reduced list of samples.
    """
    if n <= 0:
        raise ValueError(f"n = {n}, must be >= 1")
    elif block_size <= 0:
        raise ValueError(f"block_size = {block_size}, must be >= 1")
    if block_size == 1:
        return samples[n - 1 - initial :: n]
    else:
        return [
            item
            for i, item in enumerate(samples)
            if (i + block_size + initial) // block_size % n == 0
        ]


def remove_every_nth(
    samples: list, n: int, block_size: int = 1, initial: int = 0
) -> list:
    """Reduce the size of a list of samples by removing every nth sample. The
    samples can be grouped together into blocks, so that every nth block is
    removed.

    Args:
        samples (list): List of samples
        n (int): Every nth sample (or block of samples) will be removed.
        block_size (int, optional): Number of samples per block. Defaults to 1.
        initial (int, optional): End point of processing from a previous chunk.

    Returns:
        list: Reduced list of samples.
    """
    if n <= 0:
        raise ValueError(f"n = {n}, must be >= 1")
    elif block_size <= 0:
        raise ValueError(f"block_size = {block_size}, must be >= 1")
    return [
        item
        for i, item in enumerate(samples)
        if (i + block_size + initial) // block_size % n != 0
    ]


def add_generic_args(parser):
    parser.add_argument(
        "filename", type=str, help="path/to/file.pb of PB file being processed"
    )
    parser.add_argument(
        "--new-filename",
        type=str,
        default=None,
        help="path/to/file.pb of new file to write to "
        + "(default: writes over original file)",
    )
    parser.add_argument(
        "--backup-filename",
        type=str,
        default=None,
        help="path/to/file.pb of a backup file, "
        + "(default: {original_filename}_backup.pb)",
    )
    parser.add_argument(
        "-t",
        "--write-txt",
        action="store_true",
        help="write result to a .txt file (default: False)",
    )
    parser.add_argument(
        "--chunk",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"chunk size in lines (default: {DEFAULT_CHUNK_SIZE})",
    )
    return parser


def validate_pb_file(filepath, should_exist=False):
    filepath = Path(filepath)
    if filepath.suffix != ".pb":
        raise ValueError(
            f"Invalid file extension for {filepath}: '{filepath.suffix}'. "
            + "Expected '.pb'."
        )
    if should_exist:
        if not filepath.is_file():
            raise FileNotFoundError(f"{filepath} is not a filepath.")


def process_generic_args(args):
    validate_pb_file(args.filename, should_exist=True)

    if args.backup_filename is None and (args.new_filename in (None, args.filename)):
        args.backup_filename = args.filename.replace(".pb", "_backup.pb")
        args.new_filename = args.filename
    elif args.new_filename is None:
        args.new_filename = args.filename
    if args.backup_filename in (args.filename, args.new_filename):
        raise ValueError(
            f"Backup filename {args.backup_filename} cannot be the same as filename or"
            + " new-filename"
        )

    validate_pb_file(args.new_filename)
    if args.backup_filename is not None:
        validate_pb_file(args.backup_filename)
    return args


def aa_reduce_to_period():
    """Reduce the frequency of data in a PB file by setting a minimum period between
    data points."""
    parser = argparse.ArgumentParser()
    parser = add_generic_args(parser)
    parser.add_argument(
        "period", type=float, help="Minimum period between each data point"
    )
    args = parser.parse_args()
    args = process_generic_args(args)

    filename = Path(args.filename)
    new_pb = Path(args.new_filename)
    if args.backup_filename is not None:
        subprocess.run(["cp", filename, Path(args.backup_filename)], check=True)

    txt_filepath = new_pb.with_suffix(".txt")
    pb = PBUtils(chunk_size=args.chunk)
    last_sample = None
    while pb.read_done is False:
        pb.read_pb(filename)
        pb.samples = apply_min_period(
            pb.samples, period=args.period, initial_sample=last_sample
        )
        pb.write_pb(new_pb)
        if args.write_txt:
            pb.write_to_txt(txt_filepath)
        if pb.samples:
            last_sample = pb.samples[-1]


def aa_reduce_by_factor():
    """Reduce the number of data points in a PB file by a certain factor by removing all
    but every nth."""
    parser = argparse.ArgumentParser()
    parser = add_generic_args(parser)
    parser.add_argument("factor", type=int, help="factor to reduce the data by")
    parser.add_argument("--block", type=int, default=1)
    args = parser.parse_args()
    args = process_generic_args(args)

    filename = Path(args.filename)
    new_pb = Path(args.new_filename)
    if args.backup_filename is not None:
        subprocess.run(["cp", filename, Path(args.backup_filename)], check=True)

    txt_filepath = new_pb.with_suffix(".txt")
    pb = PBUtils(chunk_size=args.chunk)
    initial = 0
    while pb.read_done is False:
        pb.read_pb(filename)
        pb.samples = keep_every_nth(
            pb.samples, args.factor, block_size=args.block, initial=initial
        )
        pb.write_pb(new_pb)
        if args.write_txt:
            pb.write_to_txt(txt_filepath)
        initial = (args.chunk + initial) % (args.factor * args.block)


def aa_remove_every_nth():
    """Remove every nth data point in a PB file."""
    parser = argparse.ArgumentParser()
    parser = add_generic_args(parser)
    parser.add_argument("n", type=int, help="remove every nth data point")
    parser.add_argument("--block", type=int, default=1)
    args = parser.parse_args()
    args = process_generic_args(args)

    filename = Path(args.filename)
    new_pb = Path(args.new_filename)
    if args.backup_filename is not None:
        subprocess.run(["cp", filename, Path(args.backup_filename)], check=True)

    txt_filepath = new_pb.with_suffix(".txt")
    pb = PBUtils(chunk_size=args.chunk)
    initial = 0
    while pb.read_done is False:
        pb.read_pb(filename)
        pb.samples = remove_every_nth(
            pb.samples, args.n, block_size=args.block, initial=initial
        )
        pb.write_pb(new_pb)
        if args.write_txt:
            pb.write_to_txt(txt_filepath)
        initial = (args.chunk + initial) % (args.n * args.block)


def aa_remove_data_before():
    """Remove all data points before a certain timestamp in a PB file."""
    parser = argparse.ArgumentParser()
    parser = add_generic_args(parser)
    parser.add_argument(
        "--ts",
        nargs="+",
        type=int,
        required=True,
        metavar="timestamp",
        help="timestamp in the form 'month day hour minute second nanosecond' "
        + "- month is required (default: {month} 1 0 0 0 0)",
    )
    args = parser.parse_args()
    args = process_generic_args(args)

    filename = Path(args.filename)
    new_pb = Path(args.new_filename)
    if args.backup_filename is not None:
        subprocess.run(["cp", filename, Path(args.backup_filename)], check=True)
    timestamp = args.ts
    if not len(timestamp) <= 6:
        raise ValueError(
            "Give timestamp in the form 'month day hour minute second nanosecond'. "
            + "Month is required. All must be integers."
        )

    pb_header = PBUtils(Path(args.filename), chunk_size=0)
    year = pb_header.header.year

    if len(timestamp) == 6:
        nano = timestamp.pop(5)
    else:
        nano = 0
    if len(timestamp) == 1:
        timestamp.append(1)

    diff = datetime(*([year] + timestamp)) - datetime(year, 1, 1)
    seconds = int(diff.total_seconds())
    txt_filepath = new_pb.with_suffix(".txt")
    pb = PBUtils(chunk_size=args.chunk)
    while pb.read_done is False:
        pb.read_pb(filename)
        pb.samples = remove_before_ts(pb.samples, seconds, nano=nano)
        pb.write_pb(new_pb)
        if args.write_txt:
            pb.write_to_txt(txt_filepath)


def aa_remove_data_after():
    """Remove all data points after a certain timestamp in a PB file."""
    parser = argparse.ArgumentParser()
    parser = add_generic_args(parser)
    parser.add_argument(
        "--ts",
        nargs="+",
        type=int,
        required=True,
        metavar="timestamp",
        help="timestamp in the form 'month day hour minute second nanosecond' "
        + "- month is required (default: {month} 1 0 0 0 0)",
    )
    args = parser.parse_args()
    args = process_generic_args(args)

    filename = Path(args.filename)
    new_pb = Path(args.new_filename)
    if args.backup_filename is not None:
        subprocess.run(["cp", filename, Path(args.backup_filename)], check=True)

    timestamp = args.ts
    if not len(timestamp) <= 6:
        raise ValueError(
            "Give timestamp in the form 'month day hour minute second nanosecond'. "
            + "Month is required. All must be integers."
        )

    pb_header = PBUtils(Path(args.filename), chunk_size=0)
    year = pb_header.header.year

    if len(timestamp) == 6:
        nano = timestamp.pop(5)
    else:
        nano = 0
    if len(timestamp) == 1:
        timestamp.append(1)

    diff = datetime(*([year] + timestamp)) - datetime(year, 1, 1)
    seconds = int(diff.total_seconds())
    txt_filepath = new_pb.with_suffix(".txt")
    pb = PBUtils(chunk_size=args.chunk)
    while pb.read_done is False:
        pb.read_pb(filename)
        pb.samples = remove_after_ts(pb.samples, seconds, nano=nano)
        pb.write_pb(new_pb)
        if args.write_txt:
            pb.write_to_txt(txt_filepath)
