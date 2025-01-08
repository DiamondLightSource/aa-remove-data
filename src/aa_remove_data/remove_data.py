import argparse
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from aa_remove_data.pb_utils import PBUtils


def get_nano_diff(sample1: type, sample2: type) -> int:
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
    assert diff > 0
    return diff


def get_seconds_diff(sample1: type, sample2: type) -> int:
    """Get the difference in whole seconds between two samples.

    Args:
        sample1 (type): An Archiver Appliance sample.
        sample2 (type): Another Archiver Appliance sample.

    Returns:
        _type_: Difference in seconds
    """
    diff = sample2.secondsintoyear - sample1.secondsintoyear
    assert diff >= 0
    return diff


def benchmark(function: Callable, *args: Any, **kwargs: Any) -> Any:
    """Print the time taken for a function to execute.

    Args:
        function (Callable): Function to execute.

    Returns:
        Any: Result of the function.
    """
    start_time = time.time()
    result = function(*args, **kwargs)
    end_time = time.time()
    print(f"Time taken for {function.__name__}: {end_time - start_time:.2f}s.")
    return result


def reduce_freq(samples: list, freq: float = 0, period: float = 0) -> list:
    """Reduce the frequency of a list of samples. Specify the desired frequency
    or period (not both).

    Args:
        samples (list): _description_
        freq (float, optional): Desired frequency. Defaults to 0.
        period (float, optional): Desired period. Defaults to 0.

    Returns:
        list: Reduced list of samples
    """
    assert (
        freq * period == 0 and (freq + period) > 0
    ), "Must set either frequency or period, not both or none."
    if freq:
        seconds_delta = 1 / freq
    else:
        seconds_delta = period
    nano_delta = (seconds_delta * 10**9) // 1
    i = len(samples) - 1
    diff = 0
    assert nano_delta >= 1, "Must have a period of more than 1 nanosecond."
    if seconds_delta >= 5:  # Save time for long periods by ignoring nano
        delta = seconds_delta
        get_diff = get_seconds_diff
    else:
        delta = nano_delta  # For short periods still count nano
        get_diff = get_nano_diff
    reduced_samples = [samples[-1]]
    for i in range(len(samples) - 2, -1, -1):
        diff += get_diff(samples[i], samples[i + 1])
        if diff >= delta:
            reduced_samples.append(samples[i])
            diff = 0
    return list(reversed(reduced_samples))


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
    if diff > 0:
        return samples[: index + 1]
    else:
        return samples[:index]


def keep_every_nth(samples: list, n: int, block_size: int = 1) -> list:
    """Reduce the size of a list of samples, keeping every nth sample and
    removing the rest. The samples can be grouped together into blocks, so
    that every nth block is kept.

    Args:
        samples (list): List of samples
        n (int): Every nth sample (or block of samples) will be kept.
        block_size (int, optional): Number of samples per block. Defaults to 1.

    Returns:
        list: Reduced list of samples.
    """
    if block_size == 1:
        return samples[n - 1 :: n]
    else:
        return [
            item
            for i, item in enumerate(samples)
            if (i + block_size) // block_size % n == 0
        ]


def remove_every_nth(samples: list, n: int, block_size: int = 1) -> list:
    """Reduce the size of a list of samples by removing every nth sample. The
    samples can be grouped together into blocks, so that every nth block is
    removed.

    Args:
        samples (list): List of samples
        n (int): Every nth sample (or block of samples) will be removed.
        block_size (int, optional): Number of samples per block. Defaults to 1.

    Returns:
        list: Reduced list of samples.
    """
    if block_size == 1:
        return [item for i, item in enumerate(samples) if (i + 1) % n != 0]
    else:
        return [
            item
            for i, item in enumerate(samples)
            if (i + block_size) // block_size % n != 0
        ]


def aa_reduce_freq():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("period", type=float)
    parser.add_argument("--new_filename", type=str, default=None)
    parser.add_argument("--backup_filename", type=str, default=None)
    parser.add_argument("--write_txt", action="store_true")
    args = parser.parse_args()

    assert args.filename.endswith(".pb")

    if args.new_filename is None:
        new_pb = Path(args.filename)
    else:
        assert args.new_filename.endswith(".pb")
        new_pb = Path(args.new_filename)

    if args.backup_filename is None:
        backup_pb = Path(args.filename.strip(".pb") + "_backup.pb")
    else:
        assert args.backup_filename.endswith(".pb")
        backup_pb = Path(args.backup_filename)

    pb = PBUtils(Path(args.filename))
    pb.write_pb(backup_pb)
    pb.samples = reduce_freq(pb.samples, period=args.period)
    pb.write_pb(new_pb)
    if args.write_txt:
        txt_filepath = Path(str(new_pb).strip(".pb") + ".txt")
        pb.write_to_txt(txt_filepath)


def aa_reduce_by_factor():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("factor", type=int)
    parser.add_argument("--new_filename", type=str, default=None)
    parser.add_argument("--backup_filename", type=str, default=None)
    parser.add_argument("--write_txt", action="store_true")
    parser.add_argument("--block", type=int, default=1)
    args = parser.parse_args()

    assert args.filename.endswith(".pb")

    if args.new_filename is None:
        new_pb = Path(args.filename)
    else:
        assert args.new_filename.endswith(".pb")
        new_pb = Path(args.new_filename)

    if args.backup_filename is None:
        backup_pb = Path(args.filename.strip(".pb") + "_backup.pb")
    else:
        assert args.backup_filename.endswith(".pb")
        backup_pb = Path(args.backup_filename)

    pb = PBUtils(Path(args.filename))
    pb.write_pb(backup_pb)
    pb.samples = keep_every_nth(pb.samples, args.factor, block_size=args.block)
    pb.write_pb(new_pb)
    if args.write_txt:
        txt_filepath = Path(str(new_pb).strip(".pb") + ".txt")
        pb.write_to_txt(txt_filepath)


def aa_remove_every_nth():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("n", type=int)
    parser.add_argument("--new_filename", type=str, default=None)
    parser.add_argument("--backup_filename", type=str, default=None)
    parser.add_argument("--write_txt", action="store_true")
    parser.add_argument("--block", type=int, default=1)
    args = parser.parse_args()

    assert args.filename.endswith(".pb")

    if args.new_filename is None:
        new_pb = Path(args.filename)
    else:
        assert args.new_filename.endswith(".pb")
        new_pb = Path(args.new_filename)

    if args.backup_filename is None:
        backup_pb = Path(args.filename.strip(".pb") + "_backup.pb")
    else:
        assert args.backup_filename.endswith(".pb")
        backup_pb = Path(args.backup_filename)

    pb = PBUtils(Path(args.filename))
    pb.write_pb(backup_pb)
    pb.samples = remove_every_nth(pb.samples, args.n, block_size=args.block)
    pb.write_pb(new_pb)
    if args.write_txt:
        txt_filepath = Path(str(new_pb).strip(".pb") + ".txt")
        pb.write_to_txt(txt_filepath)


def aa_remove_data_before():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("--ts", nargs="+", type=int, required=True)
    parser.add_argument("--new_filename", type=str, default=None)
    parser.add_argument("--backup_filename", type=str, default=None)
    parser.add_argument("--write_txt", action="store_true")
    args = parser.parse_args()

    assert args.filename.endswith(".pb")

    if args.new_filename is None:
        new_pb = Path(args.filename)
    else:
        assert args.new_filename.endswith(".pb")
        new_pb = Path(args.new_filename)

    if args.backup_filename is None:
        backup_pb = Path(args.filename.strip(".pb") + "_backup.pb")
    else:
        assert args.backup_filename.endswith(".pb")
        backup_pb = Path(args.backup_filename)

    pb_header = PBUtils(Path(args.filename), chunk_size=0)
    year = pb_header.header.year
    timestamp = args.ts
    assert len(timestamp) <= 6, (
        "Give timestamp in the form 'month.day.hour.minute.second.nanosecond'. "
        + "Month is required. All must be integers."
    )
    if len(timestamp) == 6:
        nano = timestamp.pop(5)
    else:
        nano = 0
    if len(timestamp) == 1:
        timestamp.append(1)

    diff = datetime(*([year] + timestamp)) - datetime(year, 1, 1)
    seconds = int(diff.total_seconds())
    pb = PBUtils(Path(args.filename))
    pb.write_pb(backup_pb)
    pb.samples = remove_before_ts(pb.samples, seconds, nano=nano)
    pb.write_pb(new_pb)
    if args.write_txt:
        txt_filepath = Path(str(new_pb).strip(".pb") + ".txt")
        pb.write_to_txt(txt_filepath)


def aa_remove_data_after():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("--ts", nargs="+", type=int, required=True)
    parser.add_argument("--new_filename", type=str, default=None)
    parser.add_argument("--backup_filename", type=str, default=None)
    parser.add_argument("--write_txt", action="store_true")
    args = parser.parse_args()

    assert args.filename.endswith(".pb")

    if args.new_filename is None:
        new_pb = Path(args.filename)
    else:
        assert args.new_filename.endswith(".pb")
        new_pb = Path(args.new_filename)

    if args.backup_filename is None:
        backup_pb = Path(args.filename.strip(".pb") + "_backup.pb")
    else:
        assert args.backup_filename.endswith(".pb")
        backup_pb = Path(args.backup_filename)

    pb_header = PBUtils(Path(args.filename), chunk_size=0)
    year = pb_header.header.year
    timestamp = args.ts
    assert len(timestamp) <= 6, (
        "Give timestamp in the form 'month day hour minute second nanosecond'. "
        + "Month is required. All must be integers."
    )
    if len(timestamp) == 6:
        nano = timestamp.pop(5)
    else:
        nano = 0
    if len(timestamp) == 1:
        timestamp.append(1)

    diff = datetime(*([year] + timestamp)) - datetime(year, 1, 1)
    seconds = int(diff.total_seconds())
    pb = PBUtils(Path(args.filename))
    pb.write_pb(backup_pb)
    pb.samples = remove_after_ts(pb.samples, seconds, nano=nano)
    pb.write_pb(new_pb)
    if args.write_txt:
        txt_filepath = Path(str(new_pb).strip(".pb") + ".txt")
        pb.write_to_txt(txt_filepath)
