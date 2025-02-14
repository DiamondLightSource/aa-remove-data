from collections.abc import Iterator
from itertools import chain
from typing import Any


def apply_min_period(samples: Iterator, period: float) -> Iterator:
    """Reduce the frequency of samples by applying a minimum period.

    Args:
        samples (Iterator): Iterator of samples.
        period (float): Desired minimum period between adjacent samples.

    Raises:
        ValueError: Raised if a period of less than 1 nanosecond is given.

    Yields:
        Iterator: Iterator of reduced list of samples
    """
    seconds_delta = period
    nano_delta = (seconds_delta * 10**9) // 1
    if not nano_delta >= 1:
        raise ValueError(f"Period ({period}) must be at least 1 nanosecond.")

    if seconds_delta >= 5:  # Save time for long periods by ignoring nano
        delta = seconds_delta
        get_diff = get_seconds_diff
    else:
        delta = nano_delta  # For short periods still count nano
        get_diff = get_nano_diff

    first_sample = next(samples)
    yield first_sample
    last_yielded_sample = first_sample
    for sample in samples:
        if get_diff(last_yielded_sample, sample) >= delta:
            last_yielded_sample = sample
            yield sample


def reduce_by_factor(samples: Iterator, factor) -> Iterator:
    """Reduce the number of samples by a certain factor.

    Args:
        samples (Iterator): Iterator of samples.
        factor (int): Factor to reduce the data by.

    Returns:
        Iterator: Iterator of reduced list of samples.
    """
    if factor < 0:
        raise ValueError(f"Factor ({factor}) should be > 0.")
    return (sample for i, sample in enumerate(samples) if i % factor == 0)


def remove_before_ts(samples: Iterator, seconds: int, nano: int = 0) -> Iterator:
    """Remove all samples before a certain timestamp.

    Args:
        samples (Iterator): Iterator of samples.
        seconds (int): Seconds portion of timestamp.
        nano (int, optional): Nanoseconds portion of timestamp. Defaults to 0.

    Returns:
        Iterator: Iterator of reduced list of samples.
    """
    if nano >= 10**9 or nano < 0:
        seconds += nano // (10**9)
        nano = nano % (10**9)
    for sample in samples:
        if not is_before(sample, seconds, nano):
            return chain([sample], samples)
    return iter([])


def remove_after_ts(samples: Iterator, seconds: int, nano: int = 0) -> Iterator:
    """Remove all samples after a certain timestamp.

    Args:
        samples (list): Iterator of samples.
        seconds (int): Seconds portion of timestamp.
        nano (int, optional): Nanoseconds portion of timestamp. Defaults to 0.

    Returns:
        Iterator: Iterator of reduced list of samples.
    """
    if nano >= 10**9 or nano < 0:
        seconds += nano // (10**9)
        nano = nano % (10**9)
    for sample in samples:
        if not is_after(sample, seconds, nano):
            yield sample
        else:
            break


def get_nano_diff(sample1: Any, sample2: Any) -> int:
    """Get the difference in nano seconds between two samples.

    Args:
        sample1 (Any): An Archiver Appliance sample.
        sample2 (Any): Another Archiver Appliance sample.

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


def get_seconds_diff(sample1: Any, sample2: Any) -> int:
    """Get the difference in whole seconds between two samples.

    Args:
        sample1 (Any): An Archiver Appliance sample.
        sample2 (Any): Another Archiver Appliance sample.

    Returns:
        int: Difference in seconds
    """
    diff = sample2.secondsintoyear - sample1.secondsintoyear
    if not diff >= 0:
        raise ValueError(
            f"diff ({diff}) is negative - ensure sample2 comes after sample1."
        )
    return diff


def is_before(sample: Any, seconds: int, nano: int) -> bool:
    """Determine whether a sample's timestamp is before a given timestamp.

    Args:
        sample (Any): Sample being looked at.
        seconds (int): Seconds of target timestamp.
        nano (int): Nanoseconds of a target timestamp.

    Returns:
        bool: True if before, otherwise False
    """
    if sample.secondsintoyear < seconds:
        return True
    elif sample.secondsintoyear == seconds and sample.nano < nano:
        return True
    else:
        return False


def is_after(sample: Any, seconds: int, nano: int) -> bool:
    """Determine whether a sample's timestamp is after a given timestamp.

    Args:
        sample (Any): Sample being looked at.
        seconds (int): Seconds of target timestamp.
        nano (int): Nanoseconds of a target timestamp.

    Returns:
        bool: True if after, otherwise False
    """
    if sample.secondsintoyear > seconds:
        return True
    elif sample.secondsintoyear == seconds and sample.nano > nano:
        return True
    else:
        return False
