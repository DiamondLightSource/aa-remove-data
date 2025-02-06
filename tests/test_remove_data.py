import filecmp
from pathlib import Path

import pytest

import aa_remove_data.remove_data as remove_data
from aa_remove_data.archiver_data import ArchiverData


def test_get_nano_diff():
    ad = ArchiverData()
    ad.generate_test_samples(start=10, seconds_gap=1, nano_gap=11, samples=5)
    for i in range(1, len(ad.samples)):
        assert remove_data.get_nano_diff(ad.samples[i - 1], ad.samples[i]) == 1000000011
    ad.generate_test_samples(start=10, seconds_gap=0, nano_gap=294814, samples=5)
    for i in range(1, len(ad.samples)):
        assert remove_data.get_nano_diff(ad.samples[i - 1], ad.samples[i]) == 294814
    ad.generate_test_samples(start=10, seconds_gap=20, nano_gap=999999999, samples=10)
    for i in range(1, len(ad.samples)):
        assert (
            remove_data.get_nano_diff(ad.samples[i - 1], ad.samples[i]) == 20999999999
        )


def test_get_seconds_diff():
    ad = ArchiverData()
    ad.generate_test_samples(start=10, seconds_gap=1, nano_gap=11, samples=5)
    for i in range(1, len(ad.samples)):
        assert remove_data.get_seconds_diff(ad.samples[i - 1], ad.samples[i]) == int(
            1.000000011 * i
        ) - int(1.000000011 * (i - 1))

    ad.generate_test_samples(start=10, seconds_gap=0, nano_gap=294814000, samples=100)
    for i in range(1, len(ad.samples)):
        assert remove_data.get_seconds_diff(ad.samples[i - 1], ad.samples[i]) == int(
            0.294814 * i
        ) - int(0.294814 * (i - 1))

    ad.generate_test_samples(start=10, seconds_gap=20, nano_gap=999999999, samples=5)
    for i in range(1, len(ad.samples)):
        assert remove_data.get_seconds_diff(ad.samples[i - 1], ad.samples[i]) == int(
            20.999999999 * i
        ) - int(20.999999999 * (i - 1))


def test_apply_min_period():
    filepath = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filepath)
    samples = remove_data.apply_min_period(ad.samples, period=1)
    for i in range(len(samples) - 1):
        diff = (
            samples[i + 1].secondsintoyear
            - samples[i].secondsintoyear
            + samples[i + 1].nano * 10**-9
            - samples[i].nano * 10**-9
        )
        assert diff >= 1


def test_apply_min_period_tiny_period():
    filepath = Path("tests/test_data/RAW:2025_short.pb")
    write_filepath = Path("tests/test_data/RAW:2025_short_test_apply_min_period.pb")
    ad = ArchiverData(filepath)
    samples = remove_data.apply_min_period(ad.samples, period=0.01)
    # Shorter period than any time gap in the file so new file should be identical
    for i in range(len(samples) - 1):
        diff = (
            samples[i + 1].secondsintoyear
            - samples[i].secondsintoyear
            + samples[i + 1].nano * 10**-9
            - samples[i].nano * 10**-9
        )
        assert diff >= 0.01
    ad.write_pb(write_filepath)
    are_identical = filecmp.cmp(filepath, write_filepath, shallow=False)
    assert are_identical is True
    write_filepath.unlink()  # Delete results file if test passes


def test_apply_min_period_gives_neg_diff_error():
    ad = ArchiverData()
    ad.generate_test_samples(start=1000, seconds_gap=-1)
    with pytest.raises(ValueError):
        remove_data.apply_min_period(ad.samples, period=1)


def test_get_index_at_timestamp():
    filename = Path("tests/test_data/SCALAR_BYTE_test_data.pb")
    ad = ArchiverData(filename)
    for i in range(100):
        index, diff = remove_data.get_index_at_timestamp(ad.samples, i + 400)
        assert ad.samples[i].secondsintoyear == i + 400
        assert index == i
        assert diff == 0


def test_get_index_at_timestamp_before_any_samples():
    filename = Path("tests/test_data/P:2021_short.pb")
    ad = ArchiverData(filename)
    index, diff = remove_data.get_index_at_timestamp(ad.samples, 10000000)
    assert index == 0
    assert diff == -(2743982 * 10**9 + 176675494)


def test_get_index_at_timestamp_with_pos_diff():
    filename = Path("tests/test_data/WAVEFORM_FLOAT_test_data.pb")
    ad = ArchiverData(filename)
    for i in range(100):
        index, diff = remove_data.get_index_at_timestamp(
            ad.samples, i + 900, nano=300000000
        )
        assert ad.samples[i].secondsintoyear * 10**9 + diff == (i + 900 + 0.3) * 10**9
        assert diff == 0.3 * 10**9


def test_get_index_at_timestamp_with_neg_diff():
    filename = Path("tests/test_data/WAVEFORM_INT_test_data.pb")
    ad = ArchiverData(filename)
    for i in range(110):
        index, diff = remove_data.get_index_at_timestamp(
            ad.samples, i + 1200, nano=600000000
        )
        if i >= 99:  # Timestamp is after the last sample here
            assert index == 99
            seconds_diff = i - 99 + 0.6
            assert diff == seconds_diff * 10**9
        else:
            assert index == i + 1
            assert diff == -0.4 * 10**9


def test_remove_before_ts():
    filename = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filename)
    samples = remove_data.remove_before_ts(ad.samples, 111, nano=650000000)
    if samples != ad.samples[578:]:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(ad.samples[578:])}\n"
            + f"samples[0] = \n{samples[0]}, should be \n{ad.samples[578:][0]}\n"
            + f"samples[-1] = \n{samples[-1]}, should be \n{ad.samples[578:][-1]}"
        )


def test_remove_before_ts_greater_than_max():
    filename = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filename)
    samples = remove_data.remove_before_ts(ad.samples, 200, nano=650000000)
    if samples != []:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be 0. "
            + "Samples should be an empty list\n"
            + f"samples[0] = \n{samples[0]}, shouldn't exist\n"
            + f"samples[-1] = \n{samples[-1]}, shouldn't exist"
        )


def test_remove_before_ts_at_max():
    filename = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filename)
    samples = remove_data.remove_before_ts(ad.samples, 193, nano=102601528)
    if samples != [ad.samples[-1]]:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be 1\n"
            + f"samples[0] = \n{samples[0]}, "
            + f"should be \n{ad.samples[-1]}\n"
            + f"samples[-1] = \n{samples[-1]}, "
            + f"should be \n{ad.samples[-1]}"
        )


def test_remove_before_ts_lesser_than_min():
    filename = Path("tests/test_data/P:2021_short.pb")
    ad = ArchiverData(filename)
    samples = remove_data.remove_before_ts(ad.samples, 12743981, nano=650000000)
    if samples != ad.samples:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(ad.samples)}\n"
            + f"samples[0] = \n{samples[0]}, "
            + f"should be \n{ad.samples[0]}\n"
            + f"samples[-1] = \n{samples[-1]}, "
            + f"should be \n{ad.samples[-1]}"
        )


def test_remove_before_ts_at_min():
    filename = Path("tests/test_data/P:2021_short.pb")
    ad = ArchiverData(filename)
    samples = remove_data.remove_before_ts(ad.samples, 12743982, nano=176675494)
    if samples != ad.samples:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(ad.samples)}\n"
            + f"samples[0] = \n{samples[0]}, "
            + f"should be \n{ad.samples[0]}\n"
            + f"samples[-1] = \n{samples[-1]}, "
            + f"should be \n{ad.samples[-1]}"
        )


def test_remove_before_ts_increasing():
    filename = Path("tests/test_data/SCALAR_SHORT_test_data.pb")
    ad = ArchiverData(filename)
    for seconds, nano in zip(
        range(105, 210), range(0, 10 * 10**9, 30000000), strict=False
    ):
        samples = remove_data.remove_before_ts(ad.samples, seconds, nano=nano)

        if seconds * 10**9 + nano > 199 * 10**9:
            assert samples == []
        else:
            expected_min_nanoseconds = seconds * 10**9 + nano
            actual_lowest_nanoseconds = (
                samples[0].secondsintoyear * 10**9 + samples[0].nano
            )
            assert actual_lowest_nanoseconds >= expected_min_nanoseconds
            if nano == 0:
                assert actual_lowest_nanoseconds == seconds * 10**9


def test_remove_before_ts_decreasing():
    filename = Path("tests/test_data/SCALAR_SHORT_test_data.pb")
    ad = ArchiverData(filename)
    for seconds, nano in zip(
        range(195, 90, -1), range(0, -(10 * 10**9), -30000000), strict=False
    ):
        samples = remove_data.remove_before_ts(ad.samples, seconds, nano=nano)

        if seconds * 10**9 + nano < 100 * 10**9:
            assert samples == ad.samples
        else:
            expected_min_nanoseconds = seconds * 10**9 + nano
            actual_lowest_nanoseconds = (
                samples[0].secondsintoyear * 10**9 + samples[0].nano
            )
            assert actual_lowest_nanoseconds >= expected_min_nanoseconds
            if nano == 0:
                assert actual_lowest_nanoseconds == seconds * 10**9


def test_remove_after_ts():
    filename = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filename)
    samples = remove_data.remove_after_ts(ad.samples, 111, nano=650000000)
    if samples != ad.samples[:578]:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(ad.samples[:578])}\n"
            + f"samples[0] = \n{samples[0]}, should be \n{ad.samples[:578][0]}\n"
            + f"samples[-1] = \n{samples[-1]}, should be \n{ad.samples[:578][-1]}"
        )


def test_remove_after_ts_greater_than_max():
    filename = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filename)
    samples = remove_data.remove_after_ts(ad.samples, 200, nano=650000000)
    if samples != ad.samples:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(ad.samples)}\n"
            + f"samples[0] = \n{samples[0]}, "
            + f"should be \n{ad.samples[0]}\n"
            + f"samples[-1] = \n{samples[-1]}, "
            + f"should be \n{ad.samples[-1]}"
        )


def test_remove_after_ts_at_max():
    filename = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filename)
    samples = remove_data.remove_after_ts(ad.samples, 193, nano=102601528)
    if samples != ad.samples:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(ad.samples)}\n"
            + f"samples[0] = \n{samples[0]}, "
            + f"should be \n{ad.samples[-1]}\n"
            + f"samples[-1] = \n{samples[-1]}, "
            + f"should be \n{ad.samples[-1]}"
        )


def test_remove_after_ts_lesser_than_min():
    filename = Path("tests/test_data/P:2021_short.pb")
    ad = ArchiverData(filename)
    samples = remove_data.remove_after_ts(ad.samples, 12743981, nano=650000000)
    if samples != []:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be 0. "
            + "Samples should be an empty list\n"
            + f"samples[0] = {samples[0]}, shouldn't exist\n"
            + f"samples[-1] = {samples[-1]}, shouldn't exist"
        )


def test_remove_after_ts_at_min():
    filename = Path("tests/test_data/P:2021_short.pb")
    ad = ArchiverData(filename)
    samples = remove_data.remove_after_ts(ad.samples, 12743982, nano=176675494)
    if samples == []:
        raise AssertionError(
            "Samples don't match: samples != [ad.samples[0]]\n"
            + f"Samples = {samples}, should be [{ad.samples[0]}]"
        )
    elif samples != [ad.samples[0]]:
        raise AssertionError(
            "Samples don't match: samples != [ad.samples[0]\n"
            + f"len(samples) = {len(samples)}, should be 1\n"
            + f"samples[0] = {samples[0]}, "
            + f"should be {ad.samples[0]}\n"
            + f"samples[-1] = {samples[-1]}, "
            + f"should be {ad.samples[-1]}"
        )


def test_remove_after_ts_increasing():
    filename = Path("tests/test_data/SCALAR_SHORT_test_data.pb")
    ad = ArchiverData(filename)
    for seconds, nano in zip(
        range(105, 210), range(0, 10 * 10**9, 30000000), strict=False
    ):
        samples = remove_data.remove_after_ts(ad.samples, seconds, nano=nano)

        if seconds * 10**9 + nano >= 199 * 10**9:
            assert samples == ad.samples
        else:
            expected_max_nanoseconds = seconds * 10**9 + nano
            actual_highest_nanoseconds = (
                samples[-1].secondsintoyear * 10**9 + samples[-1].nano
            )
            assert actual_highest_nanoseconds <= expected_max_nanoseconds
            if nano == 0:
                assert actual_highest_nanoseconds == seconds * 10**9


def test_remove_after_ts_decreasing():
    filename = Path("tests/test_data/SCALAR_SHORT_test_data.pb")
    ad = ArchiverData(filename)
    for seconds, nano in zip(
        range(195, 90, -1), range(0, -(10 * 10**9), -30000000), strict=False
    ):
        samples = remove_data.remove_after_ts(ad.samples, seconds, nano=nano)

        if seconds * 10**9 + nano < 100 * 10**9:
            assert samples == []
        else:
            expected_max_nanoseconds = seconds * 10**9 + nano
            actual_highest_nanoseconds = (
                samples[-1].secondsintoyear * 10**9 + samples[-1].nano
            )
            assert actual_highest_nanoseconds <= expected_max_nanoseconds
            if nano == 0:
                assert actual_highest_nanoseconds == seconds * 10**9


def test_reduce_by_factor():
    samples = list(range(1000))
    for n in range(1, 51):
        expected = list(range(0, 1000, n))
        actual = remove_data.reduce_by_factor(samples, n)
        assert actual == expected


def test_reduce_by_factor_n_too_big():
    samples = list(range(1000))
    n = 2000
    actual = remove_data.reduce_by_factor(samples, n)
    assert actual == [samples[0]]


def test_reduce_by_factor_n_is_1():
    samples = list(range(1000))
    n = 1
    actual = remove_data.reduce_by_factor(samples, n)
    assert actual == samples


def test_reduce_by_factor_n_is_0():
    samples = list(range(1000))
    n = 0
    with pytest.raises(ValueError):
        remove_data.reduce_by_factor(samples, n)


def test_reduce_by_factor_n_is_neg():
    samples = list(range(1000))
    n = -5
    with pytest.raises(ValueError):
        remove_data.reduce_by_factor(samples, n)


def test_reduce_by_factor_n_is_len():
    samples = list(range(1000))
    n = len(samples)
    actual = remove_data.reduce_by_factor(samples, n)
    assert actual == [samples[0]]
