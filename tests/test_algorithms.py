import filecmp
from pathlib import Path

import pytest

import aa_remove_data.algorithms as algorithms
from aa_remove_data.archiver_data import ArchiverData
from aa_remove_data.archiver_data_generated import ArchiverDataGenerated


def test_get_nano_diff():
    adg = ArchiverDataGenerated(start=10, seconds_gap=1, nano_gap=11, samples=5)
    samples = list(adg.get_samples())
    for i in range(1, len(samples)):
        assert algorithms.get_nano_diff(samples[i - 1], samples[i]) == 1000000011

    adg = ArchiverDataGenerated(start=10, seconds_gap=0, nano_gap=294814, samples=5)
    samples = list(adg.get_samples())
    for i in range(1, len(samples)):
        assert algorithms.get_nano_diff(samples[i - 1], samples[i]) == 294814

    adg = ArchiverDataGenerated(
        start=10, seconds_gap=20, nano_gap=999999999, samples=10
    )
    samples = list(adg.get_samples())
    for i in range(1, len(samples)):
        assert algorithms.get_nano_diff(samples[i - 1], samples[i]) == 20999999999


def test_get_seconds_diff():
    adg = ArchiverDataGenerated(start=10, seconds_gap=1, nano_gap=11, samples=5)
    samples = list(adg.get_samples())
    for i in range(1, len(samples)):
        assert algorithms.get_seconds_diff(samples[i - 1], samples[i]) == int(
            1.000000011 * i
        ) - int(1.000000011 * (i - 1))

    adg = ArchiverDataGenerated(
        start=10, seconds_gap=0, nano_gap=294814000, samples=100
    )
    samples = list(adg.get_samples())
    for i in range(1, len(samples)):
        assert algorithms.get_seconds_diff(samples[i - 1], samples[i]) == int(
            0.294814 * i
        ) - int(0.294814 * (i - 1))

    adg = ArchiverDataGenerated(start=10, seconds_gap=20, nano_gap=999999999, samples=5)
    samples = list(adg.get_samples())
    for i in range(1, len(samples)):
        assert algorithms.get_seconds_diff(samples[i - 1], samples[i]) == int(
            20.999999999 * i
        ) - int(20.999999999 * (i - 1))


def test_apply_min_period():
    filepath = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filepath)
    samples = list(algorithms.apply_min_period(ad.get_samples(), period=1))
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
    samples = list(algorithms.apply_min_period(ad.get_samples(), period=0.01))
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
    adg = ArchiverDataGenerated(start=1000, seconds_gap=-1)
    with pytest.raises(ValueError):
        list(algorithms.apply_min_period(adg.get_samples(), period=1))


def test_remove_before_ts():
    filename = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filename)
    samples = list(algorithms.remove_before_ts(ad.get_samples(), 111, nano=650000000))
    all_samples = list(ad.get_samples())
    if samples != list(ad.get_samples())[578:]:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(all_samples[578:])}\n"
            + f"samples[0] = \n{samples[0]}, should be \n{all_samples[578:][0]}\n"
            + f"samples[-1] = \n{samples[-1]}, should be \n{all_samples[578:][-1]}"
        )


def test_remove_before_ts_greater_than_max():
    filename = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filename)
    samples = list(algorithms.remove_before_ts(ad.get_samples(), 200, nano=650000000))
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
    samples = list(algorithms.remove_before_ts(ad.get_samples(), 193, nano=102601528))
    if samples != [list(ad.get_samples())[-1]]:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be 1\n"
            + f"samples[0] = \n{samples[0]}, "
            + f"should be \n{list(ad.get_samples())[-1]}\n"
            + f"samples[-1] = \n{samples[-1]}, "
            + f"should be \n{list(ad.get_samples())[-1]}"
        )


def test_remove_before_ts_lesser_than_min():
    filename = Path("tests/test_data/P:2021_short.pb")
    ad = ArchiverData(filename)
    samples = list(
        algorithms.remove_before_ts(ad.get_samples(), 12743981, nano=650000000)
    )
    all_samples = list(ad.get_samples())
    if samples != list(ad.get_samples()):
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(all_samples)}\n"
            + f"samples[0] = \n{samples[0]}, "
            + f"should be \n{all_samples[0]}\n"
            + f"samples[-1] = \n{samples[-1]}, "
            + f"should be \n{all_samples[-1]}"
        )


def test_remove_before_ts_at_min():
    filename = Path("tests/test_data/P:2021_short.pb")
    ad = ArchiverData(filename)
    samples = list(
        algorithms.remove_before_ts(ad.get_samples(), 12743982, nano=176675494)
    )
    all_samples = list(ad.get_samples())
    if samples != all_samples:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(list(all_samples))}\n"
            + f"samples[0] = \n{samples[0]}, "
            + f"should be \n{list(all_samples)[0]}\n"
            + f"samples[-1] = \n{samples[-1]}, "
            + f"should be \n{list(all_samples)[-1]}"
        )


def test_remove_before_ts_increasing():
    filename = Path("tests/test_data/SCALAR_SHORT_test_data.pb")
    for seconds, nano in zip(
        range(210, 420, 2), range(0, 20 * 10**9, 60000000), strict=False
    ):
        ad = ArchiverData(filename)
        samples = list(
            algorithms.remove_before_ts(ad.get_samples(), seconds, nano=nano)
        )

        if seconds * 10**9 + nano > 398 * 10**9:
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
        range(390, 180, -2), range(0, -(20 * 10**9), -60000000), strict=False
    ):
        ad = ArchiverData(filename)
        samples = list(
            algorithms.remove_before_ts(ad.get_samples(), seconds, nano=nano)
        )

        if seconds * 10**9 + nano < 200 * 10**9:
            assert samples == list(ad.get_samples())
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
    samples = list(algorithms.remove_after_ts(ad.get_samples(), 111, nano=650000000))
    all_samples = list(ad.get_samples())
    if samples != list(ad.get_samples())[:578]:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(all_samples[:578])}\n"
            + f"samples[0] = \n{samples[0]}, should be \n{all_samples[:578][0]}\n"
            + f"samples[-1] = \n{samples[-1]}, should be \n{all_samples[:578][-1]}"
        )


def test_remove_after_ts_greater_than_max():
    filename = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filename)
    samples = list(algorithms.remove_after_ts(ad.get_samples(), 200, nano=650000000))
    all_samples = list(ad.get_samples())
    if samples != all_samples:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(list(all_samples))}\n"
            + f"samples[0] = \n{samples[0]}, "
            + f"should be \n{list(all_samples)[0]}\n"
            + f"samples[-1] = \n{samples[-1]}, "
            + f"should be \n{list(all_samples)[-1]}"
        )


def test_remove_after_ts_at_max():
    filename = Path("tests/test_data/RAW:2025_short.pb")
    ad = ArchiverData(filename)
    samples = list(algorithms.remove_after_ts(ad.get_samples(), 193, nano=102601528))
    all_samples = list(ad.get_samples())
    if samples != all_samples:
        raise AssertionError(
            "Samples don't match:\n"
            + f"len(samples) = {len(samples)}, should be {len(all_samples)}\n"
            + f"samples[0] = \n{samples[0]}, "
            + f"should be \n{list(all_samples)[-1]}\n"
            + f"samples[-1] = \n{samples[-1]}, "
            + f"should be \n{list(all_samples)[-1]}"
        )


def test_remove_after_ts_lesser_than_min():
    filename = Path("tests/test_data/P:2021_short.pb")
    ad = ArchiverData(filename)
    samples = list(
        algorithms.remove_after_ts(ad.get_samples(), 12743981, nano=650000000)
    )
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
    samples = list(
        algorithms.remove_after_ts(ad.get_samples(), 12743982, nano=176675494)
    )
    if samples == []:
        raise AssertionError(
            "Samples don't match: samples != [list(ad.get_samples())[0]]\n"
            + f"Samples = {samples}, should be [{list(ad.get_samples())[0]}]"
        )
    elif samples != [list(ad.get_samples())[0]]:
        raise AssertionError(
            "Samples don't match: samples != [list(ad.get_samples())[0]\n"
            + f"len(samples) = {len(samples)}, should be 1\n"
            + f"samples[0] = {samples[0]}, "
            + f"should be {list(ad.get_samples())[0]}\n"
            + f"samples[-1] = {samples[-1]}, "
            + f"should be {list(ad.get_samples())[-1]}"
        )


def test_remove_after_ts_increasing():
    filename = Path("tests/test_data/SCALAR_SHORT_test_data.pb")
    ad = ArchiverData(filename)
    for seconds, nano in zip(
        range(210, 420, 2), range(0, 20 * 10**9, 60000000), strict=False
    ):
        samples = list(algorithms.remove_after_ts(ad.get_samples(), seconds, nano=nano))

        if seconds * 10**9 + nano >= 398 * 10**9:
            assert samples == list(ad.get_samples())
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
        range(390, 180, -2), range(0, -(20 * 10**9), -60000000), strict=False
    ):
        samples = list(algorithms.remove_after_ts(ad.get_samples(), seconds, nano=nano))

        if seconds * 10**9 + nano < 200 * 10**9:
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
    samples = range(100)
    for n in range(1, 51):
        expected = list(range(0, 100, n))
        actual = list(algorithms.reduce_by_factor(iter(samples), n))
        assert actual == expected


def test_reduce_by_factor_n_too_big():
    samples = range(100)
    n = 2000
    actual = list(algorithms.reduce_by_factor(iter(samples), n))
    assert actual == [list(samples)[0]]


def test_reduce_by_factor_n_is_1():
    samples = range(100)
    n = 1
    actual = list(algorithms.reduce_by_factor(iter(samples), n))
    assert actual == list(samples)


def test_reduce_by_factor_n_is_0():
    samples = iter(range(100))
    n = 0
    with pytest.raises(ZeroDivisionError):
        list(algorithms.reduce_by_factor(samples, n))


def test_reduce_by_factor_n_is_neg():
    samples = range(100)
    n = -5
    with pytest.raises(ValueError):
        list(algorithms.reduce_by_factor(iter(samples), n))


def test_reduce_by_factor_n_is_len():
    samples = range(100)
    n = len(list(samples))
    actual = list(algorithms.reduce_by_factor(iter(samples), n))
    assert actual == [list(samples)[0]]
