import filecmp
from pathlib import Path

import pytest

import aa_remove_data.remove_data as remove_data
from aa_remove_data.pb_utils import PBUtils


def test_get_nano_diff():
    pb = PBUtils()
    pb.generate_test_samples(start=10, seconds_gap=1, nano_gap=11, samples=5)
    for i in range(1, len(pb.samples)):
        assert remove_data.get_nano_diff(pb.samples[i - 1], pb.samples[i]) == 1000000011
    pb.generate_test_samples(start=10, seconds_gap=0, nano_gap=294814, samples=5)
    for i in range(1, len(pb.samples)):
        assert remove_data.get_nano_diff(pb.samples[i - 1], pb.samples[i]) == 294814
    pb.generate_test_samples(start=10, seconds_gap=20, nano_gap=999999999, samples=10)
    for i in range(1, len(pb.samples)):
        assert (
            remove_data.get_nano_diff(pb.samples[i - 1], pb.samples[i]) == 20999999999
        )


def test_get_seconds_diff():
    pb = PBUtils()
    pb.generate_test_samples(start=10, seconds_gap=1, nano_gap=11, samples=5)
    for i in range(1, len(pb.samples)):
        assert remove_data.get_seconds_diff(pb.samples[i - 1], pb.samples[i]) == int(
            1.000000011 * i
        ) - int(1.000000011 * (i - 1))

    pb.generate_test_samples(start=10, seconds_gap=0, nano_gap=294814000, samples=100)
    for i in range(1, len(pb.samples)):
        assert remove_data.get_seconds_diff(pb.samples[i - 1], pb.samples[i]) == int(
            0.294814 * i
        ) - int(0.294814 * (i - 1))

    pb.generate_test_samples(start=10, seconds_gap=20, nano_gap=999999999, samples=5)
    for i in range(1, len(pb.samples)):
        assert remove_data.get_seconds_diff(pb.samples[i - 1], pb.samples[i]) == int(
            20.999999999 * i
        ) - int(20.999999999 * (i - 1))


def test_reduce_freq():
    filepath = Path("tests/test_data/RAW:2025_short.pb")
    pb = PBUtils(filepath)
    samples = remove_data.reduce_freq(pb.samples, period=1)
    for i in range(len(samples) - 1):
        diff = (
            samples[i + 1].secondsintoyear
            - samples[i].secondsintoyear
            + samples[i + 1].nano * 10**-9
            - samples[i].nano * 10**-9
        )
        assert diff >= 1


def test_reduce_freq_tiny_period():
    filepath = Path("tests/test_data/RAW:2025_short.pb")
    write_filepath = Path("tests/test_data/RAW:2025_short_reduce_freq_test.pb")
    pb = PBUtils(filepath)
    samples = remove_data.reduce_freq(pb.samples, period=0.01)
    # Shorter period than any time gap in the file so new file should be identical
    for i in range(len(samples) - 1):
        diff = (
            samples[i + 1].secondsintoyear
            - samples[i].secondsintoyear
            + samples[i + 1].nano * 10**-9
            - samples[i].nano * 10**-9
        )
        assert diff >= 0.01
    pb.write_pb(write_filepath)
    are_identical = filecmp.cmp(filepath, write_filepath, shallow=False)
    assert are_identical is True
    write_filepath.unlink()  # Delete results file if test passes


def test_reduce_freq_gives_neg_diff_error():
    pb = PBUtils()
    pb.generate_test_samples(start=1000, seconds_gap=-1)
    with pytest.raises(AssertionError):
        remove_data.reduce_freq(pb.samples, freq=1)


def test_get_index_at_timestamp():
    filename = Path("tests/test_data/SCALAR_BYTE_test_data.pb")
    pb = PBUtils(filename)
    for i in range(100):
        index, diff = remove_data.get_index_at_timestamp(pb.samples, i + 400)
        assert pb.samples[i].secondsintoyear == i + 400
        assert index == i
        assert diff == 0


def test_get_index_at_timestamp_before_any_samples():
    filename = Path("tests/test_data/SCALAR_BYTE_test_data.pb")
    pb = PBUtils(filename)
    index, diff = remove_data.get_index_at_timestamp(pb.samples, 100)
    assert index == 0
    assert diff == -300 * 10**9


def test_get_index_at_timestamp_with_pos_diff():
    filename = Path("tests/test_data/WAVEFORM_FLOAT_test_data.pb")
    pb = PBUtils(filename)
    for i in range(100):
        index, diff = remove_data.get_index_at_timestamp(
            pb.samples, i + 900, nano=300000000
        )
        assert pb.samples[i].secondsintoyear * 10**9 + diff == (i + 900 + 0.3) * 10**9
        assert diff == 0.3 * 10**9


def test_get_index_at_timestamp_with_neg_diff():
    filename = Path("tests/test_data/WAVEFORM_INT_test_data.pb")
    pb = PBUtils(filename)
    for i in range(100):
        index, diff = remove_data.get_index_at_timestamp(
            pb.samples, i + 1200, nano=600000000
        )
        if i == 99:  # Timestamp is after the last sample here
            assert index == i
            assert diff == 0.6 * 10**9
        else:
            assert index == i + 1
            assert diff == -0.4 * 10**9
