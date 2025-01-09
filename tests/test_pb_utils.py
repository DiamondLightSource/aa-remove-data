import filecmp
from datetime import datetime
from pathlib import Path

import pytest

from aa_remove_data.generated import EPICSEvent_pb2
from aa_remove_data.pb_utils import PBUtils


def test_replace_newline_chars_escape_character():
    pb = PBUtils()
    data = b"fdknbkj\x1bjlgk;fgbbcv\x1bofg"
    expected = b"fdknbkj\x1b\x01jlgk;fgbbcv\x1b\x01ofg"
    result = pb._replace_newline_chars(data)
    assert expected == result


def test_replace_newline_chars_newline():
    pb = PBUtils()
    data = b"fdkndfjkbb\x0ajlgk;fgbdfvdfvbcv\nofgdfv"
    expected = b"fdkndfjkbb\x1b\x02jlgk;fgbdfvdfvbcv\x1b\x02ofgdfv"
    result = pb._replace_newline_chars(data)
    assert expected == result


def test_replace_newline_chars_carriage_return():
    pb = PBUtils()
    data = b"fdknfghdfjk\x0dgk;fgfgfvdfdfvbcv\x0dfgdfv"
    expected = b"fdknfghdfjk\x1b\x03gk;fgfgfvdfdfvbcv\x1b\x03fgdfv"
    result = pb._replace_newline_chars(data)
    assert expected == result


def test_replace_newline_chars_all():
    pb = PBUtils()
    data = (
        b"fasddknf\x1basdozdf\x0ajjkjd\n21asd1/.,gh\\\x1bt:dfasdasffj"
        + b"k\x0dgk;fgf\ngfvdf\x0adfvbcv\x0dfgdfv"
    )
    expected = (
        b"fasddknf\x1b\x01asdozdf\x1b\x02jjkjd\x1b\x0221asd1/.,gh\\\x1b\x01t:"
        + b"dfasdasffjk\x1b\x03gk;fgf\x1b\x02gfvdf\x1b\x02dfvbcv\x1b\x03fgdfv"
    )
    result = pb._replace_newline_chars(data)
    assert expected == result


def test_restore_newline_chars_escape_character():
    pb = PBUtils()
    data = b"fdknbkj\x1b\x01jlgk;fgbbcv\x1b\x01ofg"
    expected = b"fdknbkj\x1bjlgk;fgbbcv\x1bofg"
    result = pb._restore_newline_chars(data)
    assert expected == result


def test_restore_newline_chars_newline():
    pb = PBUtils()
    data = b"fdkndfjkbb\x1b\x02jlgk;fgbdfvdfvbcv\x1b\x02ofgdfv"
    expected = b"fdkndfjkbb\x0ajlgk;fgbdfvdfvbcv\nofgdfv"
    result = pb._restore_newline_chars(data)
    assert expected == result


def test_restore_newline_chars_carriage_return():
    pb = PBUtils()
    data = b"fdknfghdfjk\x1b\x03gk;fgfgfvdfdfvbcv\x1b\x03fgdfv"
    expected = b"fdknfghdfjk\x0dgk;fgfgfvdfdfvbcv\x0dfgdfv"
    result = pb._restore_newline_chars(data)
    assert expected == result


def test_restore_newline_chars_all():
    pb = PBUtils()
    data = (
        b"fasddknf\x1b\x01asdozdf\x1b\x02jjkjd\x1b\x0221asd1/.,gh\\\x1b\x01t:"
        + b"dfasdasffjk\x1b\x03gk;fgf\x1b\x02gfvdf\x1b\x02dfvbcv\x1b\x03fgdfv"
    )
    expected = (
        b"fasddknf\x1basdozdf\x0ajjkjd\n21asd1/.,gh\\\x1bt:dfasdasffj"
        + b"k\x0dgk;fgf\ngfvdf\x0adfvbcv\x0dfgdfv"
    )
    result = pb._restore_newline_chars(data)
    assert expected == result


def test_get_proto_class_name():
    pv_type_to_class_name = {
        "SCALAR_STRING": "ScalarString",
        "SCALAR_SHORT": "ScalarShort",
        "SCALAR_FLOAT": "ScalarFloat",
        "SCALAR_ENUM": "ScalarEnum",
        "SCALAR_BYTE": "ScalarByte",
        "SCALAR_INT": "ScalarInt",
        "SCALAR_DOUBLE": "ScalarDouble",
        "WAVEFORM_STRING": "VectorString",
        "WAVEFORM_SHORT": "VectorShort",
        "WAVEFORM_FLOAT": "VectorFloat",
        "WAVEFORM_ENUM": "VectorEnum",
        "WAVEFORM_BYTE": "VectorByte",
        "WAVEFORM_INT": "VectorInt",
        "WAVEFORM_DOUBLE": "VectorDouble",
        "V4_GENERIC_BYTES": "V4GenericBytes",
    }
    pb = PBUtils()
    for pv_type, expected_class_name in pv_type_to_class_name.items():
        pb.pv_type = pv_type
        result = pb._get_proto_class_name()
        assert result == expected_class_name


def test_convert_to_datetime():
    pb = PBUtils()
    year = 2024
    seconds = 31537000
    expected = datetime(2024, 12, 31, 0, 16, 40)
    result = pb.convert_to_datetime(year, seconds)
    assert result == expected, f"Got {result}, expected {expected}."

    year = 2013
    month = 5
    day = 20
    hour = 19
    minute = 40
    second = 35
    seconds = 86400 * (31 * 2 + 28 + 30 + day - 1) + hour * 3600 + minute * 60 + second
    expected = datetime(year, month, day, hour, minute, second)
    result = pb.convert_to_datetime(year, seconds)
    assert result == expected


def test_convert_to_datetime_too_many_seconds_fails():
    pb = PBUtils()
    with pytest.raises(ValueError):
        pb.convert_to_datetime(2023, 31536000)

    with pytest.raises(ValueError):
        pb.convert_to_datetime(2024, 31622400)  # Leap year


def test_convert_to_datetime_negative_seconds_fails():
    pb = PBUtils()
    with pytest.raises(ValueError):
        pb.convert_to_datetime(2023, -1)

    with pytest.raises(ValueError):
        pb.convert_to_datetime(2024, -100000000)


def test_format_datastr():
    pb = PBUtils()
    sample = EPICSEvent_pb2.ScalarDouble()  # type: ignore
    sample.secondsintoyear = 31535999
    sample.nano = 123456789
    sample.val = 987654321
    expected = "2010-12-31 23:59:59    31535999    123456789    987654321.0\n"
    result = pb.format_datastr(sample, 2010)
    assert result == expected


def test_assign_invalid_pv_type_num():
    pb = PBUtils()
    # Only types 0-14 (inclusive) are valid
    with pytest.raises(ValueError):
        pb.header.type = 15
    with pytest.raises(ValueError):
        pb.header.type = -1


def test_get_pv_type():
    pv_type_enum = {  # Copied from EPICSEvent.proto
        "SCALAR_STRING": 0,
        "SCALAR_SHORT": 1,
        "SCALAR_FLOAT": 2,
        "SCALAR_ENUM": 3,
        "SCALAR_BYTE": 4,
        "SCALAR_INT": 5,
        "SCALAR_DOUBLE": 6,
        "WAVEFORM_STRING": 7,
        "WAVEFORM_SHORT": 8,
        "WAVEFORM_FLOAT": 9,
        "WAVEFORM_ENUM": 10,
        "WAVEFORM_BYTE": 11,
        "WAVEFORM_INT": 12,
        "WAVEFORM_DOUBLE": 13,
        "V4_GENERIC_BYTES": 14,
    }
    pb = PBUtils()
    for expected_pv_type, num in pv_type_enum.items():
        pb.header.type = num
        result = pb.get_pv_type()
        assert result == expected_pv_type


def test_get_proto_class():
    pv_type_num_to_proto_class = {
        0: EPICSEvent_pb2.ScalarString,  # type: ignore
        1: EPICSEvent_pb2.ScalarShort,  # type: ignore
        2: EPICSEvent_pb2.ScalarFloat,  # type: ignore
        3: EPICSEvent_pb2.ScalarEnum,  # type: ignore
        4: EPICSEvent_pb2.ScalarByte,  # type: ignore
        5: EPICSEvent_pb2.ScalarInt,  # type: ignore
        6: EPICSEvent_pb2.ScalarDouble,  # type: ignore
        7: EPICSEvent_pb2.VectorString,  # type: ignore
        8: EPICSEvent_pb2.VectorShort,  # type: ignore
        9: EPICSEvent_pb2.VectorFloat,  # type: ignore
        10: EPICSEvent_pb2.VectorEnum,  # type: ignore
        # 11: EPICSEvent_pb2.VectorByte,  THIS CLASS DOESN'T EXIST!
        # Need to investigate - it's in DBR2PBTypeMapping but not EPICSEvent.proto
        12: EPICSEvent_pb2.VectorInt,  # type: ignore
        13: EPICSEvent_pb2.VectorDouble,  # type: ignore
        14: EPICSEvent_pb2.V4GenericBytes,  # type: ignore
    }
    for pv_type_num, expected_class in pv_type_num_to_proto_class.items():
        pb = PBUtils()
        pb.header.type = pv_type_num
        result = pb.get_proto_class()
        assert result == expected_class


def test_generate_test_samples_gaps():
    pb = PBUtils()
    seconds_gaps = [2, 7, 20, 1, 3214, 0, -7]
    nano_gaps = [500000000, 1, 93184, 0, 999999999, 383838, 5]
    start = 60000
    for i in range(len(seconds_gaps)):
        pb.generate_test_samples(
            pv_type=1,
            samples=10,
            year=2024,
            start=start,
            seconds_gap=seconds_gaps[i],
            nano_gap=nano_gaps[i],
        )
        gap = seconds_gaps[i] * 10**9 + nano_gaps[i]
        for i, sample in enumerate(pb.samples):
            assert sample.secondsintoyear == start + (gap * i) // 10**9
            assert sample.nano == (gap * i) % 10**9


def test_generate_test_samples_number_of_samples():
    pb = PBUtils()
    n_samples = [1, 0, 1000, 50, 13049, 238]
    for n in n_samples:
        pb.generate_test_samples(samples=n)
        assert len(pb.samples) == n


def test_generate_test_samples_all_types():
    pv_type_enum = {  # Copied from EPICSEvent.proto
        "SCALAR_STRING": 0,
        "SCALAR_SHORT": 1,
        "SCALAR_FLOAT": 2,
        "SCALAR_ENUM": 3,
        "SCALAR_BYTE": 4,
        "SCALAR_INT": 5,
        "SCALAR_DOUBLE": 6,
        "WAVEFORM_STRING": 7,
        "WAVEFORM_SHORT": 8,
        "WAVEFORM_FLOAT": 9,
        "WAVEFORM_ENUM": 10,
        # "WAVEFORM_BYTE": 11, proto class doesn't exist - see above
        "WAVEFORM_INT": 12,
        "WAVEFORM_DOUBLE": 13,
        "V4_GENERIC_BYTES": 14,
    }
    pb = PBUtils()
    for pv_type, num in pv_type_enum.items():
        pb.generate_test_samples(pv_type=num)
        assert pb.pv_type == pv_type
        assert pb.samples


def test_write_to_txt():
    pb = PBUtils()
    pb.header.ParseFromString(b"\x08\x06\x12\x04test\x18\xe8\x0f")
    samples_b = [
        b"\x08\x80\xa0\xc0\x0e\x10\x00\x19\x00\x00\x00\x00\x00\x00\x00\x00",
        b"\x08\x99\xa0\xc0\x0e\x10\x80\xca\xb5\xee\x01\x19\x00\x00\x00\x00\x00\x00\xf0?",
        b"\x08\xb3\xa0\xc0\x0e\x10\x00\x19\x00\x00\x00\x00\x00\x00\x00@",
        b"\x08\xcc\xa0\xc0\x0e\x10\x80\xca\xb5\xee\x01\x19\x00\x00\x00\x00\x00\x00\x08@",
        b"\x08\xe6\xa0\xc0\x0e\x10\x00\x19\x00\x00\x00\x00\x00\x00\x10@",
    ]
    proto_class = pb.get_proto_class()
    pb.samples = [proto_class() for _ in range(len(samples_b))]
    for i, sample in enumerate(pb.samples):
        sample.ParseFromString(samples_b[i])

    expected = Path("tests/expected_write_to.txt")
    result = Path("tests/result_write_to.txt")
    pb.write_to_txt(Path("tests/result_write_to.txt"))
    are_identical = filecmp.cmp(expected, result, shallow=False)
    if are_identical is True:
        result.unlink()  # Delete results file if test passes
    assert are_identical is True


def test_read_write_pb():
    read = Path("tests/test_data/P:2021_short.pb")
    write = Path("tests/test_data/P:2021_short_write_result.pb")
    pb = PBUtils(read)
    pb.write_pb(write)
    are_identical = filecmp.cmp(read, write, shallow=False)
    if are_identical is True:
        write.unlink()  # Delete results file if test passes
    assert are_identical is True


# Takes too long but useful to test occasionally as it's unaltered real data.
# def test_read_write_long():
#     read = Path("tests/pb_data/P:2021.pb")
#     write = Path("tests/pb_data/P:2021_write_result.pb")
#     pb = PBUtils(read)
#     pb.write_pb(write)
#     are_identical = filecmp.cmp(read, write, shallow=False)
#     if are_identical is True:
#         write.unlink()  # Delete results file if test passes
#     assert are_identical is True


def test_read_write_pb_all_types():
    pv_types = (
        "SCALAR_BYTE",
        "SCALAR_DOUBLE",
        "SCALAR_ENUM",
        "SCALAR_FLOAT",
        "SCALAR_INT",
        "SCALAR_SHORT",
        "SCALAR_STRING",
        "WAVEFORM_DOUBLE",
        "WAVEFORM_ENUM",
        "WAVEFORM_FLOAT",
        "WAVEFORM_INT",
        "WAVEFORM_SHORT",
        "WAVEFORM_STRING",
        "V4_GENERIC_BYTES",
    )
    for pv_type in pv_types:
        read = Path(f"tests/test_data/{pv_type}_test_data.pb")
        write = Path(f"tests/test_data/result_write_{pv_type}_test_data.pb")
        pb = PBUtils(read)
        assert pb.pv_type == pv_type
        pb.write_pb(write)
        are_identical = filecmp.cmp(read, write, shallow=False)
        if are_identical is True:
            write.unlink()  # Delete results file if test passes
        assert are_identical is True


def test_chunking_read_all_types():
    pv_types = (
        "SCALAR_BYTE",
        "SCALAR_DOUBLE",
        "SCALAR_ENUM",
        "SCALAR_FLOAT",
        "SCALAR_INT",
        "SCALAR_SHORT",
        "SCALAR_STRING",
        "WAVEFORM_DOUBLE",
        "WAVEFORM_ENUM",
        "WAVEFORM_FLOAT",
        "WAVEFORM_INT",
        "WAVEFORM_SHORT",
        "WAVEFORM_STRING",
        "V4_GENERIC_BYTES",
    )
    for pv_type in pv_types:
        read_file = Path(f"tests/test_data/{pv_type}_test_data.pb")
        pb = PBUtils(read_file)
        assert pb.chunked is False
        pb_chunked = PBUtils(chunk_size=7)
        for i, sample in enumerate(pb.samples):
            if i % 7 == 0:
                pb_chunked.read_pb(read_file)
                assert pb_chunked.chunked is True
                # Check exactly 7 samples is read each time, other than the last
                # chunk which will have the remaining samples (< 7).
                assert len(pb_chunked.samples) in (7, len(pb.samples) - i)
            assert pb_chunked.samples[i % 7] == sample
        assert pb_chunked.read_done is True


def test_chunking_read_write_all_types():
    pv_types = (
        "SCALAR_BYTE",
        "SCALAR_DOUBLE",
        "SCALAR_ENUM",
        "SCALAR_FLOAT",
        "SCALAR_INT",
        "SCALAR_SHORT",
        "SCALAR_STRING",
        "WAVEFORM_DOUBLE",
        "WAVEFORM_ENUM",
        "WAVEFORM_FLOAT",
        "WAVEFORM_INT",
        "WAVEFORM_SHORT",
        "WAVEFORM_STRING",
        "V4_GENERIC_BYTES",
    )
    for pv_type in pv_types:
        read_file = Path(f"tests/test_data/{pv_type}_test_data.pb")
        write_file = Path(f"tests/test_data/write_chunked_{pv_type}_test_data.pb")
        write_file2 = Path(f"tests/test_data/write_chunked_{pv_type}_test_data2.pb")
        pb = PBUtils(read_file, chunk_size=13)
        pb.write_pb(write_file)
        pb.write_pb(write_file2)
        assert pb.read_done is False
        while pb.read_done is False:
            pb.read_pb(read_file)
            pb.write_pb(write_file)
            pb.write_pb(write_file2)
        are_identical = filecmp.cmp(read_file, write_file, shallow=False)
        are_identical2 = filecmp.cmp(read_file, write_file2, shallow=False)
        if are_identical is True:
            write_file.unlink()  # Delete results file if test passes
        assert are_identical is True
        if are_identical2 is True:
            write_file2.unlink()  # Delete results file if test passes
        assert are_identical2 is True
