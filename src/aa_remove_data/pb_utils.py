import argparse
from collections.abc import Callable
from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path

from aa_remove_data.generated import EPICSEvent_pb2


class PBUtils:
    def __init__(self, filepath: PathLike | None = None):
        """Initialise a PBUtils object. If filepath is set, read the protobuf
        file at this location to gether its header, samples and type.

        Args:
            filepath (Optional[PathLike], optional): Path to PB file to be
            read. Defaults to None.
        """
        self.header = EPICSEvent_pb2.PayloadInfo()  # type: ignore
        self.samples = []
        self.pv_type = ""
        if filepath:
            self.read_pb(filepath)

    def _replace_newline_chars(self, data: bytes) -> bytes:
        """Replace newline characters with alternative to conform with the
        archiver PB format. See https://epicsarchiver.readthedocs.io.
        Args:
            data (bytes): A serialised protobuf sample.

        Returns:
            bytes: The serialised sample with escape characters replaced.
        """
        data = data.replace(b"\x1b", b"\x1b\x01")  # Escape escape character
        data = data.replace(b"\x0a", b"\x1b\x02")  # Escape newline
        data = data.replace(b"\x0d", b"\x1b\x03")  # Escape carriage return
        return data

    def _restore_newline_chars(self, data: bytes) -> bytes:
        """Restore newline characters that have been replaced by the archiver
        PB format. See https://epicsarchiver.readthedocs.io.
        Args:
            data (bytes): A serialised protobuf message with escape characters
            replaced.

        Returns:
            bytes: The serialised protobuf message containing escape
            characters.
        """
        data = data.replace(b"\x1b\x03", b"\x0d")  # Unescape carriage return
        data = data.replace(b"\x1b\x02", b"\x0a")  # Unescape newline
        data = data.replace(b"\x1b\x01", b"\x1b")  # Unescape escape character
        return data

    def _get_proto_class_name(self) -> str:
        """Convert the name of a pv type to CamelCase to match the proto class
        name.

        Returns:
            str: Name of proto class, e.g VectorDouble.
        """
        # Split the enum name by underscores and capitalize each part
        parts = self.pv_type.split("_")
        return "".join(part.capitalize() for part in parts)

    def convert_to_datetime(self, year: int, seconds: int) -> datetime:
        """Get the date and time from a year and the number of seconds passed.
        Args:
            year (int): A year
            seconds (int): The number of seconds into that year that have
            passed.
        Returns:
            datetime: A datetime object of the correct date and time.
        """
        return datetime(year, 1, 1) + timedelta(seconds=seconds)

    def format_datastr(self, sample: type, year: int) -> str:
        """Get a string containing information about a sample.
        Args:
            sample (type): A sample from a PB file.
            year (int): The year the sample was collected.
        Returns:
            str: A string containing the sample information.
        """
        date = self.convert_to_datetime(year, sample.secondsintoyear)
        return (
            f"{date}    {sample.secondsintoyear:8d}    {sample.nano:9d}"
            f"    {sample.val}\n"
        )

    def get_pv_type(self) -> str:
        """Get the name of a PB file's pv type using information in its
        header.

        Returns:
            str: Name of pv type, e.g VECTOR_DOUBLE.
        """
        type_descriptor = self.header.DESCRIPTOR.fields_by_name["type"]
        enum_descriptor = type_descriptor.enum_type
        return enum_descriptor.values_by_number[self.header.type].name

    def get_proto_class(self) -> Callable:
        """Get the EPICSEvent_pb2 class corresponding to the pv in a PB file.
        Instances of this class can interpret PB messages of a matching type.

        Returns:
            Callable: EPICSEvent_pb2 protocol buffer class.
        """
        # Ensure self.pv_type is set first.
        if not self.pv_type:
            self.pv_type = self.get_pv_type()
        pv_type_camel = self._get_proto_class_name()
        proto_class = getattr(EPICSEvent_pb2, pv_type_camel)
        return proto_class

    def write_to_txt(self, filepath: PathLike):
        """Write a text file from a PBUtils object.

        Args:
            filepath (PathLike): Filepath for file to be written.
        """
        pvname = self.header.pvname
        year = self.header.year
        data_strs = [self.format_datastr(sample, year) for sample in self.samples]
        with open(filepath, "w") as f:
            # Write header
            f.write(f"{pvname}, {self.pv_type}, {year}\n")
            # Write column titles
            f.write(f"DATE{' ' * 19}SECONDS{' ' * 5}NANO{' ' * 9}VAL\n")
            # Write body
            f.writelines(data_strs)

    def read_pb(self, filepath: PathLike):
        """Read a PB file that is structured in the Archiver Appliance format.
        Gathers the header and samples from this file and assigns them to
        self.header self.samples.

        Args:
            filepath (PathLike): Path to PB file.
        """
        with open(filepath, "rb") as f:
            first_line = self._restore_newline_chars(f.readline().strip())
            lines = f.readlines()
        self.header.ParseFromString(first_line)
        proto_class = self.get_proto_class()
        self.samples = [proto_class() for _ in range(len(lines))]
        for i, sample in enumerate(self.samples):
            line = self._restore_newline_chars(lines[i].strip())
            sample.ParseFromString(line)

    def write_pb(self, filepath: PathLike):
        """Write a PB file that is structured in the Archiver Appliance format.
        Must have a valid header and list of samples to write.

        Args:
            filepath (PathLike): Path to file to be written.
        """
        header_b = self._replace_newline_chars(self.header.SerializeToString()) + b"\n"
        samples_b = [
            self._replace_newline_chars(sample.SerializeToString()) + b"\n"
            for sample in self.samples
        ]
        with open(filepath, "wb") as f:
            f.writelines([header_b] + samples_b)


def pb_2_txt():
    """Convert a .pb file to a human-readable .txt file"""
    parser = argparse.ArgumentParser()
    parser.add_argument("pb_filename", type=str)
    parser.add_argument("txt_filename", type=str)
    args = parser.parse_args()
    pb_file = Path(args.pb_filename)
    txt_file = Path(args.txt_filename)
    if not pb_file.is_file():
        raise FileNotFoundError(f"The file {pb_file} does not exist.")
    if pb_file.suffix != ".pb":
        raise ValueError(f"Invalid file extension: '{pb_file.suffix}'. Expected '.pb'.")
    pb = PBUtils(pb_file)
    pb.write_to_txt(txt_file)


def print_header():
    parser = argparse.ArgumentParser()
    parser.add_argument("pb_filename", type=str)
    parser.add_argument("--lines", type=int, default=0)
    args = parser.parse_args()
    pb_file = Path(args.pb_filename)
    lines = args.lines
    if not pb_file.is_file():
        raise FileNotFoundError(f"The file {pb_file} does not exist.")
    if pb_file.suffix != ".pb":
        raise ValueError(f"Invalid file extension: '{pb_file.suffix}'. Expected '.pb'.")
    if lines < 0:
        raise ValueError(f"Cannot have a negative number of lines ({lines}).")
    pb = PBUtils(pb_file)
    pvname = pb.header.pvname
    year = pb.header.year
    print(f"Name: {pvname}, Type: {pb.pv_type}, Year: {year}")
    if lines > 0:
        print(f"DATE{' ' * 19}SECONDS{' ' * 5}NANO{' ' * 9}VAL")
        for i in range(lines):
            print(pb.format_datastr(pb.samples[i], year).strip())
