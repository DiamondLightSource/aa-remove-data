import argparse
import subprocess
from collections.abc import Callable
from datetime import datetime, timedelta
from itertools import islice
from os import PathLike
from pathlib import Path

from aa_remove_data.generated import EPICSEvent_pb2

DEFAULT_CHUNK_SIZE = 10000000


class PBUtils:
    def __init__(self, filepath: PathLike | None = None, chunk_size=DEFAULT_CHUNK_SIZE):
        """Initialise a PBUtils object. If filepath is set, read the protobuf
        file at this location to gether its header, samples and type.

        Args:
            filepath (Optional[PathLike], optional): Path to PB file to be
            read. Defaults to None.
            chunk_size (Optional[int], optional): Number of lines to read/write
            at one time.
        """
        self.header = EPICSEvent_pb2.PayloadInfo()  # type: ignore
        self.samples = []
        self.pv_type = ""
        self.data_chunked = False
        self.read_done = False
        self._chunk_size = chunk_size
        self._start_line = 0
        self._write_started = []
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
        """Convert the name of a pv type to match the proto class name. This
        involves coverting to CamelCase and replacing 'WAVEFORM' with 'Vector'.
        The full mapping is described in
        epicsarchiverap/src/main/edu/stanford/slac/archiverappliance/PB/data/DBR2PBTypeMapping.java

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

    def generate_test_samples(
        self,
        pv_type: int = 6,
        samples: int = 100,
        year: int = 2024,
        start: int = 0,
        seconds_gap: int = 1,
        nano_gap: int = 0,
    ):
        """Generate test Archiver Appliance samples.

        Args:
            pv_type (int, optional): PV type enum. Defaults to 6 (SCALAR_DOUBLE).
            samples (int, optional): Number of samples to be generated.
            Defaults to 100.
            year (int, optional): Year associated with samples. Defaults to 2024.
            seconds_gap (int, optional): Gap in seconds between samples.
            start (int, optional): Initial number of seconds for first sample.
            Defaults to 1.
            nano_gap (int, optional): Gap in nanoseconds between samples.
            Defaults to 0.
        """
        self.header.pvname = "test"
        self.header.year = year
        self.header.type = pv_type

        sample_class = self.get_proto_class()
        self.samples = [sample_class() for _ in range(samples)]
        time_gap = seconds_gap * 10**9 + nano_gap
        time = start * 10**9

        for i, sample in enumerate(self.samples):
            sample.secondsintoyear = time // 10**9
            sample.nano = time % 10**9
            sample.val = i
            time += time_gap

    def write_to_txt(self, filepath: PathLike):
        """Write a text file from a PBUtils object.

        Args:
            filepath (PathLike): Filepath for file to be written.
        """
        pvname = self.header.pvname
        year = self.header.year
        data_strs = [self.format_datastr(sample, year) for sample in self.samples]
        if filepath not in self._write_started or self.data_chunked is False:
            with open(filepath, "w") as f:
                # Write header
                f.write(f"{pvname}, {self.pv_type}, {year}\n")
                # Write column titles
                f.write(f"DATE{' ' * 19}SECONDS{' ' * 5}NANO{' ' * 9}VAL\n")
                # Write body
                f.writelines(data_strs)
                self._write_started.append(filepath)
        else:
            with open(filepath, "a") as f:
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
            if self._start_line == 0:
                # Read header
                result = subprocess.run(
                    ["wc", "-l", filepath], stdout=subprocess.PIPE, text=True
                )
                self._total_lines = int(result.stdout.split()[0])
                first_line = self._restore_newline_chars(f.readline().strip())
                self.header.ParseFromString(first_line)
                f.seek(0)
                self._start_line += 1

            # Extract samples from file
            end_line = min(self._start_line + self._chunk_size, self._total_lines)
            lines = list(islice(f, self._start_line, end_line))

        if self._total_lines == end_line:
            self.read_done = True
        else:
            self.data_chunked = True
        self._start_line = end_line
        proto_class = self.get_proto_class()
        self.samples = [proto_class() for _ in range(len(lines))]
        for i, sample in enumerate(self.samples):
            line = self._restore_newline_chars(lines[i].strip())
            sample.ParseFromString(line)

    def write_pb(self, filepath: PathLike):
        """Write a pb file that is structured in the Archiver Appliance format.
        Must have a valid header and list of samples to write.

        Args:
            filepath (PathLike): Path to file to be written.
        """
        samples_bytes = [
            self._replace_newline_chars(sample.SerializeToString()) + b"\n"
            for sample in self.samples
        ]
        if filepath not in self._write_started or self.data_chunked is False:
            # Write header + samples, start new file
            header_bytes = (
                self._replace_newline_chars(self.header.SerializeToString()) + b"\n"
            )
            with open(filepath, "wb") as f:
                f.writelines([header_bytes] + samples_bytes)
            self._write_started.append(filepath)
        else:
            # Add samples to existing file
            with open(filepath, "ab") as f:
                f.writelines(samples_bytes)


def pb_2_txt():
    """Convert a .pb file to a human-readable .txt file."""
    parser = argparse.ArgumentParser()
    parser.add_argument("pb_filename", type=str)
    parser.add_argument("txt_filename", type=str)
    parser.add_argument("--chunk", type=int, default=DEFAULT_CHUNK_SIZE)
    args = parser.parse_args()
    pb_file = Path(args.pb_filename)
    txt_file = Path(args.txt_filename)
    if not pb_file.is_file():
        raise FileNotFoundError(f"The file {pb_file} does not exist.")
    if pb_file.suffix != ".pb":
        raise ValueError(f"Invalid file extension: '{pb_file.suffix}'. Expected '.pb'.")
    pb = PBUtils(chunk_size=args.chunk)
    while pb.read_done is False:
        pb.read_pb(pb_file)
        pb.write_to_txt(txt_file)
    print("Write completed!")


def print_header():
    """Print the header and first few lines of a .pb file."""
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
    pb = PBUtils(pb_file, chunk_size=lines)
    pvname = pb.header.pvname
    year = pb.header.year
    print(f"Name: {pvname}, Type: {pb.pv_type}, Year: {year}")
    if pb.samples:
        print(f"DATE{' ' * 19}SECONDS{' ' * 5}NANO{' ' * 9}VAL")
        for sample in pb.samples:
            print(pb.format_datastr(sample, year).strip())
