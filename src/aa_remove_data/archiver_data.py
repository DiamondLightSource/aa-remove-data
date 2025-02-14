import subprocess
from collections.abc import Callable, Generator, Iterator
from datetime import datetime, timedelta
from itertools import islice
from os import PathLike
from pathlib import Path
from typing import Any

from tqdm import tqdm

from aa_remove_data.generated import EPICSEvent_pb2


class ArchiverData:
    def __init__(self, filepath: PathLike):
        """Initialise a ArchiverData object. If filepath is set, read the protobuf
        file at this location to gather its header, samples and type.

        Args:
            filepath (Optional[PathLike], optional): Path to PB file to be
            read. Defaults to None.
            chunk_size (Optional[int], optional): Number of lines to read/write
            at one time.
        """
        self.filepath = Path(filepath)
        with open(filepath, "rb") as f:
            self.header = self.deserialize(f.readline(), EPICSEvent_pb2.PayloadInfo)  # type: ignore
        self.pv_type = self._get_pv_type()
        self.proto_class = self._get_proto_class()

    def get_samples(self) -> Generator:
        """Read a PB file that is structured in the Archiver Appliance format.
        Gathers the header and samples from this file and assigns them to
        self.header self.samples.

        Args:
            filepath (PathLike): Path to PB file.
        """
        with open(self.filepath, "rb") as f:
            for line in islice(f, 1, None):
                yield self.deserialize(line, self.proto_class)

    def get_samples_bytes(self) -> Generator:
        """Read a PB file that is structured in the Archiver Appliance format.
        Gathers the header and samples from this file and assigns them to
        self.header self.samples.

        Args:
            filepath (PathLike): Path to PB file.
        """
        with open(self.filepath, "rb") as f:
            yield from islice(f, 1, None)

    def get_processed_samples(
        self,
        process_func: Callable,
        process_args: list | None = None,
        process_kwargs: dict | None = None,
        raw: bool = False,
    ):
        process_args = process_args or []
        process_kwargs = process_kwargs or {}
        samples = self.get_samples_bytes() if raw else self.get_samples()
        yield from process_func(samples, *process_args, **process_kwargs)

    def process_and_write(
        self,
        filepath: PathLike,
        write_txt: bool,
        process_func: Callable,
        process_args: list | None = None,
        process_kwargs: dict | None = None,
        raw: bool = False,
    ):
        filepath = Path(filepath)
        txt_filepath = filepath.with_suffix(".txt")

        mv_to = ""
        if filepath == self.filepath:
            mv_to = filepath
            filepath = self.get_temp_filename(filepath)

        samples = self.get_processed_samples(
            process_func,
            process_args=process_args,
            process_kwargs=process_kwargs,
            raw=raw,
        )
        if write_txt:
            self.write_pb_and_txt(filepath, txt_filepath, samples, raw=raw)
        else:
            self.write_pb(filepath, samples=samples, raw=raw)
        if mv_to:
            print(f"Moving {filepath} to {mv_to}")
            subprocess.run(["mv", filepath, mv_to], check=True)

    def write_pb_and_txt(
        self,
        pb_filepath: PathLike,
        txt_filepath: PathLike,
        samples: Iterator,
        raw=False,
    ):
        print(f"Writing {pb_filepath}")
        print(f"Writing {txt_filepath}")
        year = self.header.year
        with open(pb_filepath, "wb") as f_pb, open(txt_filepath, "w") as f_txt:
            # Write header
            f_pb.write(self.serialize(self.header))
            f_txt.write(f"{self.header.pvname}, {self.pv_type}, {self.header.year}\n")
            f_txt.write(f"DATE{' ' * 19}SECONDS{' ' * 5}NANO{' ' * 9}VAL\n")
            if raw:
                for sample in tqdm(samples, mininterval=0.1):
                    f_pb.write(sample)
                    f_txt.write(
                        self.format_datastr(
                            self.deserialize(sample, self.proto_class), year
                        )
                    )
            else:
                for sample in tqdm(samples, mininterval=0.1):
                    f_pb.write(self.serialize(sample))
                    f_txt.write(self.format_datastr(sample, year))

    def write_pb(self, filepath: PathLike, samples: Iterator | None = None, raw=True):
        print(f"Writing {filepath}")
        samples = samples or self.get_samples_bytes()
        with open(filepath, "wb") as f:
            f.write(self.serialize(self.header))
            f.writelines(
                tqdm(samples, mininterval=0.1)
                if raw
                else (
                    self.serialize(sample) for sample in tqdm(samples, mininterval=0.1)
                )
            )

    def write_txt(self, filepath: PathLike, samples: Iterator | None = None):
        print(f"Writing {filepath}")
        samples = samples or self.get_samples()
        with open(filepath, "w") as f:
            # Write header
            f.write(f"{self.header.pvname}, {self.pv_type}, {self.header.year}\n")
            # Write column titles
            f.write(f"DATE{' ' * 19}SECONDS{' ' * 5}NANO{' ' * 9}VAL\n")
            # Write samples
            f.writelines(
                self.format_datastr(sample, self.header.year) for sample in samples
            )

    @staticmethod
    def serialize(sample: Any) -> bytes:
        return ArchiverData._replace_newline_chars(sample.SerializeToString()) + b"\n"

    @staticmethod
    def deserialize(line: bytes, proto_class: Callable) -> Any:
        sample_bytes = ArchiverData._restore_newline_chars(line.rstrip(b"\n"))
        sample = proto_class()
        sample.ParseFromString(sample_bytes)
        return sample

    @staticmethod
    def _replace_newline_chars(data: bytes) -> bytes:
        """Replace newline characters with alternative to conform with the
        archiver PB format. See https://epicsarchiver.readthedocs.io/en/latest/developer/pb_pbraw.html.
        Args:
            data (bytes): A serialised protobuf sample.

        Returns:
            bytes: The serialised sample with escape characters replaced.
        """
        data = data.replace(b"\x1b", b"\x1b\x01")  # Escape escape character
        data = data.replace(b"\x0a", b"\x1b\x02")  # Escape newline
        data = data.replace(b"\x0d", b"\x1b\x03")  # Escape carriage return
        return data

    @staticmethod
    def _restore_newline_chars(data: bytes) -> bytes:
        """Restore newline characters that have been replaced by the archiver
        PB format. See https://epicsarchiver.readthedocs.io/en/latest/developer/pb_pbraw.html.
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

    @staticmethod
    def convert_to_datetime(year: int, seconds: int) -> datetime:
        """Get the date and time from a year and the number of seconds passed.
        Args:
            year (int): A year
            seconds (int): The number of seconds into that year that have
            passed.
        Returns:
            datetime: A datetime object of the correct date and time.
        """
        ts = datetime(year, 1, 1) + timedelta(seconds=seconds)
        if ts.year != year:
            raise ValueError
        return ts

    @staticmethod
    def format_datastr(sample: Any, year: int) -> str:
        """Get a string containing information about a sample.
        Args:
            sample (type): A sample from a PB file.
        Returns:
            str: A string containing the sample information.
        """
        date = ArchiverData.convert_to_datetime(year, sample.secondsintoyear)
        return (
            f"{date}    {sample.secondsintoyear:8d}    {sample.nano:9d}"
            f"    {sample.val}\n"
        )

    def _get_pv_type(self) -> str:
        """Get the name of a PB file's pv type using information in its
        header.

        Returns:
            str: Name of pv type, e.g VECTOR_DOUBLE.
        """
        type_descriptor = self.header.DESCRIPTOR.fields_by_name["type"]
        enum_descriptor = type_descriptor.enum_type
        return enum_descriptor.values_by_number[self.header.type].name

    def _get_proto_class(self) -> Callable:
        """Get the EPICSEvent_pb2 class corresponding to the pv in a PB file.
        Instances of this class can interpret PB messages of a matching type.

        Returns:
            Callable: EPICSEvent_pb2 protocol buffer class.
        """
        proto_class_name = self._get_proto_class_name()
        proto_class = getattr(EPICSEvent_pb2, proto_class_name)
        return proto_class

    def _get_proto_class_name(self) -> str:
        """Convert the name of a pv type to match the proto class name. This
        mapping is described in
        epicsarchiverap/src/main/edu/stanford/slac/archiverappliance/PB/data/DBR2PBTypeMapping.java

        Returns:
            str: Name of proto class, e.g VectorDouble.
        """
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
        return pv_type_to_class_name[self.pv_type]

    @staticmethod
    def get_temp_filename(filename: PathLike) -> Path:
        filename = Path(filename)
        filename = filename.with_stem(f"{filename.stem}_tmp")
        if filename.exists():
            filename = ArchiverData.get_temp_filename(filename)
        return filename
