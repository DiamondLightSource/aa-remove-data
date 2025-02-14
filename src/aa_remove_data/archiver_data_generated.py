from pathlib import Path

from aa_remove_data.archiver_data import ArchiverData
from aa_remove_data.generated import EPICSEvent_pb2


class ArchiverDataGenerated(ArchiverData):
    def __init__(
        self,
        samples: int = 100,
        pv_type: int = 6,
        year: int = 2024,
        start: int = 0,
        seconds_gap: int = 1,
        nano_gap: int = 0,
    ):
        self.header = EPICSEvent_pb2.PayloadInfo()  # type: ignore
        self.header.pvname = "generated_test_data"
        self.header.year = year
        self.header.type = pv_type
        self.pv_type = self._get_pv_type()
        self.proto_class = self._get_proto_class()
        self.samples = samples
        self.start = start
        self.seconds_gap = seconds_gap
        self.nano_gap = nano_gap
        self.filepath = Path("dummy")

    def get_samples(self):
        """Read a PB file that is structured in the Archiver Appliance format.
        Gathers the header and samples from this file and assigns them to
        self.header self.samples.

        Args:
            filepath (PathLike): Path to PB file.
        """
        time_gap = self.seconds_gap * 10**9 + self.nano_gap
        time = self.start * 10**9
        for i in range(self.samples):
            sample = self.proto_class()
            sample.secondsintoyear = time // 10**9
            sample.nano = time % 10**9
            if self.pv_type.startswith("WAVEFORM"):
                sample.val.extend(
                    [self.generate_test_value(i * 5 + j) for j in range(5)]
                )
            else:
                sample.val = self.generate_test_value(i)
            time += time_gap
            yield sample

    def get_samples_bytes(self):
        for sample in self.get_samples():
            yield self.serialize(sample)

    def generate_test_value(self, val: int) -> str | bytes | int:
        """Generate an appropriate value for a sample based on it's pv type.

        Args:
            val (int): The original value.

        Returns:
            str | bytes | int: The value converted to an oppropriate type.
        """
        if self.pv_type.endswith("STRING"):
            return str(val)
        elif self.pv_type.endswith("BYTE") or self.pv_type.endswith("BYTES"):
            return val.to_bytes(2, byteorder="big")
        else:
            return val
