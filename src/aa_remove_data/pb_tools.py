from itertools import islice
from pathlib import Path

import typer

from aa_remove_data.archiver_data import ArchiverData
from aa_remove_data.remove_data import validate_pb_file

app = typer.Typer()

FILENAME_ARGUMENT = typer.Argument(help="path/to/file.pb of PB file.")
TXT_FILENAME_ARGUMENT = typer.Argument(None, help="path/to/file.txt of text file.")


@app.command()
def pb_2_txt(
    filename: Path = FILENAME_ARGUMENT,
    txt_filename: Path | None = TXT_FILENAME_ARGUMENT,
):
    """Convert a PB file to a human-readable text file."""
    txt_file = txt_filename if txt_filename else filename.with_suffix(".txt")
    print(f"Writing {txt_file}")
    # Validation
    validate_pb_file(filename, should_exist=True)
    ad = ArchiverData(filename)
    ad.write_txt(txt_file)
    print("Write completed!")


@app.command()
def print_header(filename: Path = FILENAME_ARGUMENT, lines: int = 0, start: int = 0):
    """Print the header and a few lines of a PB file."""
    # Validation
    validate_pb_file(filename, should_exist=True)
    ad = ArchiverData(filename)
    print(f"Name: {ad.header.pvname}, Type: {ad.pv_type}, Year: {ad.header.year}")
    if lines:
        print(f"DATE{' ' * 19}SECONDS{' ' * 5}NANO{' ' * 9}VAL")
        for sample in islice(ad.get_samples(), start, start + lines):
            print(ad.format_datastr(sample, ad.header.year).strip())
