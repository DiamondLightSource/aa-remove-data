# Script to remove field values to make data used for testing cleaner

import argparse
from pathlib import Path

from aa_remove_data.archiver_data import ArchiverData


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    return parser.parse_args()


def remove_field_values(ad):
    for sample in ad.get_samples():
        sample.ClearField("fieldvalues")
        yield sample


def get_samples_without_field_values(ad):
    return (sample for sample in remove_field_values(ad))


if __name__ == "__main__":
    args = get_args()
    ad = ArchiverData(args.filename)
    write_filename = Path(str(Path(args.filename).with_suffix("")) + "_no_fields.pb")
    ad.process_and_write(
        Path(f"{write_filename}"), False, get_samples_without_field_values
    )
