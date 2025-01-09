# Script to remove field values to make data used for testing cleaner

import argparse
from pathlib import Path

from aa_remove_data.pb_utils import PBUtils


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    return parser.parse_args()


def remove_field_values(filename):
    pb = PBUtils(filename)
    for sample in pb.samples:
        sample.ClearField("fieldvalues")
    pb.write_pb(filename)


if __name__ == "__main__":
    args = get_args()
    filename = Path(args.filename)
    remove_field_values(filename)
