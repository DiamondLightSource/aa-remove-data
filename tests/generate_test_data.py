# Script to generate data for testing purposes.

from pathlib import Path

from aa_remove_data.pb_utils import PBUtils


def generate_test_data():
    pb = PBUtils()
    for i in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14):
        pb.generate_test_samples(pv_type=i, start=i * 100)
        pb._write_started = False
        pb.write_to_txt(Path(f"tests/test_data/{pb.pv_type}_test_data.txt"))
        pb.write_pb(Path(f"tests/test_data/{pb.pv_type}_test_data.pb"))


if __name__ == "__main__":
    generate_test_data()
