# Script to generate data for testing purposes.

from pathlib import Path

from aa_remove_data.archiver_data_generated import ArchiverDataGenerated


def generate_test_data():
    for i in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14):
        adg = ArchiverDataGenerated(pv_type=i, start=i * 200, seconds_gap=2)
        adg.write_txt(Path(f"tests/test_data/{adg.pv_type}_test_data.txt"))
        adg.write_pb(Path(f"tests/test_data/{adg.pv_type}_test_data.pb"))


if __name__ == "__main__":
    generate_test_data()
