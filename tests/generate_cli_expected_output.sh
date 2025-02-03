echo 'Generating cli_expected_output'
aa-reduce-data-by-factor tests/test_data/SCALAR_STRING_test_data.pb 3 --new-filename tests/test_data/cli_expected_output/SCALAR_STRING_reduce_by_factor.pb -t
echo 'Results in tests/test_data/cli_expected_output/SCALAR_STRING_reduce_by_factor.*'
aa-reduce-data-by-factor tests/test_data/SCALAR_STRING_test_data.pb 4 --new-filename tests/test_data/cli_expected_output/SCALAR_STRING_reduce_by_factor_blocks.pb -t --block 5
echo 'Results in tests/test_data/cli_expected_output/SCALAR_STRING_reduce_by_factor_blocks.*'
aa-reduce-data-to-period tests/test_data/SCALAR_STRING_test_data.pb 5 --new-filename tests/test_data/cli_expected_output/SCALAR_STRING_reduce_to_period.pb -t
echo 'Results in tests/test_data/cli_expected_output/SCALAR_STRING_reduce_to_period.*'
aa-remove-data-before tests/test_data/SCALAR_STRING_test_data.pb 1,1,0,1,5 --new-filename tests/test_data/cli_expected_output/SCALAR_STRING_remove_before.pb -t
echo 'Results in tests/test_data/cli_expected_output/SCALAR_STRING_remove_before.*'
aa-remove-data-after tests/test_data/SCALAR_STRING_test_data.pb 1,1,0,1,5 --new-filename tests/test_data/cli_expected_output/SCALAR_STRING_remove_after.pb -t
echo 'Results in tests/test_data/cli_expected_output/SCALAR_STRING_remove_after.*'
echo 'Complete!'
