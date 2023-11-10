
# Execute several bulk loads with the same data going to the same database and container,
# but with different Throughput and Priority configurations.
# See the build.gradle file for the configuration of each task/scenario.

echo 'clean and build ...'
gradle clean
gradle build

echo 'deleting logfile ...'
del tmp\throughput_tests.txt

# Disabling low and high priority tests for now as this feature
# is in private preview and not yet used by my Cosmos DB account.
# Start-Sleep -Seconds 60
# echo 'throughput_test_low_priority ...'
# gradle throughput_test_low_priority > tmp\throughput_tests.txt

# Start-Sleep -Seconds 60
# echo 'throughput_test_high_priority ...'
# gradle throughput_test_high_priority >> tmp\throughput_tests.txt

Start-Sleep -Seconds 60
echo 'throughput_test_ru_limited_local ...'
gradle throughput_test_ru_limited_local >> tmp\throughput_tests.txt

Start-Sleep -Seconds 60
echo 'throughput_test_ru_limited_global ...'
gradle throughput_test_ru_limited_global >> tmp\throughput_tests.txt

Start-Sleep -Seconds 60
echo 'throughput_test_pct_limited_local ...'
gradle throughput_test_pct_limited_local >> tmp\throughput_tests.txt

Start-Sleep -Seconds 60
echo 'throughput_test_pct_limited_global ...'
gradle throughput_test_pct_limited_global >> tmp\throughput_tests.txt

echo 'done'

