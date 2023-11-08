
echo 'clean and build ...'
gradle clean
gradle build

echo 'deleting logfile ...'
del tmp\throughput_test1.txt

echo 'throughput_test_low_priority ...'
gradle throughput_test_low_priority  >  tmp\throughput_test1.txt

echo 'throughput_test_high_priority ...'
gradle throughput_test_high_priority >> tmp\throughput_test1.txt

echo 'throughput_test_ru_limited_local ...'
gradle throughput_test_ru_limited_local >> tmp\throughput_test1.txt

echo 'throughput_test_ru_limited_global ...'
gradle throughput_test_ru_limited_global >> tmp\throughput_test1.txt

echo 'throughput_test_pct_limited_local ...'
gradle throughput_test_pct_limited_local >> tmp\throughput_test1.txt

echo 'throughput_test_pct_limited_global ...'
gradle throughput_test_pct_limited_global >> tmp\throughput_test1.txt

echo 'done'

