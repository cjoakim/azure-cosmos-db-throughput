
echo 'clean and build ...'
gradle clean
gradle build

echo 'deleting logfile ...'
del tmp\throughput_test1.txt

echo 'local ...'
gradle load_cosmos_baseball_batters_local_throughput  > tmp\throughput_test1.txt

#echo 'global ...'
#gradle load_cosmos_baseball_batters_global_throughput >> tmp\throughput_test1.txt

echo 'done'

