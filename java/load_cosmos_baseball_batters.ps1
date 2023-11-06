
echo 'clean and build ...'
gradle clean
gradle build

echo 'deleting logfile ...'
del tmp\throughput.txt

echo 'local ...'
gradle load_cosmos_baseball_batters_local_throughput  > tmp\throughput.txt

echo 'global ...'
gradle load_cosmos_baseball_batters_global_throughput >> tmp\throughput.txt

echo 'done'

