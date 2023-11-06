
gradle clean

gradle build

echo 'local ...'
gradle load_cosmos_baseball_batters_local_throughput  > tmp\throughput.txt

echo 'global ...'
gradle load_cosmos_baseball_batters_global_throughput > tmp\throughput.txt

echo 'done'

