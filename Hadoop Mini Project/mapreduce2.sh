hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-file /home/hdfs/autoinc_mapper2.py    -mapper /home/hdfs/autoinc_mapper2.py \
-file /home/hdfs/autoinc_reducer2.py   -reducer /home/hdfs/autoinc_reducer2.py \
-input /output/ \
-output /accidents_count_per_make