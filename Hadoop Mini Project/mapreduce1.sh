hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-file /home/hdfs/autoinc_mapper1.py    -mapper /home/hdfs/autoinc_mapper1.py \
-file /home/hdfs/autoinc_reducer1.py   -reducer /home/hdfs/autoinc_reducer1.py \
-input /hadoopminiprk/data.csv \
-output /output